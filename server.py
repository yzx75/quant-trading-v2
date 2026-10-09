# -*- coding: utf-8 -*-
"""
server.py — 量化交易引擎 v2 单一入口（启动服务一个文件就够了）

用法：
    python server.py            # 起引擎 + HTTP 服务（默认 0.0.0.0:8801）
    python server.py --port 9000
    DRY_RUN=1 python server.py  # 干跑模式：引擎照跑、不下真单（实验）

接口：
    GET  /           看板（自动刷新）
    GET  /health     引擎健康
    GET  /api/state  权益/持仓/最近决策/教训
    POST /pause      暂停决策（平仓守护照跑）
    POST /resume     恢复
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from core import config as cfg_mod          # noqa: E402
from core import log as core_log            # noqa: E402
from core import state                      # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="量化交易引擎 v2")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8801)
    args = ap.parse_args()

    cfg = cfg_mod.load_config()
    cfg_mod.load_env()
    core_log.init(cfg["log_dir"], echo=True)
    LOG = core_log.log
    # 完整交易流水目录（10-03 01:20 用户要求「完整每一单的日志，方便后期整理」）
    try:
        from core import journal
        journal.bind(cfg["runtime_dir"])
    except Exception:
        pass

    from engine import Engine
    engine = Engine(cfg)
    engine.start()
    LOG(f"引擎已启动（{'DRY_RUN 干跑' if os.environ.get('DRY_RUN') == '1' else '实盘'}）"
        f"品种 {len(cfg['universe'])} 个 | 高频≤{cfg['limits']['max_hf_positions']}仓 | 服务 :{args.port}")

    # ---- HTTP 服务（延迟导入，引擎先行）----
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse, JSONResponse
    import uvicorn

    app = FastAPI(title="Quant Engine v2")
    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_methods=["*"], allow_headers=["*"])

    # build_payload 短缓存（10-04 16:1x，17:1x 重修，见该函数 docstring）
    # ⚠ TTL 必须 **大于**云镜像推送周期（5s），否则每次推送都恰好过期、缓存等于没有。
    #   首版错设 3.0< 5.0 → 元凶（同步 Gate API 调用）一次没被挡住，
    #   16:58 仍因「HTTP 无响应」被 watchdog 重启（实测复发，见 logs/watchdog.log）。
    #   现设12s：镜像 5s 一次必命中，看板高频刷新也命中；12s 的数据延迟对看板无感。
    PAYLOAD_TTL_S = 12.0
    _cache = {}

    def _orders_refresher():
        """后台周期刷新交易所条件单快照（10-04 17:1x 新增，治本）。

        为什么必须有这个线程：/api/state 与云镜像共用 build_payload()，而
        build_payload 需要「交易所侧真实条件单」来给持仓补 SL/TP 显示值。
        原来它在**请求路径里**同步调 list_price_orders()（0.48~0.76s，
        网络差时可达 20s）→ /api/state 是 FastAPI 同步 def handler →
        直接占住 uvicorn worker 线程 → watchdog 5s 探活超时 → 反复重启引擎
        （10-04 单日 146 次拉起，见 logs/watchdog.log）。

        首版只加 PAYLOAD_TTL_S=3.0 缓存，但镜像周期是 5s > 3s，每次推送都
        恰好过期 → 每次仍真调 API → 16:58 照样卡死重启。教训：**光靠限流
        缓存挡不住周期性调用者，必须把 IO 移出请求路径。**

        现在条件单由本线程按 10s 周期拉取（只为看板显示用，10s 延迟无感），
        build_payload 纯本地读缓存，永不阻塞 HTTP。注意：**交易用的 SL/TP
        不依赖此快照** —— guard 线程自己查交易所、交易所侧也真挂着单，
        这里只是看板显示用的兜底展示。
        """
        from exchange import executor as _ex
        while True:
            try:
                snap = {}
                for o in _ex.list_price_orders():
                    ini = o.get("initial") or {}
                    c = ini.get("contract")
                    txt = ini.get("text") or ""
                    if c and (txt.startswith("t-workbot-") or txt.startswith("t-v2-")):
                        tag = txt.split("-", 2)[-1]          # TP / SL
                        snap.setdefault(c, {})[tag] = float(
                            (o.get("trigger") or {}).get("price") or 0)
                _cache["ex_orders"] = snap
            except Exception as e:  # noqa: BLE001
                LOG(f"⚠ 条件单快照刷新失败（看板 SL/TP 可能显示旧值）: {str(e)[:100]}")
            time.sleep(10)

    # ------------------------------------------------------------------ 订单生命
    # 「历史列表 → 点进去看这一单的完整生命」的数据源：core/journal.py 落盘的
    #   open / mark / close 三类事件（字段约定见该文件 docstring；`mark` 是
    #   10-06 上线的遥测，开仓后 +15/30/60/120/300/900s 各采一次浮盈）。
    #
    # ⚠ **为什么不新开一个 /api/life 接口**：云端看板是**只读镜像** —— 本地
    #   引擎每 5s 把整个 payload `POST /push` 推过去，云端**没有反查本地引擎
    #   的能力**（见 cloud/main.py 的 do_GET）。所以手机端能看到的任何数据都
    #   必须先在 payload 里被推过去 → 只能挂 payload 字段，**做不了「点击时
    #   才拉」的懒加载**。
    #
    # ⚠ **为什么必须后台聚合**：聚合要读 journal 文件（磁盘 IO）。放进
    #   /api/state 的请求路径会重演 10-04 的「HTTP 无响应 → watchdog 反复重启
    #   引擎」（完整教训见 _orders_refresher docstring）。这里与它同构：后台
    #   线程按周期算好放 `_cache`，build_payload 只读内存。
    LIFE_N = 30                  # 保留最近多少笔（payload 体积 ↔ 展示需求的平衡）
    LIFE_TAIL_BYTES = 2_000_000  # 单文件只读尾部 2MB（一天 150 笔 ≈ 0.3MB，足够）
    LIFE_BAD_SYM = "AAA_USDT"    # 单测桩，不展示（记忆「统计铁律」：先过滤 AAA_USDT）
    LIFE_REASON_MAX = 90         # 模型 reason 是大白话、可能很长 → 截断控体积

    def _build_life() -> dict:
        """聚合最近 LIFE_N 笔订单生命周期。**只被 _life_refresher 调用**（后台）。"""
        rt_ = cfg["runtime_dir"]
        evs: dict = {}
        now = time.time()
        for i in range(2):                       # 今天 + 昨天，覆盖跨日持仓
            day = time.strftime("%Y%m%d", time.localtime(now - i * 86400))
            path = os.path.join(rt_, f"journal-{day}.jsonl")
            try:
                size = os.path.getsize(path)
                with open(path, "rb") as fh:
                    fh.seek(max(0, size - LIFE_TAIL_BYTES))
                    buf = fh.read().decode("utf-8", "ignore")
            except Exception:
                continue
            lines = buf.split("\n")
            if size > LIFE_TAIL_BYTES:
                lines = lines[1:]                # 尾部读会把首行切残 → 丢掉
            for ln in lines:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    r = json.loads(ln)
                except Exception:
                    continue
                ev = r.get("ev")
                if ev not in ("open", "mark", "close") or r.get("sym") == LIFE_BAD_SYM:
                    continue
                tid = r.get("trade_id")
                if not tid and ev == "close":
                    # 老 close 记录（10-06 14:04 前落盘的）没有 trade_id →
                    #   用 open_ts 回退构造。口径与 decision._execute 的
                    #   f"{sym}|{side}|{int(t_at_open*1000)}" **同源**，故能配上。
                    ot = float(r.get("open_ts") or 0)
                    if ot and r.get("sym") and r.get("side"):
                        tid = f"{r['sym']}|{r['side']}|{int(ot * 1000)}"
                if not tid:
                    continue
                rec = evs.setdefault(tid, {"marks": []})
                if ev == "mark":
                    rec["marks"].append(r)
                else:
                    rec[ev] = r
        # 标签/EQUITY 补全：老 open 事件（10-06 14:04 前落盘的）**不含**
        #   regime / entry_src / m1h_mom / band_pos / win_rate —— 这些标签其实
        #   一直躺在 trades.json 的 open 记录里（decision._trade_log 写的）。
        #   按**同一个 trade_id 口径**（`t` 就是 t_at_open）配回来，让历史单
        #   也有标签可看，否则详情页对旧单只剩价格、看不出「当初为什么开」。
        # ⚠ 用显式 None 判断而非 `or`：`m1h_mom`/`band_pos` 合法值可能是 0.0，
        #   用 `or` 会被当成假值、退回到 None。
        tmap = {}
        try:
            for t in state.load(os.path.join(rt_, "trades.json"), []):
                if isinstance(t, dict) and t.get("action") == "open" and t.get("sym"):
                    k = (f"{t['sym']}|{t.get('side')}|"
                         f"{int(float(t.get('t') or 0) * 1000)}")
                    tmap[k] = t
        except Exception:
            pass

        def _pk(a, b, k):
            v = a.get(k)
            return v if v is not None else b.get(k)

        items = []
        for tid, e in evs.items():
            o, c = e.get("open") or {}, e.get("close") or {}
            if not o and not c:
                continue
            base = o or c                        # 只有 close 的老记录也要能显示
            lab = tmap.get(tid) or {}
            marks = sorted(e["marks"], key=lambda m: float(m.get("offset") or 0))
            # 开仓时刻：**以 tid 的毫秒段为准**（它就是 decision._execute 里的
            #   `int(t_at_open*1000)`，即"真正下单那一刻"）。
            #   ⚠ 不能用 journal open 事件的 `open_ts`：那是 `place_tpsl` **之后**
            #   才取的 time.time()，实测比 t_at_open 晚 48ms ~ 7.8s（80/80 对
            #   配对样本全部非零）—— 用它当开仓时间会偏晚，且和 tid 对不上。
            try:
                t_open = int(str(tid).rsplit("|", 1)[-1]) / 1000.0
            except Exception:
                t_open = 0.0
            if not t_open:
                t_open = float(o.get("open_ts") or c.get("open_ts") or 0)
            items.append({
                "tid": tid,
                "sym": base.get("sym"), "side": base.get("side"),
                "t_open": t_open,
                "t_close": float(c.get("close_ts") or 0) or None,
                "order_id": o.get("order_id"),
                "sl_id": o.get("sl_id") or c.get("sl_id"),
                "tp_id": o.get("tp_id") or c.get("tp_id"),
                "entry": o.get("entry") or c.get("entry"),
                "sl": o.get("sl") or c.get("sl"), "tp": o.get("tp") or c.get("tp"),
                "size": _pk(o, lab, "size"), "lev": o.get("lev"),
                "regime": _pk(o, lab, "regime"), "sig": _pk(o, lab, "sig"),
                "win_rate": _pk(o, lab, "win_rate"),
                "entry_src": _pk(o, lab, "entry_src"),
                "m1h_mom": _pk(o, lab, "m1h_mom"),
                "band_pos": _pk(o, lab, "band_pos"),
                "atr15": o.get("atr15"), "bw_pct": o.get("bw_pct"),
                "mid": o.get("mid"),
                "reason": str(_pk(o, lab, "reason")
                              or c.get("open_reason") or "")[:LIFE_REASON_MAX],
                # 浮盈采样：o=档位秒 / a=实际 age / px=当时价 / u=当时浮盈(U) / dd=距峰值回撤%
                "marks": [{"o": m.get("offset"), "a": m.get("age_s"),
                           "px": m.get("px"), "u": m.get("upnl"),
                           "dd": m.get("dd_from_peak_pct")} for m in marks],
                "exit_px": c.get("px"), "pnl": c.get("pnl"),
                "mfe": c.get("mfe"), "mae": c.get("mae_pct"),
                "peak": c.get("peak"), "peak_t": c.get("peak_reach_s"),
                "hold_min": c.get("hold_min"), "cause": c.get("close_cause"),
                "text": c.get("text"),
            })
        items.sort(key=lambda x: x["t_open"], reverse=True)
        items = items[:LIFE_N]
        return {"t": round(time.time(), 1), "n": len(items), "items": items}

    def _life_refresher():
        """后台周期聚合订单生命周期 → _cache["life"]（IO 不进请求路径）。"""
        while True:
            try:
                _cache["life"] = _build_life()
            except Exception as e:  # noqa: BLE001
                LOG(f"⚠ 订单生命聚合失败（看板该块显示旧值）: {str(e)[:120]}")
            time.sleep(10)

    @app.get("/", response_class=HTMLResponse)
    def index():
        with open(os.path.join(BASE, "web", "index.html"), encoding="utf-8") as f:
            return f.read()

    @app.get("/health")
    def health():
        return {"ok": True, "paused": engine.paused.is_set(),
                "started_at": engine.snapshot["started_at"],
                "last_hf_cycle": engine.snapshot["last_hf_cycle"],
                "last_guard_cycle": engine.snapshot["last_guard_cycle"],
                "hf_errors": engine.snapshot.get("hf_errors", 0),
                "now": __import__("time").time()}

    def build_payload() -> dict:
        """看板数据总装（本地 /api/state 与云端镜像共用）

        ⚠ 10-04 17:1x 重修（首版 16:1x 的修法是错的，教训记在这）：
          build_payload 里有两处**同步阻塞**操作——
            ① _ex.list_price_orders() 同步打 Gate API（实测 0.48~0.76s，网络差时 20s）
            ② open(llm_decisions.md).read()（已改seek 尾部读，此项不再是瓶颈）
          /api/state 是 FastAPI **同步 def** handler → 直接占住 uvicorn worker
          线程；而它被**两处**调用：云端镜像线程每 5s 一次 + 看板每次刷新。
          结果 HTTP :8801 偶尔 >5s 无响应 → watchdog 判「卡死」→ 反复重启引擎。

        **首版为什么无效**：只加了 PAYLOAD_TTL_S=3.0 缓存，但云镜像周期是 5s——
        每次推送时缓存必然已过期 → 每次都真的执行 ① → 阻塞原封不动。
        16:58 仍被重启（见 watchdog.log）证明此路不通。

        **现在的修法（治本）**：把交易所调用**从请求路径彻底移出去**。
        条件单快照由 watcher 线程周期性地拉一次放进缓存，build_payload 只读快照，
        永不发起网络调用。这样无论被调多少次、间隔多密，都不可能阻塞 HTTP 线程。
        缓存 TTL 退化为「快照最多陈旧多久」（12s），不再是"防重复调用"的限流阀。
        """
        cs = _cache.get("payload")
        if cs and (time.time() - cs[0]) < PAYLOAD_TTL_S:
            return cs[1]
        p = _build_payload_uncached()
        _cache["payload"] = (time.time(), p)
        return p

    def _build_payload_uncached() -> dict:
        rt = cfg["runtime_dir"]
        live = state.load(rt + "/live.json", {})
        trades = state.load(rt + "/trades.json", [])
        # ---- 10-05 翻仓快照透传 ----
        # `live.json` 由 engine._watch() 每 2s 写入，其中 `flip` 段由
        # core/flip.py 的 snapshot() 拼装（每品种 S0~S5 事务状态 / 冻结剩余秒数 /
        # 双开审计记录 / 计数器）。这里是**纯本地读 + 取键**，零 IO、零阻塞。
        # ⚠ 不新增接口、不改既有字段结构 —— 只在payload 顶层多一个 `flip` 键，
        #   前端按需自取；老前端忽略它即可（向后兼容）。
        flip_snap = live.get("flip") or {}
        if not flip_snap:
            try:
                from core import flip as _flip
                flip_snap = _flip.snapshot()
            except Exception as e:  # noqa: BLE001
                LOG(f"⚠ 翻仓快照不可用（看板该块显示为空）: {str(e)[:100]}")
                flip_snap = {}
        # 今日统计服务端算好（10-02 修复：trades[-60:] 截断导致看板当日
        # 笔数/盈亏只统计到最近一段，388 条流水里今日 58 笔 close 只露出 30 笔）
        t0 = time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
        today = [t for t in trades
                 if t.get("action") == "close" and float(t.get("t") or 0) >= t0]
        today_stat = {"count": len(today),
                      "real": round(sum(float(t.get("pnl") or 0) for t in today), 4),
                      "wins": sum(1 for t in today if float(t.get("pnl") or 0) > 0)}
        try:
            lessons = open(rt + "/lessons.md", encoding="utf-8").readlines()[-10:]
        except Exception:
            lessons = []
        try:
            # 10-04 16:1x：只读文件尾部 32KB 再切片，别把 6.4MB 全读进内存
            # （该文件只追加增长，全量 read 是纯浪费，实测 26ms → 0.3ms）。
            # ⚠ 必须做 universal-newline 归一：原 text 模式 read() 会把 \r\n 折成
            #   \n，字符数因此变少；直接 binary decode 会保留 \r，切片窗口与原版
            #   错位（回归测试 _test_logfix_1016.py 用例 4 实测抓到）。
            _dp = rt + "/llm_decisions.md"
            with open(_dp, "rb") as _f:
                _f.seek(max(0, os.path.getsize(_dp) - 32000))
                decisions = (_f.read().decode("utf-8", "ignore")
                             .replace("\r\n", "\n").replace("\r", "\n")[-6000:])
        except Exception:
            decisions = ""
        # 持仓管理（平仓决策）流水 —— 10-07 新增，与上面同口径：seek 尾部读，
        # 不做全量 read（本文件只追加）。走同一个 universal-newline 归一。
        try:
            _xp = rt + "/exit_decisions.md"
            with open(_xp, "rb") as _f:
                _f.seek(max(0, os.path.getsize(_xp) - 12000))
                exit_decisions = (_f.read().decode("utf-8", "ignore")
                                  .replace("\r\n", "\n").replace("\r", "\n")[-3000:])
        except Exception:
            exit_decisions = ""
        hist = state.load(rt + "/hist.json", {"equity": [], "px": {}})
        # ---- 标签统计（10-05 任务 H：接管原review.json 的展示位）----
        # 数据源改成 `runtime/trades.json` 的**真实平仓记录**（open/close 按
        # trade_id 配对），而不是 review.json 的 segments/shadow/verdicts ——
        # 那三个键已核实零外部消费者、纯展示，且数字是"影子参数估算"，
        # 不是自己真金白银跑出来的胜率。
        #⚠ 性能：`/api/state` 被云镜像每 5s 调一次，绝不能变慢。
        #   `_tag_stats_dims` 内部走 `agents.decision._read_trades_tail()`
        #   的 **seek 尾部读**（只读文件末尾 64KB），**不是**全量 read
        #   （trades.json 已 135KB+ 且在长，全量 read+json.loads 实测 26ms，
        #   乘上每 5s 一次的白云镜像就是纯浪费）。实测本段 ~1ms 量级。
        #   另：这里**不**再额外读 review.json —— 省一次磁盘读。
        try:
            from agents.decision import _tag_stats_dims
            rev = _tag_stats_dims(rt)
            # 10-05（只读字段，零新增 IO）：平仓总数，供前端算「标签覆盖率」。
            # ⚠ 必须用**上面 :156 已全量加载**的 `trades`，**不能**在
            #   `_tag_stats_dims` 里再读一次 —— 那个函数走的是 64KB **尾部**读
            #   （性能考虑），只有 83 条 close；全量是 233 条。用尾部分母会把
            #   覆盖率算成 28/83=34%，而真实是 28/233=12% —— **高估近 3 倍**，
            #   恰好是这个字段要防的那种误导。
            rev["total_closed"] = sum(1 for t in trades
                                      if isinstance(t, dict)
                                      and t.get("action") == "close")
        except Exception as e:  # noqa: BLE001
            LOG(f"⚠ 标签统计刷新失败（看板该块显示为空）: {str(e)[:100]}")
            rev = {"n": 0, "dims": {}, "groups": [],
                   "total_closed": 0, "note": "统计暂不可用"}
        # 持仓合并 SL/TP：本地记录优先，缺失则回退查交易所真实条件单
        hf_meta = (state.load(rt + "/hf_state.json", {}).get("open_meta") or {})
        # ⚠ 10-04 17:1x：**只读快照，不再在这里打 Gate API**。
        #   原来每次请求都同步调 list_price_orders()（0.48~0.76s，网络差时 20s），
        #   这是「HTTP :8801 无响应 → watchdog 反复重启」的唯一元凶。
        #   现在由 _orders_snapshot_loop 后台线程周期刷新（见 start_orders_refresher），
        #   build_payload 纯本地读 → 永不阻塞 HTTP 线程。
        ex_orders = _cache.get("ex_orders") or {}
        for p in live.get("positions") or []:
            key = f"{p.get('contract')}|{p.get('side')}"
            m = hf_meta.get(key) or {}
            p["sl"] = m.get("sl") or ex_orders.get(p.get("contract"), {}).get("SL")
            p["tp"] = m.get("tp") or ex_orders.get(p.get("contract"), {}).get("TP")
            # 10-09：E 段动态止盈（动态绳 / 武装线 / 分批）已整体删除 →
            #   原来的 `watching` / `tp_arm` / `tp_need_pct` 三个字段失去数据源，
            #   且全项目**零读取点**（前端 10-07 起改用固定的「AI 监管」标签，
            #   见 web/index.html::aiTag），一并删除。
            #   出场口径只剩固定 SL/TP，由下方 `_exit_cfg` 下发给前端图例。
            p["peak"] = float(m.get("tp_peak") or 0) or None
        import glob as _glob
        log_lines = []
        logs = sorted(_glob.glob(os.path.join(cfg["log_dir"], "engine-*.log")))
        if logs:
            log_lines = core_log.tail(logs[-1], 80)
        # ---- 最近交易回顾（10-05 任务 D）：最近 8 笔已平仓的 open/close 配对 ----
        # 复用 agents.decision 里的同一个格式化函数，保证**看板与 brief 里
        # 模型看到的文字完全一致**（两处口径漂移会让模型困惑）。
        # ⚠ **不引入新的阻塞 IO**：`trades` 上面已经 state.load 全量读过了，
        #   这里只在内存里做配对 + 字符串拼接（8 行，微秒级），不碰文件、
        #   不打交易所 API —— /api/state 的 0.1s 响应预算不受影响。
        try:
            from agents.decision import build_recent_reviews
            recent_trades = build_recent_reviews(trades, 8)
        except Exception:
            recent_trades = []
        # ---- 顶层 `positions` 冗余镜像（10-05 用户批准，纯冗余零IO）----
        # 起因：team-lead 今天连续 4 次用 `d.get('positions')` 读**顶层**拿到
        # `[]`，误判成「字段失真/数据不一致」，排查绕了大圈 —— 实际持仓在
        # `live.positions`，顶层本来就没有 `positions` 键。
        # 这里把同一份内存对象镜像到顶层，让这类误读不再发生。
        # ⚠ **纯引用复制，不新增任何 IO、不重新读盘、不打交易所 API**
        #   （`live` 上面已 state.load 过），/api/state 的 57ms 预算不受影响。
        # ⚠ 向后兼容：老前端读 `live.positions` 完全不受影响；两个键**同源同值**
        #   （就是同一个 list 对象），不存在两处口径漂移的可能。
        _positions_mirror = live.get("positions") or []
        # ---- 出场档位全局快照（10-07）：前端图例用 ----
        # 为什么要顶层给一份：空仓时逐仓字段不存在，前端就无法渲染图例。纯 config 读，零 IO。
        # ⚠ 原三层保护档位键（be_arm_pct/tp_arm_pct/arm_tp_r/keep_r/min_net_pct）
        #   随看板改版（图例换「AI 监管」）已无消费者 → 死数据，已删。
        _exit_cfg = {
            # 出场监管图例要显示**真实**的止损/止盈口径，而「前端不硬编码任何
            #   百分比」是本项目铁律（历史上多次因看板写死一套、引擎另一套而漏报）
            #   → 由后端从 config 读出来下发。
            "sl_pct": float(cfg["exit"].get("sl_min_pct", 0.0) or 0.0),
            "tp_pct": float(cfg["exit"].get("tp_fixed_pct", 0.0) or 0.0),
        }
        return {"live": live, "trades": trades[-60:],
                "recent_trades": recent_trades,
                "today": today_stat,
                "today_closes": today[::-1][-150:],   # 最新在前（今日全量平仓流水）
                "lessons": [x.strip() for x in lessons],
                "decisions_tail": decisions,
                "exit_tail": exit_decisions,   # 持仓管理（平仓决策）流水，见上方读取块
                "exit_review": {"enabled": bool((cfg.get("exit_review") or {}).get("enabled", True)),
                                "interval_s": (cfg.get("exit_review") or {}).get("interval_s", 60)},
                "scan": state.load(rt + "/scan.json", {}),
                "hist": {"equity": (hist.get("equity") or [])[-160:],
                         "px": {k: v[-100:] for k, v in (hist.get("px") or {}).items()}},
                "review": rev,
                "flip": flip_snap,
                "exit_cfg": _exit_cfg,   # 三层保护档位阈值（前端图例用，见上方注释）
                "positions": _positions_mirror,   # 顶层冗余镜像（见上方注释）
                "model": os.environ.get("CLOUD_MODEL", "--"),
                "mode": "dry" if os.environ.get("DRY_RUN") == "1" else "live",
                "intervals": {"hf": cfg["hf_interval_s"],
                              "guard": cfg["guard_interval_s"]},
                "limits": {"hf": cfg["limits"]["max_hf_positions"]},
                "paused": engine.paused.is_set(),
                "snapshot": {k: v for k, v in engine.snapshot.items()},
                # 订单生命周期（最近 LIFE_N 笔，供「历史 → 点进去看这一单」）：
                #   后台 `_life_refresher` 算好放 `_cache`，这里是**纯内存读、
                #   零 IO**（见该函数注释）。空时给合法空结构，前端不用判 null。
                "life": _cache.get("life") or {"t": 0, "n": 0, "items": []},
                "log_lines": log_lines}

    @app.get("/api/state")
    def api_state():
        return build_payload()

    @app.post("/pause")
    def pause():
        engine.paused.set()
        LOG("⏸ 决策已暂停（guard 守护照跑）")
        return {"ok": True, "paused": True}

    @app.post("/resume")
    def resume():
        engine.paused.clear()
        LOG("▶ 决策已恢复")
        return {"ok": True, "paused": False}

    @app.get("/api/log")
    def api_log(n: int = 120):
        import glob
        logs = sorted(glob.glob(os.path.join(cfg["log_dir"], "engine-*.log")))
        if not logs:
            return JSONResponse([])
        lines = core_log.tail(logs[-1], n)
        return JSONResponse([x.rstrip("\n") for x in lines])

    # ---- 云端镜像推送（MIRROR_URL 配置后启用；看板数据 5 秒一推）----
    mirror_url = os.environ.get("MIRROR_URL", "")
    if mirror_url:
        import urllib.request as _ur

        def _mirror_loop():
            token = os.environ.get("MIRROR_TOKEN", "")
            fail = 0
            via_direct = False          # 上次成功走的路径：False=系统代理 True=直连
            op_proxy = _ur.build_opener()
            op_direct = _ur.build_opener(_ur.ProxyHandler({}))
            while True:
                try:
                    data = json.dumps(build_payload(), ensure_ascii=False).encode("utf-8")
                    req = _ur.Request(mirror_url.rstrip("/") + "/push", data=data,
                                      headers={"Content-Type": "application/json",
                                               "X-Token": token})
                    # 上次走代理成功就先代理，失败自动换直连重试一次（反之亦然）
                    first, second = (op_direct, op_proxy) if via_direct else (op_proxy, op_direct)
                    try:
                        first.open(req, timeout=15).read()
                    except Exception:
                        second.open(req, timeout=15).read()
                        via_direct = not via_direct
                    if fail >= 6:
                        LOG(f"☁ 云端镜像已恢复（{'直连' if via_direct else '代理'}）")
                    fail = 0
                except Exception as e:  # noqa: BLE001
                    fail += 1
                    if fail % 12 == 1:   # 每分钟最多记一条，防刷屏
                        LOG(f"⚠ 云端镜像推送失败(×{fail}): {str(e)[:100]}")
                time.sleep(5)

        threading.Thread(target=_mirror_loop, daemon=True, name="mirror").start()
        LOG(f"☁ 云端镜像已启用 → {mirror_url}")

    # 条件单快照后台刷新（必须在 uvicorn.run 之前起，见 _orders_refresher docstring）。
    # ⚠ 10-04 17:2x 教训：这一行曾放在 import 之后立刻执行 → UnboundLocalError
    #   （_orders_refresher 定义在下方、此时还没绑定）→ 引擎起不来、
    #   watchdog 每 20s 拉起一次又崩，形成"连环重启"。函数定义后再起线程。
    threading.Thread(target=_orders_refresher, daemon=True, name="orders").start()
    # 订单生命周期聚合（同上：IO 不进请求路径；函数定义已在上方完成绑定）
    threading.Thread(target=_life_refresher, daemon=True, name="life").start()

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
