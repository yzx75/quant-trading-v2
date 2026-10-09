# -*- coding: utf-8 -*-
"""
engine.py — 引擎编排：五线程
  watcher  行情快照（供看板/护栏）
  hf_loop  开仓决策：60 秒一轮（布林候选扫描 → 模型决策 → 风控执行 → open/hold）
  guard_loop 保证金守护：2 秒一轮（裸仓补挂/孤儿单巡检/保证金守护；出场类护栏现全关）
  review_loop 复盘（现为事件驱动：教训在平仓当场生成，本线程不产生 IO）
  exit_loop 持仓管理：60 秒一轮（**只持仓的品种** → 模型判 hold/close → 市价平）
全部线程可经 pause/resume 总闸控制；任何线程异常只记日志不拖垮进程。
"""
from __future__ import annotations

import json
import os
import threading
import time
import traceback

from agents import decision as hf_mod
from agents import exit_agent as exit_mod
from agents import review as review_mod
from core import flip
from core import journal
from core import log as core_log
from core import state
from exchange import executor as ex
from strategy.boll import BollStateMachine

LOG = core_log.log

# ---- open_meta 读-改-写临界区锁（10-05 修 hf_state.json 静默丢数据）----
# hf_state.json 有三个并发写入方：
#   ① guard 线程（2s）：_reconcile_closes 清 open_meta / _writeback_meta 更新 peak
#   ② watcher 线程（2s）：_reconcile_closes 由 _watch 调用，同样清 open_meta
#   ③ hf 线程（60s）：agents/decision.py 开仓/加仓写 open_meta
# core.state.save 只保证**单次写**原子（进程内锁 + tmp + os.replace），
# 但调用方的模式是「load()全量读 → 改 → save()全量写」这个**复合操作**，
# 锁只锁写、不锁读改全程 → 两个线程各自基于**陈旧快照**写回时，
# 后写的会把对方刚写的字段静默覆盖回旧值（丢数据，且不抛任何异常）。
#
# 本锁把「读最新 → 改 → 写」收敛成一个不可分割的临界区。
# ⚠ 锁顺序铁律：必须 **先本锁 (_META_LOCK) → 再 state._lock**，绝不可反向。
#   本锁只被 engine.py 的 _mutate_meta 持有，其内部调用链是
#   _META_LOCK → hf._st()/hf._save() → state.load()（无锁）→ state.save()（取 _lock）
#   即 state._lock 永远是内层锁。core/state.py 内部不会回调本锁，
#   因此不存在环路 → 无死锁可能。
_META_LOCK = threading.Lock()

# ---------------------------------------------------------------- 遥测（10-06 建 / 10-07 改节奏）
# 开仓后按下面的节奏持续采浮盈样本，写 journal-YYYYMMDD.jsonl 的 `ev=mark` 行。
# 纯旁路：**不参与任何决策**，采集异常只吞掉（journal.log 内部本就 try/except）。
# 用途（现在做不了、有了才能做的四件事）：
#   ① 「15 秒浮盈能否预测最终结果」——决定要不要在几十秒时砍掉浮亏单；
#   ② 「标签 × 早期浮盈」——找实时特征，而不是只看事后标签；
#   ③ 峰值到达时间 → 判断"这单走起来了没有"（模型侧 ②a/②b 与停滞锁盈都用它）；
#   ④ **全程盈亏波形** → 看板「订单生命」的曲线（10-07：原等距 6 档最长只到
#      900s=15min，而实测持仓中位 >1h，曲线只覆盖前 23%、看不到出场怎么亏的）。
#
# 🔴 为什么加密采样几乎免费：`_tlm_mark` 本来就被 guard 守护轮**每 2s 调用
# 一次**、px（现价）就在手上（engine.py 里那处 try 调用）。加采样档位**不新增
# 任何 HTTP、不新增任何计算**，唯一成本是 journal 行数。
#   段内等间隔、跨段翻倍：密集段抓路径拐点（15s 足以看清 1 分钟内的锯齿），
#   长尾降频防行数爆炸（24h 单也只 ~171 点）。
_TLM_TIERS = (
    (600, 15),      # 前 10 分钟：每 15s   → 40 点
    (1800, 30),     # 10~30 分钟：每 30s
    (3600, 60),     # 30~60 分钟：每 60s
    (10800, 180),   # 1~3 小时：每 3min
    (86400, 600),   # 3~24 小时：每 10min
)
_TLM_MAX_N = 240   # 点数硬顶（1h 单约 110 点，24h 单约 171 点，够用）
# 采样宽容窗口：age 超出该次目标这么多秒以上就**只推进不写**（那是重启后的陈旧
# 档位，补采出来只会是同一现价的噪声）。45s ≈ 容忍 guard 连续数轮偏慢。
_TLM_GRACE_S = 45


def _tlm_iv(age: float) -> float:
    """该持仓年龄下该用多少秒的采样间隔（分段递增，见 _TLM_TIERS）。"""
    for lim, iv in _TLM_TIERS:
        if age < lim:
            return iv
    return _TLM_TIERS[-1][1]


class Engine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        # 翻仓冻结闸总开关（config.yaml 的 `flip.guard_freeze`）。回填一次即可：
        # `is_frozen()` 是热路径读点，不能在 2s 轮询里去读配置。
        flip.set_freeze_enabled(
            bool((cfg.get("flip") or {}).get("guard_freeze", True)))
        self.paused = threading.Event()          # set = 暂停
        self.stopped = threading.Event()
        self.snapshot = {                        # 供 API/看板读取的共享快照
            "started_at": time.time(),
            "last_hf_cycle": 0, "last_guard_cycle": 0,
            "hf_errors": 0,
            "regime": "", "bias": "",
        }
        self.sm = BollStateMachine(cfg["boll"])
        self.risk = hf_mod.RiskManager(cfg)
        self.hf = hf_mod.HfAgent(cfg, self.sm, self.risk)
        # 10-05 翻仓：把 Engine 实例回填给 hf 线程用。
        # F4 落账需要 `Engine._mutate_meta`（复用本模块的 _META_LOCK 临界区，
        # 保证「删旧 key + 写新 key」不可分割）。**绝不能让 decision.py 反向
        # `from engine import Engine`** —— 本模块已经 import 了 decision，
        # 那样直接成环（import 环 = 引擎起不来，watchdog 反复拉起）。
        self.hf._engine_ref = self
        self.review = review_mod.ReviewAgent(cfg)   # 复盘迭代线程（甲+乙）
        # 持仓管理（平仓决策）线程：只对**账内持仓**调模型，判 hold/close。
        # 与 hf 的分工见 agents/exit_agent.py 的模块 docstring（互不越界）。
        self.exit = exit_mod.ExitAgent(cfg)
        self._pos_keys = None                     # 上一轮持仓快照（平仓对账用）
        self._ghost_seen = set()# 已报告过的非本系统挂单 id（防刷屏）
        self._exit_ghost_path = os.path.join(cfg["runtime_dir"], "exit_ghosts.json")
        self._exit_ghosts = state.load(self._exit_ghost_path, {})
        if not isinstance(self._exit_ghosts, dict):
            self._exit_ghosts = {}
        self._dual_audited = False                # 启动双开审计只做一次
        # 裸仓补挂的按 symbol 限流表（10-05：裸仓巡检脱离 heavy 门控后，
        # 用它把「稳态有裸仓」时的 Gate 查询压到 1 次 / bare_cool_s 秒）。
        # 纯内存 dict，进程重启即清空 —— 对保命逻辑无影响（首轮必然放行）。
        self._bare_cool: dict = {}
        # 孤儿仓兜底的下次检查时刻（monotonic）。与 _bare_cool 语义**不同**：
        #   _bare_cool  = 「补挂后冷却」（防重复补挂）
        #   _orphan_next= 「检查频率」（= 发现延迟，因孤儿态本地不可判，只能问交易所）
        self._orphan_next = 0.0
        # 遥测采样进度表（内存）：key = "sym|side" → (trade_id, last_at, n)
        #   last_at = 上次采样的名义秒数；n = 已采点数（硬顶 _TLM_MAX_N）。
        # **刻意不落盘**——guard 循环里本条 meta 未必走到末尾的 `_writeback_meta`
        # （裸仓巡检/冻结闸会 continue），落盘会漏 → 下一轮重复采同一档位。
        # 见 `_tlm_mark`。纯旁路，丢了只影响采样，不影响交易。
        self._tlm_seen: dict = {}

    # -------------------------------------------------- 线程体
    def _loop(self, name: str, interval: float, fn):
        while not self.stopped.is_set():
            if self.paused.is_set():
                time.sleep(1)
                continue
            t0 = time.time()
            try:
                fn()
                self.snapshot[f"last_{name}_cycle"] = time.time()
            except Exception as e:  # noqa: BLE001
                key = f"{name}_errors"
                self.snapshot[key] = self.snapshot.get(key, 0) + 1
                LOG(f"✗ {name} 异常: {type(e).__name__}: {str(e)[:160]}\n"
                    f"{traceback.format_exc(limit=3)}")
            dt = time.time() - t0
            time.sleep(max(1.0, interval - dt))

    # -------------------------------------------------- 启动双开审计（G2/G3）
    # ⚠️ 位置铁律：本方法**必须定义在 start() 之前**。MEMORY 17:22 血泪：
    #   `Thread(target=_f)` 写在 `def _f` 之前 → UnboundLocalError →
    #   引擎起不来、watchdog 每 20s 拉起又崩。`py_compile` **查不出**顺序问题。
    def _startup_dual_audit(self, positions: list = None):
        """引擎启动后做一次双开审计：**只告警，绝不自动动**（用户 10-05 拍板）。

        Gate 是双向持仓模式，同 contract 可多空并存。启动时若发现两腿并存：

        | open_meta key 数 | 判读| 处置 |
        |---|---|---|
        | 2 个 | 历史遗留双开（改动前的，或App 手动造成） | S2，只告警 |
        | 1 个 | **翻仓事务中途崩溃的指纹**（F4 未完成） | S2 + 明确写「疑似翻仓中断」 |
        | 0 个（单腿） | 账外仓语义 | 不属本函数范围，走现状孤儿兜底 |

        **为什么不自动收敛**（PRD G2/G3 + 用户拍板第 6 条）：无法区分
        「翻仓中断」与「历史遗留双开」，两者的正确处置完全不同（前者应回滚，
        后者应由人工决策）。**误自动平仓比留着双开更危险** —— 那动用真金白银
        替用户决定持有什么。上线时存量的 SNDK/HYPE 双开由用户手动平。

        **S2 不拒开新仓**（用户 10-05 改判 Q-C，覆盖架构文档原设计）：只告警 +
        记审计，`_try_open` 照常放行。
        """
        if self._dual_audited:
            return 0
        self._dual_audited = True
        if not (self.cfg.get("flip") or {}).get("audit_on_start", True):
            return 0
        try:
            if positions is None:
                # 🔴 10-05 补（空快照灾难链，用户批准的 4 个调用点之外）：
                #   这是本轮 T04 我自己新增的调用点，同样用了宽松 `ex.positions()`。
                #   空 body 时它会静默得到 `[]` → 审计打出「✅ 无同contract
                #   双腿并存」的**假安全结论** —— 而 SNDK/HYPE 双开正是本审计
                #   要抓的东西。属QA 在 P1 指出的同一类「认知误导」，
                #   故一并改成严格版（本任务点的 4 处之外，我主动补齐这一处）。
                #   只影响「查不到时是否谎报安全」，不会动仓位，故低风险。
                _aok, positions, _aerr = ex.positions_strict_rows()
                if not _aok:
                    LOG(f"  ⚠ 启动双开审计跳过：持仓快照不可信（{_aerr}）"
                        f"——不谎报「无双腿并存」，请人工核对")
                    try:
                        journal.log("anomaly", kind="positions_unavailable",
                                    where="startup_dual_audit", err=str(_aerr)[:200],
                                    why="审计跳过：查不到不等于无仓，不谎报安全")
                    except Exception:
                        pass
                    return 0
        except Exception as e:  # noqa: BLE001 —— 查不到持仓就别审计，绝不影响启动
            LOG(f"  ⚠ 启动双开审计跳过：持仓查询失败 {str(e)[:100]}")
            return 0
        st = self.hf._st()
        own = set((st.get("open_meta") or {}).keys())
        held: dict = {}
        for p in positions:
            sz = float(p.get("size") or 0)
            if sz == 0:
                continue
            sym = p.get("contract")
            side = "long" if sz > 0 else "short"
            held.setdefault(sym, {})[side] = abs(sz)
        n = 0
        for sym, leg in sorted(held.items()):
            if not (float(leg.get("long") or 0) > 0
                    and float(leg.get("short") or 0) > 0):
                continue                      # 单腿 = 稳态 S1，不属审计范围
            keys = sorted(k for k in own if k.split("|")[0] == sym)
            suspected = (len(keys) == 1)
            why = ("疑似翻仓中断（两腿但只有 1 个 open_meta key = F4 未完成）"
                   if suspected else "历史遗留双开（两腿 + 2 个 open_meta key）")
            rec = flip.audit_dual(sym, leg, meta_keys=keys, source="startup",
                                  note=why)
            try:
                journal.log("anomaly", kind="legacy_both", sym=sym,
                            legs=leg, meta_keys=keys, startup=True,
                            suspected_flip_interrupt=suspected,
                            why=why + "；只告警不自动收敛，等人工处理")
            except Exception:
                pass
            LOG(f"  🚨🚨 启动双开审计 {sym}：{leg}两腿并存，{why}"
                f" —— **不自动动**，请手动清理多余腿（已记入双开审计）")
            n += 1
        if n == 0:
            LOG("  ✅ 启动双开审计：无同contract 双腿并存")
        return n

    # ---- 启动回填：把 journal 的历史浮盈曲线喂回 open_meta 的峰/谷 ----
    # 🔴 10-08 用户令「修」。病根链：10-07 关掉全部机械护盈段 → 峰值被冻结在
    #   开仓价 → 10-08 12:09 才补上无条件遥测。但**修复之前开的仓**、以及**每次重启
    #   之后**，真实峰值/谷值都不在 meta 里。实证：SNDK 多 10-08 08:13 开，
    #   真实峰值 +0.543U（=0.52R，10:34:49 采样到），而引擎平仓记录里 peak=1704.98、
    #   mfe=0.030% —— 模型全程以为这单没赚过 → 条件②无从触发 → 最后被 SL 打掉
    #   −1.11U（②a 本可在 10:58 触发、落袋 +0.13U）。
    # 本方法只读 journal 的历史 mark 事件（每条含真实 px），把每笔**在持仓**的
    #   峰值/谷值补进 meta；**不碰任何订单、不做任何出场动作**，纯数据回填。
    # 只跑一次（start() 起线程之前），保证 exit 线程的第一轮就看到真实峰谷。
    # 回退：guards.peak_backfill_on_start 置 false。
    def _backfill_peaks_from_journal(self) -> int:
        gcfg = self.cfg.get("guards") or {}
        if not gcfg.get("peak_backfill_on_start", True):
            return 0
        rt = self.cfg["runtime_dir"]
        now = time.time()
        marks = {}                      # trade_id -> [(age_s, px)]
        for i in range(2):              # 今天 + 昨天（覆盖跨 0 点的持仓）
            day = time.strftime("%Y%m%d", time.localtime(now - i * 86400))
            path = os.path.join(rt, "journal-" + day + ".jsonl")
            try:
                size = os.path.getsize(path)
                with open(path, "rb") as fh:
                    fh.seek(max(0, size - 8_000_000))   # 尾读 8MB（一天量足够）
                    buf = fh.read().decode("utf-8", "ignore")
            except Exception:
                continue
            lines = buf.split("\n")
            if size > 8_000_000:
                lines = lines[1:]       # 尾读会把首行切残，丢掉
            for ln in lines:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    r = json.loads(ln)
                except Exception:
                    continue
                if r.get("ev") != "mark":
                    continue
                tid = r.get("trade_id")
                px = float(r.get("px") or 0)
                if tid and px > 0:
                    marks.setdefault(tid, []).append((float(r.get("age_s") or 0), px))
        if not marks:
            return 0
        om = (self.hf._st().get("open_meta") or {})
        fixed = 0
        for key, meta in list(om.items()):
            sym, _, side = key.partition("|")
            if side not in ("long", "short"):
                continue
            t0 = float(meta.get("t_at_open") or 0)
            tid = meta.get("trade_id") or (sym + "|" + side + "|" + str(int(t0 * 1000)) if t0 else "")
            rows = marks.get(tid) or []
            entry = float(meta.get("entry") or 0)
            if not rows or entry <= 0:
                continue
            px_hi = max(p for _, p in rows)
            px_lo = min(p for _, p in rows)
            pk_px = px_hi if side == "long" else px_lo       # 最有利价
            tr_px = px_lo if side == "long" else px_hi       # 最不利价
            pk_age = next((a for a, p in rows if p == pk_px), 0.0)
            cur_pk = float(meta.get("peak") or 0) or entry
            cur_tp = float(meta.get("tp_peak") or 0) or entry
            cur_tr = float(meta.get("trough") or 0) or entry
            better_pk = (pk_px > cur_pk) if side == "long" else (pk_px < cur_pk)
            better_tp = (pk_px > cur_tp) if side == "long" else (pk_px < cur_tp)
            better_tr = (tr_px < cur_tr) if side == "long" else (tr_px > cur_tr)
            if not (better_pk or better_tp or better_tr):
                continue
            snap = dict(meta)
            if better_pk:
                meta["peak"] = pk_px
            if better_tp:
                meta["tp_peak"] = pk_px
            if better_tr:
                meta["trough"] = tr_px
            if (better_pk or better_tp) and t0:
                meta["peak_t"] = t0 + pk_age     # 峰值到达时刻（喂 peak_reach_s 遥测）
            try:
                if self._writeback_meta(sym, side, meta, snap):
                    fixed += 1
                    gain = (pk_px - entry) if side == "long" else (entry - pk_px)
                    _parts = []
                    if better_pk or better_tp:
                        _parts.append("峰值 %.8g→%.8g（%+.2f%%）"
                                      % (cur_pk, pk_px, gain / entry * 100))
                    if better_tr:
                        _parts.append("谷值 %.8g→%.8g" % (cur_tr, tr_px))
                    LOG("  🩹 峰值回填 %s %s：%s（%d 个历史采样点）"
                        % (sym, side, "；".join(_parts), len(rows)))
            except Exception as e:  # noqa: BLE001
                LOG("  ⚠ 峰值回填失败 " + sym + " " + side + ": " + str(e)[:120])
        if fixed:
            LOG("  🩹 启动峰值回填：%d 笔（journal 历史曲线 → open_meta）" % fixed)
        return fixed

    def start(self):
        # 恢复：检查孤儿仓（引擎重启后open_meta 有记录但模型态丢失的仓，靠 guard 兜底）
        core_log.log("引擎启动：watcher / hf / guard / review / exit 五线程"
                     "（趋势仓策略已移除；出场 = 交易所条件单硬兜底 + 模型持仓管理）")
        # 启动双开审计（G2/G3）：只告警不自动动。放在起线程**之前**，
        # 让第一轮 guard/开仓之前就把遗留双开的状态报出来。
        try:
            self._startup_dual_audit()
        except Exception as e:  # noqa: BLE001 —— 审计失败绝不能挡住启动
            LOG(f"  ⚠ 启动双开审计异常（不影响启动）: {str(e)[:140]}")
        # 峰值回填必须**在起线程之前**跑完：exit 线程第一轮就要看到真实峰谷，
        #   否则它会拿"峰值=开仓价"把这类单误判成"没走起来"（SNDK 的教训）。
        try:
            self._backfill_peaks_from_journal()
        except Exception as e:  # noqa: BLE001 —— 回填失败绝不能挡住启动
            LOG(f"  ⚠ 启动峰值回填异常（不影响启动）: {str(e)[:140]}")
        threading.Thread(target=self._loop, args=("watch", float(self.cfg["watch_interval_s"]),
                                                  self._watch), daemon=True, name="watcher").start()
        threading.Thread(target=self._loop, args=("hf", float(self.cfg["hf_interval_s"]),
                                                  self.hf.cycle), daemon=True, name="hf_loop").start()
        threading.Thread(target=self._loop, args=("guard", float(self.cfg["guard_interval_s"]),
                                                  self._guard), daemon=True, name="guard_loop").start()
        threading.Thread(target=self._loop, args=("review", 300.0,
                                                  self.review.cycle), daemon=True, name="review_loop").start()
        # 持仓管理（平仓决策）：周期读 config 的 exit_review.interval_s（默认 60s）。
        # ⚠ 必须在 `def start` 之前没有任何引用依赖 —— `self.exit` 在 __init__ 里
        #   已构造好，本行只是起线程（MEMORY 17:22 的 UnboundLocalError 教训）。
        threading.Thread(target=self._loop,
                         args=("exit", float((self.cfg.get("exit_review") or {})
                                             .get("interval_s", 60)),
                               self.exit.cycle),
                         daemon=True, name="exit_loop").start()

    def stop(self):
        self.stopped.set()

    # -------------------------------------------------- watcher
    def _watch(self):
        snap = {}
        for sym in self.cfg["universe"]:
            snap[sym] = {"px": feeder_px(sym), "t": time.time()}
        eq = ex.equity()
        # 🔴 10-05 修（空快照灾难链，用户批准）：见 `_guard()` 里同段注释的完整
        # 推理。`_watch()` 是**同一条灾难链的第二跳**——它拿同一份不可信快照去：
        #   · 写 `live.json`（positions=[] → 看板显示空仓）
        #   · 调 `_reconcile_closes(positions)` → `now_keys={}` → **全部 open_meta
        #     key 进 `gone`** → TTL 到期后被**强删**（幽灵清扫）
        # `ok=False` 时整轮 return：不写 live.json、不跑对账、不采样 hist。
        # ⚠ 宁可让看板的持仓显示**陈旧 2 秒**，也绝不用「假空仓」覆盖它——
        #   假空仓会被下游（对账/幽灵清扫）当真，把账删掉。
        _ok, positions, _perr = ex.positions_strict_rows()
        if not _ok:
            LOG(f"  🚫 watcher 本轮中止：持仓快照不可信（{_perr}）"
                f"——不写 live.json、不做平仓对账（避免把不可信快照当成"
                f"「仓都没了」而强删 open_meta）")
            try:
                journal.log("anomaly", kind="positions_unavailable",
                            where="watch", err=str(_perr)[:200],
                            why="positions_strict ok=False，整轮跳过，不写盘不对账")
            except Exception:
                pass
            self._hist_sample(eq, snap)     # 价格采样不依赖持仓，照常（纯行情）
            return
        # 翻仓快照（10-05）：把事务状态 / 冻结剩余 / 每品种 S0~S5 状态 / 双开审计
        # 塞进 live.json → `/api/state` 直接透传给看板。
        # ⚠ 纯内存拼装（µs 级、无IO、无锁），绝不拖慢 watcher 的 2s 轮询；
        #   **不新起线程、不落独立文件**（事务态是短命的，崩就该消失）。
        try:
            _legs: dict = {}
            for _p in positions:
                _sz = float(_p.get("size") or 0)
                if _sz == 0:
                    continue
                _legs.setdefault(_p["contract"], {})[
                    "long" if _sz > 0 else "short"] = abs(_sz)
            _meta_keys = list((self.hf._st().get("open_meta") or {}).keys())
            flip_snap = flip.snapshot(legs=_legs, meta_keys=_meta_keys)
        except Exception:  # noqa: BLE001 —— 快照失败绝不能影响 watcher
            flip_snap = {"states": {}, "txn": {}, "frozen": {}, "dual_audit": []}
        state.save(self.cfg["runtime_dir"] + "/live.json", {
            "t": time.time(), "equity": eq, "tickers": snap,
            "positions": [_pub_pos(p) for p in positions],
            "hf_keys": sorted(_all_pos_keys(self.hf, positions)),
            "regime": self.snapshot.get("regime", ""), "bias": self.snapshot.get("bias", ""),
            "flip": flip_snap,
        })
        self._reconcile_closes(positions)
        self._exit_ghost_tick(snap)
        self._hist_sample(eq, snap)

    # -------------------------------------------------- 平仓对账（交易所侧落账）
    def _reconcile_closes(self, positions: list):
        """SL/TP 条件单是 Gate 服务器触发的，引擎只有对比持仓快照才能发现平仓。
        持仓消失 → 查 position_close 实际盈亏 → 写交割表 + 清 open_meta。"""
        now_keys = set()
        for p in positions:
            if float(p.get("size") or 0) != 0:
                now_keys.add(f"{p['contract']}|{'long' if float(p['size']) > 0 else 'short'}")
        prev, self._pos_keys = self._pos_keys, now_keys
        # 10-03 20:15 修（幽灵峰值 bug 根因①）：原逻辑只在**持仓集合变化时**才对账
        #   → 「同一 sym|side 平了又开」（集合不变）与「查不到平仓行」（下面那个
        #   continue 分支）两种情况下，open_meta 里的上一代记录永不清理，越积越多
        #   （今天积到 22 条，而交易所实际只有 2 个仓）→ 新仓顶替同一个 key 时
        #   继承上一代的 peak → 模型会看到假的"曾经赚过又回吐"。
        #   现改为：每轮都扫描幽灵（有记录、交易所已无仓），按 ghost_sweep_s 节流
        #   （默认 60s，避免每个幽灵每轮都打一次交易所平仓历史接口）。
        ghost_sweep_s = float(self.cfg["guards"].get("ghost_sweep_s", 60))
        due = (time.time() - float(getattr(self, "_last_ghost_sweep", 0))
               >= ghost_sweep_s)
        if prev == now_keys and not due:
            return
        st0 = self.hf._st()
        ghosts = {k for k in (st0.get("open_meta") or {}) if k not in now_keys}
        if due:
            self._last_ghost_sweep = time.time()
        if prev is None:
            # 启动首轮：引擎停机期间被 Gate 侧条件单/网页手动平掉的仓，
            # 对账快照没有"上一轮"可比 → 用 open_meta 残影补落账（16:25 幽灵单加固）
            gone = ghosts
            if not gone:
                return
            LOG(f"  🔍 启动对账：停机期间消失的仓 {sorted(gone)}")
        else:
            gone = (prev - now_keys) | ghosts
            if not gone:
                return
        rt = self.cfg["runtime_dir"]
        trades = state.load(rt + "/trades.json", [])
        st = self.hf._st()
        dirty = False
        # 10-03 20:30 修：本轮显式删除的 key 集合。HfAgent._save 写回时会把盘上
        #   最新记录并回来（防 guard/decision 互相覆盖），**删除动作会被并集吃掉**
        #   → 幽灵清了又回来（今天 23 条幽灵的真正成因）。必须显式告知 _save。
        deleted = set()
        for key in gone:
            sym, side = key.split("|")
            if key not in (st.get("open_meta") or {}):
                LOG(f"  📒 账外仓 {key} 变动（非系统所开），不落账")
                continue
            row = self._last_close_row(sym, side)
            if row is None:
                # 10-03 20:15 修（幽灵峰值 bug 根因①的另一半：**查不到就永远留着**）：
                #   原逻辑直接 continue，记录永不清 → 幽灵无限堆积。现加 TTL：
                #   超过 ghost_meta_ttl_s（默认 900s）仍查不到平仓行，判定为残留
                #   （仓早已不在、平仓行已滚出查询窗口）→ 强制清理并留痕。
                _m0 = (st.get("open_meta") or {}).get(key) or {}
                _t0 = float(_m0.get("t_at_open") or 0)
                _age = (time.time() - _t0) if _t0 else 0.0
                _ttl = float(self.cfg["guards"].get("ghost_meta_ttl_s", 900))
                if _t0 and _age > _ttl:
                    (st.get("open_meta") or {}).pop(key, None)
                    deleted.add(key)      # 见文末注释：必须显式声明，否则被 _save 并集带回
                    dirty = True
                    try:
                        journal.log("anomaly", kind="ghost_meta", sym=sym,
                                    side=side, entry=_m0.get("entry"),
                                    age_min=round(_age / 60, 1),
                                    why="有记录无仓且查不到平仓行，TTL 后强制清理")
                    except Exception:
                        pass
                    LOG(f"  🧹 幽灵清理 {key}（交易所无仓、查不到平仓行、"
                        f"已存 {_age / 60:.0f} 分钟）→ 清记录，防新仓继承旧峰值")
                else:
                    LOG(f"  📒 落账跳过 {key}：查不到平仓历史（已存 "
                        f"{_age / 60:.0f} 分钟，{_ttl / 60:.0f} 分钟内保留待查）")
                continue
            meta = (st.get("open_meta") or {}).get(key) or {}
            # 防重复：代码路径平仓（_try_close）已在消失前记过账
            recent = [t for t in trades if t.get("sym") == sym and t.get("side") == side
                      and t.get("action") == "close"
                      and abs(float(t.get("t") or 0) - float(row.get("time") or 0)) < 120]
            if recent:
                (st.get("open_meta") or {}).pop(key, None)
                deleted.add(key)
                dirty = True
                continue
            pnl = float(row.get("pnl") or 0)
            txt0 = str(row.get("text") or "")
            src = ("SL/TP 条件单触发" if txt0.startswith("ao-")
                   else "手动平仓（App）" if txt0 == "app"
                   else "模型主动平仓" if txt0 == "t-v2-mclose"
                   else "平仓（引擎执行）")
            # ---- close_cause 落库（10-05，唯一真源见 core/flip.py）----
            # 优先认领翻仓事务预登记的成因（`flip.expect_close`）：
            #   翻仓 F3 平旧腿 → flip_out（含窗口期被交易所侧 SL 打掉的 B5 情形）
            #   回滚平新腿     → flip_rollback
            # 认领不到才按现有 text 推导（老路径行为完全不变）。
            # ⚠ `risk_guard`（强平）枚举**先定义但当前不产出**：实测 500 笔交易
            #   的 reason 里没有一笔能识别为强平，不凭空写识别规则（架构 Q-A）。
            close_cause = flip.take_close_cause(sym, side)
            if not close_cause:
                # 🔴 10-06 修：原 else 分支只认 `ao-` / `app`，**没有 t-v2-flip /
                #   t-v2-fliprb 分支** → 翻仓平旧腿一旦认领失败就落进 `guard_take`，
                #   把「翻仓盈亏」算进了「止盈线程」的胜率/盈亏比（实证 10-06
                #   00:04:36 XRP_USDT short：text='t-v2-flip'、pnl +0.2369U 被记成
                #   guard_take）。纯归因/统计修正，不改变任何交易动作。
                #   仍保留「先认领、认不到才按 text 推导」的优先级不变。
                close_cause = (flip.CAUSE_SL_TP if txt0.startswith("ao-")
                               else flip.CAUSE_MANUAL if txt0 == "app"
                               else flip.CAUSE_FLIP_OUT if txt0 == "t-v2-flip"
                               else flip.CAUSE_FLIP_ROLLBACK
                               if txt0 == "t-v2-fliprb"
                               # 10-07 新增：持仓管理线程的模型主动平仓必须**单独**
                               #   归因。落到 guard_take 会让「止盈线程的胜率/盈亏比」
                               #   混进模型判断，与 10-06 修 t-v2-flip 是同一个坑。
                               else flip.CAUSE_MODEL_CLOSE if txt0 == "t-v2-mclose"
                               else flip.CAUSE_GUARD_TAKE)
            # 10-04 17:45 用户拍板补字段（纯工程，不改任何策略行为）：
            # 原来 close 只有 6 个字段，entry/sl/tp 全部丢失 → 历史 240 笔无法回答
            # 「这笔是被 SL 打爆还是被 TP 打掉」「实际持仓多久」这两个止盈口径
            # 的核心问题。也不能靠「取最近一次开仓」
            # 配对—— SUI 开过 39 次仓，同品种反复开平必然配对错位，分组统计不可信。
            # 现在把这单从生到死的关键参数固化，后续复盘可直接筛选。
            # ⚠ 下面几个变量必须在 trades.append 之前取：meta 在下面 pop 之后就
            #   拿不到了（(st["open_meta"]).pop(key, None)），取晚就全是 0。
            _e0 = float(meta.get("entry") or 0)
            _sl0 = float(meta.get("sl") or 0)
            _tp0 = float(meta.get("tp") or 0)
            _topen = float(meta.get("t_at_open") or 0)
            _ctime = float(row.get("time") or time.time())
            # 🔴 10-05 修：出场价改由 `_close_px_of()` 取（原来 `row.get("price")`
            #   在 Gate 的 position_close 返回体里**不存在** → 恒为 0.0，
            #   29 条历史 close 的 exit_px 全是 0 就是这么来的）。
            #   必须在 trades.append 之前取，且与下面 journal 的 px 同源同值。
            _cpx = self._close_px_of(row, side)
            # 纯影子记账：模型/手动等提前退出后，继续观测到原始 SL/TP，
            # 估算「若不提前干预」的结果；不下单、不改 SL/TP、不影响对账。
            try:
                self._start_exit_ghost(sym, side, meta, pnl, _cpx, src, _ctime)
            except Exception as e:  # noqa: BLE001 —— 影子记账不得影响真实平仓落账
                LOG(f"  ⚠ ghost_pnl 初始化失败 {sym} {side}: {str(e)[:100]}")
            trades.append({"t": _ctime, "sym": sym,
                           "side": side, "action": "close", "pnl": round(pnl, 4),
                           "reason": src,
                           "close_cause": close_cause,
                           "entry": _e0, "sl": _sl0, "tp": _tp0,
                           "size": meta.get("size"), "sig": meta.get("sig"),
                           "risk_u": meta.get("risk_u"), "rr": meta.get("rr"),
                           "t_at_open": meta.get("t_at_open"),
                           # 🔴 10-06 补：close 记录必须带上 trade_id。
                           #   前端「今日交割行 → 订单生命」的配对原来只能拿
                           #   `t_at_open` 反推 tid，而下面这条链会让它失真：
                           #   guard 侧「meta 入场价对账」（本文件 :1334）判定
                           #   属"上一代仓"时会把 meta 的 `t_at_open` 刷成对账
                           #   那一刻 → 反推出的 tid 与 life 里那个由**首次开仓
                           #   时间**构成的 tid 对不上（实测 TSLA 差 19.6min、
                           #   SNDK 差 20.7min）→ 那一行点不动。
                           #   `trade_id` 是开仓时定下、此后**从不改写**的，与
                           #   life 的 tid 同源 → 直接透传即可精确配对。
                           #   ⚠ 必须在本行所在的 append 之前取（meta 在下面
                           #   `open_meta.pop(key)` 之后就没了）。
                           "trade_id": meta.get("trade_id"),
                           "hold_min": (round((_ctime - _topen) / 60, 1)
                                        if _topen else None),
                           "tp_dist_pct": (round(abs(_tp0 - _e0) / _e0 * 100, 3)
                                           if _e0 and _tp0 else None),
                           "sl_dist_pct": (round(abs(_e0 - _sl0) / _e0 * 100, 3)
                                           if _e0 and _sl0 else None),
                           "peak": meta.get("tp_peak") or meta.get("peak"),
                           "exit_px": round(_cpx, 8)})
            (st.get("open_meta") or {}).pop(key, None)
            deleted.add(key)
            dirty = True
            LOG(f"  📒 落账 {sym} {side} {src} 盈亏 {pnl:+.4f}U")
            # 完整流水（10-03 01:20 用户要求）：平仓全字段进 journal。
            # entry/sl/tp/峰值等从**本轮开始时的 meta 快照**取（meta 是
            # open_meta 里的记录，pop 前还在）→ 补齐"这一单从生到死"的完整轨迹。
            # ⚠ 10-09：`armed_gap` 字段随 E 段删除一并去掉（`meta["tp_gap"]` 已无
            #   任何写入点，该字段长期恒为 None；全项目零读取点）。
            try:
                _mx = float(meta.get("tp_peak") or meta.get("peak") or 0)
                _fb = float((meta.get("fee_rt") or 0))
                _mfe = ((_mx - _e0) / _e0) if side == "long" and _mx and _e0 else \
                       ((_e0 - _mx) / _e0) if _mx and _e0 else 0.0
                # 10-06 遥测：最大不利偏移（MAE，与 MFE 同口径同符号约定：
                #   正数=有利、负数=不利）。trough 由 guard 每轮朝不利方向推进。
                _tr = float(meta.get("trough") or 0)
                _mae = ((_tr - _e0) / _e0) if side == "long" and _tr and _e0 else \
                       ((_e0 - _tr) / _e0) if _tr and _e0 else 0.0
                _tsize = int(meta.get("size") or 0)
                _mult = 1.0
                try:
                    _mult = float(ex.spec(sym).get("quanto_multiplier") or 1)
                except Exception:
                    pass
                _notl = _tsize * _mult * _e0
                _t0 = float(meta.get("t_at_open") or 0)
                journal.journal_close(
                    #🔴 10-05 修：`px` 原来用 `cp`，而 `cp` 在**本try 块之后**
                    #   才赋值（`cp = float(row.get("price") or 0)`，旧:396）→
                    #   首次执行到这行必然 UnboundLocalError（NameError 子类）
                    #   → 被下面的 `except: pass` 吞掉 → **整条 journal close
                    #   事件从未落盘**。实证：runtime 的 journal 里 close 事件
                    #   只有 6 条、且 px 全是 0.0（那6 条是 10-03 旧代码写的，
                    #   当时 `cp` 恰好有值；本次改动后新增的字段它就再也没写过）。
                    #   现改用上面已取好的 `_cpx`（与 exit_px 同源同值）。
                    sym=sym, side=side, entry=round(_e0, 8), px=round(_cpx, 8),
                    size=_tsize, pnl=round(pnl, 4),
                    pnl_pct=round(pnl / _notl * 100, 3) if _notl else None,
                    gross=round(pnl + _notl * _fb, 4), fee=round(_notl * _fb, 4),
                    fee_rt=meta.get("fee_rt"), notional=round(_notl, 4),
                    sl=_sl0, tp=_tp0, mfe=round(_mfe * 100, 3),
                    peak=round(_mx, 8),
                    sl_dist_pct=round(abs(_sl0 - _e0) / _e0 * 100, 3) if _e0 and _sl0 else None,
                    tp_dist_pct=round(abs(_tp0 - _e0) / _e0 * 100, 3) if _e0 and _tp0 else None,
                    hold_min=round((time.time() - _t0) / 60, 1) if _t0 else None,
                    closed_by=src, close_cause=close_cause, text=txt0,
                    win_rate=meta.get("wr"),
                    open_reason=meta.get("reason"), open_ts=_t0,
                    close_ts=float(row.get("time") or time.time()),
                    # ---- 10-06 遥测新增（纯落账）----
                    # ① peak_reach_s：MFE 最后一次刷新距今（≈峰值到达时间）——
                    #    回答「这单走起来了没有」的关键；没有它就是拍脑袋。
                    #    取不到（老仓无 peak_t）给 None，不做任何猜测。
                    # ② mae_pct：全程最大不利偏移（负数）。
                    # ③ 两张保护单 id：与 open 事件的 sl_id/tp_id 配对，
                    #    用于判断「这笔是被 SL 还是 TP 打的」（配合 close_cause）。
                    trade_id=meta.get("trade_id"),
                    peak_reach_s=(round(max(0.0, float(meta.get("peak_t") or 0)
                                            - _t0), 1)
                                  if (_t0 and meta.get("peak_t")) else None),
                    mae_pct=round(_mae * 100, 3),
                    sl_id=meta.get("sl_id"), tp_id=meta.get("tp_id"),
                    # ---- 10-07 17:15：开仓时的候选快照（decision.py 落 meta）----
                    #   作用：让**每笔平仓**自带"当时为什么开"的完整上下文。
                    #   之前只能反过来 join open 事件，而 10-07 的实证是：
                    #   open 记录里根本没有候选信息（journal.py 的 docstring
                    #   早有 cand 契约，实现一直漏落），open_meta 也没存 ⇒
                    #   3 笔亏损复盘时**分不清是候选机制的问题还是 regime
                    #   判定的问题**。这一组字段就是补这个洞。
                    #   ⚠ 必须透传 meta（开仓时刻），**绝不能平仓时现查**——
                    #     候选 TTL 只有几小时、regime 每轮变，现查拿到的是另一个
                    #     时刻的答案，记下来的账是假的。
                    #   老仓 meta 无这些键 → 全 None，属正常降级不报错。
                    has_cand=meta.get("has_cand"),
                    cand_side=meta.get("cand_side"),
                    cand_types=meta.get("cand_types"),
                    cand_confirm_ready=meta.get("cand_confirm_ready"),
                    open_regime=meta.get("regime"),
                    open_adx=meta.get("adx"),
                    open_band_pos=meta.get("band_pos"),
                    open_walk_dn_bars=meta.get("walk_dn_bars"),
                    open_fake_dn_n=meta.get("fake_dn_n"))
            except Exception:
                pass
            # 亏后冷却（10-02 17:0x 上线，**20:02 用户拍板删除**）：同 sym|side 亏损
            # 平仓后 N 分钟拒开同向，治「亏完就重开、同一段行情反复挨打」。
            # **默认 0（关闭）**——原默认 120 会在配置缺键时意外复活该闸；
            # 现与configs/config.yaml 的 loss_cooldown_min 同源，唯一开关在配置。
            cool_min = float(self.cfg["guards"].get("loss_cooldown_min", 0))
            if pnl < 0 and cool_min > 0:
                cpath = rt + "/cooldown.json"
                cd = state.load(cpath, {})
                until = time.time() + cool_min * 60
                cd[key] = until
                state.save(cpath, cd)
                LOG(f"  ⛔ 亏后冷却 {sym} {side}：{cool_min:.0f} 分钟内拒开同向"
                    f"（至 {time.strftime('%H:%M', time.localtime(until))}）")
            # 反思进化：条件单平仓也要出教训（后台线程，防阻塞行情快照）
            # 🔴 10-05 修：`cp` 原来在这里才赋值（`float(row.get("price") or 0)`），
            #   但 journal 的 `px=round(cp, 8)` 在**上方**就要读它 → 首轮
            #   UnboundLocalError。另外 `row.get("price")` 在 Gate 的
            #   position_close 返回体里根本不存在 → 即使顺序对了也恒为 0.0，
            #   于是 `gain`（教训正文里的「价格幅度」）也会恒为 0。
            #   现统一用上面 `_close_px_of()` 取到的 `_cpx`（与 exit_px / journal px
            #   **同源同值**，三处口径不再可能漂移）。
            cp = _cpx
            entry = float(meta.get("entry") or 0)
            gain = ((cp - entry) / entry if side == "long" else (entry - cp) / entry) \
                if cp and entry else 0.0
            if meta:
                # close_cause 随 tr 透传给 review._lesson()：翻仓/回滚的平仓
                # **不生成教训**（那不是模型判断失误，是模型主动换方向 / 系统执行
                # 层问题），但**照实进trades.json 与 4 维统计**（只挡教训、绝不挡账）。
                threading.Thread(target=self._post_close, daemon=True, args=(
                    {"sym": sym, "side": side, "pnl": pnl, "meta": meta,
                     "gain": gain, "close_cause": close_cause},)).start()
        if dirty:
            state.save(rt + "/trades.json", trades)
            # 10-05：删除动作搬进 _META_LOCK 临界区，且改为**在锁内重读最新
            # open_meta 再删**。原来是把本函数开头 line 132 那份快照 st 直接
            # 交给 hf._save —— 而 _save 会「以盘上最新为基底 + 用 st 覆盖」，
            # 于是这份**陈旧快照里的每个 key** 都会把别的线程在这期间写入的
            # 新值盖回旧值（典型：guard 线程刚更新的 peak/tp_peak 被对账线程
            # 用旧快照覆盖 → 峰值回退 → 模型看到的回撤数据失真）。
            # 改成锁内重读 + 只做删除后，本次写回不再携带任何陈旧字段，
            # deleted 语义原样透传（_save 合并后强制剔除，见 _mutate_meta 注释）。
            self._mutate_meta(self._clear_meta_keys(deleted), deleted=deleted)
        # 孤儿条件单清理（10:57 事故修复）：仓位被 Gate 侧 SL/TP 打掉后，
        # 另一条腿会残留挂单（可能抢先误触发新仓）。合约已无持仓 → 全部清掉。
        #
        # 🔴 10-05 B3 第三个活跃入口（架构复核新发现，PRD 未提）：原写
        #   `ex._cancel_price_orders(sym)` **无 tag、全撤**（该旧实现已于
        #   10-05 删除——它是 B3 缺陷的原始实现，留着就是个地雷）。
        #   翻仓窗口期若新旧两腿在这 2s 内**都**消失（都被交易所侧打掉），
        #   全撤会命中该 contract 上任何残留单 —— 包括刚挂上去的新腿保护单。
        #   现改为：① 冻结期整段跳过；② 按 gone 的**方向**逐个精确撤，
        #   只碰「确实已经没有仓的那个方向」的 SL/TP。
        if gone:
            remain = {p["contract"] for p in positions}
            for gk in gone:
                gsym = gk.split("|")[0]
                if gsym in remain:
                    continue                     # 该合约还有其它腿在仓→ 不撤
                gside = gk.split("|")[1] if "|" in gk else None
                if flip.is_frozen(gsym):
                    flip.note_suppressed(gsym)
                    LOG(f"  ❄️ 孤儿撤单跳过 {gsym} {gside}：翻仓窗口期"
                        f"（冻结剩 {flip.freeze_left(gsym):.0f}s，防误撤新腿保护单）")
                    continue
                try:
                    n = ex.cancel_price_orders_side(gsym, gside)
                    if n:
                        LOG(f"  🧹 孤儿条件单清理 {gsym} {gside}：撤 {n} 张"
                            f"（该方向已无持仓，按 auto_size 精确撤）")
                except Exception:
                    pass

    def _save_exit_ghosts(self):
        try:
            state.save(self._exit_ghost_path, self._exit_ghosts)
        except Exception as e:  # noqa: BLE001 —— 影子状态失败不得中断交易主循环
            LOG(f"  ⚠ ghost_pnl 状态写入失败: {str(e)[:100]}")

    @staticmethod
    def _exit_ghost_hit(side: str, px: float, sl: float, tp: float):
        """单个观测点已越过哪个固定保护价；两价均命中时不猜先后。"""
        if px <= 0 or sl <= 0 or tp <= 0:
            return None
        if side == "long":
            hit_sl, hit_tp = px <= sl, px >= tp
        else:
            hit_sl, hit_tp = px >= sl, px <= tp
        if hit_sl and hit_tp:
            return "ambiguous"
        if hit_sl:
            return "SL"
        if hit_tp:
            return "TP"
        return None

    @staticmethod
    def _exit_ghost_cross(side: str, prev_px: float, px: float,
                          sl: float, tp: float):
        """按相邻采样判断保护价穿越；跨过两价时明确标记不可判定。"""
        if px <= 0 or sl <= 0 or tp <= 0:
            return None
        if not prev_px or prev_px <= 0:
            return Engine._exit_ghost_hit(side, px, sl, tp)
        lo, hi = sorted((prev_px, px))
        hit_sl = lo <= sl <= hi
        hit_tp = lo <= tp <= hi
        if hit_sl and hit_tp:
            return "ambiguous"
        if hit_sl:
            return "SL"
        if hit_tp:
            return "TP"
        return None

    @staticmethod
    def _exit_ghost_estimate(shadow: dict, trigger: str, target_px: float):
        entry = float(shadow.get("entry") or 0)
        size = abs(float(shadow.get("size") or 0))
        mult = float(shadow.get("mult") or 0)
        fee_rt = shadow.get("fee_rt")
        if entry <= 0 or target_px <= 0 or size <= 0 or mult <= 0:
            return None, None, None
        direction = 1.0 if shadow.get("side") == "long" else -1.0
        gross = (target_px - entry) * direction * size * mult
        fee = (entry * size * mult * float(fee_rt)
               if fee_rt is not None else None)
        net = gross - fee if fee is not None else None
        return round(gross, 6), round(fee, 6) if fee is not None else None, \
            round(net, 6) if net is not None else None

    def _finish_exit_ghost(self, trade_id: str, shadow: dict, trigger: str,
                           target_px: float, hit_px: float, method: str):
        gross, fee, net = self._exit_ghost_estimate(shadow, trigger, target_px)
        actual = shadow.get("actual_pnl")
        delta = (round(net - float(actual), 6)
                 if net is not None and actual is not None else None)
        journal.log(
            "ghost_pnl", trade_id=trade_id, sym=shadow.get("sym"),
            side=shadow.get("side"), status="final", trigger=trigger,
            target_px=target_px, hit_px=hit_px,
            entry=shadow.get("entry"), sl=shadow.get("sl"), tp=shadow.get("tp"),
            size=shadow.get("size"), mult=shadow.get("mult"),
            fee_rt=shadow.get("fee_rt"), ghost_gross_pnl_est=gross,
            ghost_fee_est=fee, ghost_net_pnl_est=net, actual_pnl=actual,
            ghost_minus_actual=delta, closed_by=shadow.get("closed_by"),
            actual_exit_px=shadow.get("actual_exit_px"), estimate_method=method,
            target_basis=shadow.get("target_basis"),
            why="只做影子估算，不触发任何交易动作")

    def _start_exit_ghost(self, sym: str, side: str, meta: dict,
                          actual_pnl: float, actual_exit_px: float,
                          closed_by: str, close_ts: float):
        meta = meta or {}
        t0 = float(meta.get("t_at_open") or 0)
        trade_id = (meta.get("trade_id") or
                    (f"{sym}|{side}|{int(t0 * 1000)}" if t0 else None))
        entry = float(meta.get("entry") or 0)
        tp = float(meta.get("tp") or 0)
        sl_dist0 = float(meta.get("sl_dist0") or 0)
        if sl_dist0 > 0 and entry > 0:
            sl = entry - sl_dist0 if side == "long" else entry + sl_dist0
            target_basis = "entry_sl_dist0_and_tp"
        else:
            sl = float(meta.get("sl") or 0)
            target_basis = "current_sl_fallback"
        mult = 1.0
        try:
            mult = float(ex.spec(sym).get("quanto_multiplier") or 1)
        except Exception:
            pass
        fee_rt = meta.get("fee_rt")
        try:
            fee_rt = float(fee_rt) if fee_rt is not None else None
        except (TypeError, ValueError):
            fee_rt = None
        shadow = {
            "trade_id": trade_id, "sym": sym, "side": side,
            "entry": entry, "sl": sl, "tp": tp,
            "size": abs(float(meta.get("size") or 0)), "mult": mult,
            "fee_rt": fee_rt, "t_at_open": t0, "closed_at": close_ts,
            "actual_pnl": float(actual_pnl),
            "actual_exit_px": float(actual_exit_px or 0),
            "closed_by": closed_by, "target_basis": target_basis,
            "last_px": float(actual_exit_px or 0),
        }
        if not trade_id:
            journal.log("ghost_pnl", sym=sym, side=side, status="unavailable",
                        actual_pnl=actual_pnl, closed_by=closed_by,
                        why="缺少 trade_id/t_at_open，无法与 exit_brief 精确配对")
            return
        if closed_by == "SL/TP 条件单触发":
            # 已由交易所保护单自然结束：用实际成交净盈亏作基准，不再估价。
            journal.log("ghost_pnl", trade_id=trade_id, sym=sym, side=side,
                        status="final", trigger="exchange_sl_tp",
                        ghost_net_pnl_est=round(float(actual_pnl), 6),
                        actual_pnl=round(float(actual_pnl), 6),
                        ghost_minus_actual=0.0, actual_exit_px=actual_exit_px,
                        closed_by=closed_by, estimate_method="exchange_realized_close",
                        target_basis=target_basis,
                        why="交易所 SL/TP 已触发，实际平仓盈亏作为影子基准")
            return
        if (entry <= 0 or sl <= 0 or tp <= 0
                or shadow["size"] <= 0):
            journal.log("ghost_pnl", trade_id=trade_id, sym=sym, side=side,
                        status="unavailable", actual_pnl=actual_pnl,
                        closed_by=closed_by, entry=entry, sl=sl, tp=tp,
                        why="缺少有效入场价、保护价或持仓张数")
            return
        if (side == "long" and not sl < entry < tp) or \
                (side == "short" and not tp < entry < sl):
            journal.log("ghost_pnl", trade_id=trade_id, sym=sym, side=side,
                        status="unavailable", actual_pnl=actual_pnl,
                        closed_by=closed_by, entry=entry, sl=sl, tp=tp,
                        why="SL/TP 与方向或入场价不匹配，拒绝生成影子结果")
            return
        first_hit = self._exit_ghost_hit(side, shadow["actual_exit_px"], sl, tp)
        if first_hit in ("SL", "TP"):
            target = sl if first_hit == "SL" else tp
            self._finish_exit_ghost(trade_id, shadow, first_hit, target,
                                    shadow["actual_exit_px"], "actual_close_price_beyond_target")
            return
        if first_hit == "ambiguous":
            journal.log("ghost_pnl", trade_id=trade_id, sym=sym, side=side,
                        status="ambiguous", actual_pnl=actual_pnl,
                        actual_exit_px=actual_exit_px,
                        why="平仓价同时越过 SL 与 TP，先后顺序不可判定")
            return
        self._exit_ghosts[trade_id] = shadow
        self._save_exit_ghosts()
        journal.log("ghost_pnl", trade_id=trade_id, sym=sym, side=side,
                    status="pending", actual_pnl=actual_pnl,
                    actual_exit_px=actual_exit_px, sl=sl, tp=tp,
                    target_basis=target_basis, closed_by=closed_by,
                    why="真实仓已结束；继续只读行情，直到固定 SL/TP 之一触及")

    def _exit_ghost_tick(self, prices: dict):
        if not self._exit_ghosts:
            return
        changed = False
        for trade_id, shadow in list(self._exit_ghosts.items()):
            try:
                point = prices.get(shadow.get("sym")) or {}
                px = float(point.get("px") or 0)
                if px <= 0:
                    continue
                sl, tp = float(shadow.get("sl") or 0), float(shadow.get("tp") or 0)
                prev = float(shadow.get("last_px") or 0)
                hit = self._exit_ghost_cross(shadow.get("side"), prev, px, sl, tp)
                if hit in ("SL", "TP"):
                    target = sl if hit == "SL" else tp
                    self._finish_exit_ghost(trade_id, shadow, hit, target, px,
                                            "2s_watcher_price_crossing_estimate")
                    self._exit_ghosts.pop(trade_id, None)
                    changed = True
                elif hit == "ambiguous":
                    journal.log("ghost_pnl", trade_id=trade_id,
                                sym=shadow.get("sym"), side=shadow.get("side"),
                                status="ambiguous", previous_px=prev, current_px=px,
                                actual_pnl=shadow.get("actual_pnl"),
                                why="相邻采样间同时跨越 SL 与 TP，先后顺序不可判定")
                    self._exit_ghosts.pop(trade_id, None)
                    changed = True
                elif px != prev:
                    shadow["last_px"] = px
                    shadow["last_t"] = time.time()
                    changed = True
            except Exception as e:  # noqa: BLE001 —— 单笔影子异常不能挡住其它仓/主循环
                LOG(f"  ⚠ ghost_pnl 跟踪失败 {trade_id}: {str(e)[:100]}")
        if changed:
            self._save_exit_ghosts()

    def _sweep_ghost_orders(self, positions: list):
        """幽灵单巡检（16:25 加固）：对账器只清「发现平仓那一刻」的残留单，
        引擎停机期间条件单自行触发、Gate 网页手动平仓等路径都会漏。
        每轮 guard 全量扫一遍：
          1. t-v2 条件单的目标方向已无持仓 → 撤（幽灵单）
          2. 同合约同方向重复 SL/TP → 保留最新一张，其余撤（防双触发）
          3. 非 t-v2 挂单（用户手动）→ 只报告不动

        🔴 10-05 B3 **第四个活跃入口**（架构复核新发现，PRD 未提）：第2 类
        按`(sym, side, text)` 分组、保留ctime **最新**的一张。翻仓窗口期内
        新腿要挂自己的 SL/TP，此时同 `(sym, side, text="t-v2-SL")` 会出现两张；
        结论上「保留新的」恰好是对的，但**若时序反了就会撤掉新腿的那张** ——
        在途挂单的结果取决于两次 API 返回的先后，不可赌。
        故：冻结期**整段跳过该 contract**（含第 1 类幽灵判定，理由同 B4：
        F1~F3 之间旧腿刚消失、新腿刚出现，正是「看起来像幽灵」的时刻）。
        """
        held: dict = {}
        for p in positions:
            held.setdefault(p["contract"], set()).add(
                "long" if float(p["size"]) > 0 else "short")
        try:
            orders = ex.list_price_orders()
        except Exception:
            return
        ghosts, groups = [], {}
        for o in orders:
            ini = o.get("initial") or {}
            sym, txt = ini.get("contract"), (ini.get("text") or "")
            auto = ini.get("auto_size") or ""
            side = ("long" if auto == "close_long"
                    else "short" if auto == "close_short" else None)
            # 🔴 冻结闸：翻仓/回滚窗口期内，本contract 的单**一律不碰**
            if sym and flip.is_frozen(sym):
                continue
            if "t-v2" not in txt:
                oid = o.get("id")
                if oid not in self._ghost_seen:
                    self._ghost_seen.add(oid)
                    LOG(f"  👻 巡检：非本系统条件单 {sym} 「{txt[:24]}」"
                        f"触发={(o.get('trigger') or {}).get('price')}（疑似手动单，不动）")
                continue
            if side and side not in held.get(sym, set()):
                ghosts.append((o, sym, side, txt))
            tag = "SL" if "SL" in txt else ("TP" if "TP" in txt else None)
            if sym and side and tag:
                # 10-02：按完整 text 分组（不再按 SL/TP 粗标签）——趋势吊灯单 text=t-v2-TSL
                # 与高频 t-v2-SL 必须分开计组，否则两者会被判成"重复 SL"互相撤掉
                groups.setdefault((sym, side, txt), []).append(o)
        for o, sym, side, txt in ghosts:
            if flip.is_frozen(sym):       # 分组后到撤单之间仍可能被冻结，双保险
                flip.note_suppressed(sym)
                continue
            try:
                ex.cancel_price_order(o.get("id"), sym)
                LOG(f"  🧹 幽灵单清理 {sym} {side} {txt.replace('t-v2-', '')} "
                    f"触发={(o.get('trigger') or {}).get('price')}（{side} 仓已不存在）")
            except Exception:
                pass
        for (sym, side, tag), os_ in groups.items():
            if len(os_) < 2:
                continue
            if flip.is_frozen(sym):
                flip.note_suppressed(sym)
                continue
            os_.sort(key=lambda x: int(x.get("ctime") or 0), reverse=True)  # 新→旧
            for o in os_[1:]:                     # 只留最新（兼容追踪推进的短暂双挂窗口）
                if flip.is_frozen(sym):
                    flip.note_suppressed(sym)
                    break
                try:
                    ex.cancel_price_order(o.get("id"), sym)
                    LOG(f"  🧹 重复{tag}清理 {sym} {side} 触发="
                        f"{(o.get('trigger') or {}).get('price')}（保留最新一张）")
                except Exception:
                    pass

    def _close_px_of(self, row: dict, side: str) -> float:
        """从 Gate `position_close` 的一行里取「本腿的**平仓成交价**」。

        🔴 10-05 修`cp` 变量 bug 时挖出的**真正根因**（比变量顺序严重得多）：

        ## 为什么不能直接用 `row.get("price")`
        Gate 的 `/futures/usdt/position_close` 返回体里**根本没有 `price` 字段**。
        实测 BTC_USDT 一行的字段全集（22 个）：
          `accum_size, contract, first_open_time, lever, leverage, long_price,
           margin_mode, max_size, pnl, pnl_dividend, pnl_fee, pnl_fund, pnl_pnl,
           short_price, side, text, time, time_us, voucher_id`
        —— `row.get("price")` **恒为 None → 恒等于 0.0**。所以：
          · `engine.py` 里两处 `row.get("price")` 产出的 `exit_px`（trades.json）
            与 `px`（journal close）**历史上一直是 0**，与 `cp` 的顺序 bug 无关。
          · 实证：`runtime/trades.json` 29 条带 exit_px 的 close **全部为 0.0**。

        ## 平仓成交价在哪：读「**另一侧**的 price 字段」
        Gate 对一次平仓返回 `long_price` 与 `short_price`：
          · 平**多**仓时 → `long_price` = 开仓价，`short_price` = **平仓成交价**
          · 平**空**仓时 → `short_price` = 开仓价，`long_price` = **平仓成交价**
        即「关掉的那条腿的**对手方向**报价」= 本腿的出场价。

        ## 两条独立路径互验（不是猜的）
          路径A（本函数，直读字段）
          路径B（用 Gate 自己的 `pnl_pnl` 反推）
                多仓：出场 = long_price + pnl_pnl/(size × quanto_multiplier)
                空仓：出场 = short_price − pnl_pnl/(size × quanto_multiplier)
        实测 **9 个品种 45 个样本，两路径 100% 一致（0 个偏差）**；
        再拿 `my_trades`（真实成交流水）按时间窗 ±120s 交叉验证，成交价
        **9/9 吻合**。三条独立证据一致，故采用路径 A（直接、精确、无累积误差），
        路径 B 只作为路径 A 取不到值时的兜底。

        ## 语义确认：这里要的确实是「成交价」而不是 mark 价
        `:253` 的 `px` 与 `:348` 的 `exit_px` 都是**落账用的出场价**——
        与 `gain = (cp − entry)/entry`（同一变量算的**实际涨跌幅**）以及
        `review._god_view` 的 MFE/MAE 口径（要真实成交价算有利/不利极值）
        完全一致。mark 价会与之不符，故**不取 mark 价**。

        取不到时返回 0.0（调用方的既有语义：`0` = 未知，不是「价格为 0」）。
        """
        try:
            lp = float(row.get("long_price") or 0)
            sp = float(row.get("short_price") or 0)
            # 路径A：读另一侧的price 字段
            v = sp if side == "long" else lp
            if v > 0:
                return v
            # 路径B兜底：用 pnl_pnl 反推（实测与路径A 完全一致）
            opx = lp if side == "long" else sp
            sz = abs(float(row.get("accum_size") or row.get("max_size") or 0))
            mult = 1.0
            try:
                mult = float(ex.spec(row.get("contract")).get("quanto_multiplier") or 1)
            except Exception:
                mult = 1.0
            pnl_pnl = float(row.get("pnl_pnl") or 0)
            if opx > 0 and sz > 0 and mult > 0:
                return (opx + pnl_pnl / (sz * mult)) if side == "long" \
                    else (opx - pnl_pnl / (sz * mult))
        except Exception:
            pass
        return 0.0

    def _last_close_row(self, sym: str, side: str):
        """Gate 平仓历史最近一条（新→旧），匹配方向"""
        try:
            from exchange import gate_client as g
            rows = g.request_authed("GET", "/futures/usdt/position_close",
                                    query={"contract": sym, "limit": 5}) or []
            for r in rows:
                r_side = "long" if str(r.get("side")) == "long" else "short"
                if r_side == side:
                    return r
        except Exception as e:  # noqa: BLE001
            LOG(f"  ⚠ 平仓落账查询失败 {sym}: {str(e)[:100]}")
        return None

    def _post_close(self, tr: dict):
        """平仓后置处理：教训（质检+归并）+ 每单判词入库（复盘线程）"""
        try:
            self.review.on_close(tr)
        except Exception as e:  # noqa: BLE001
            LOG(f"  ⚠ 复盘判词失败 {tr.get('sym')}: {str(e)[:120]}")

    def _hist_sample(self, eq: float, snap: dict):
        """30 秒一个点，供看板 sparkline（权益曲线 + 各品种价格曲线）"""
        now = time.time()
        rt = self.cfg["runtime_dir"]
        h = state.load(rt + "/hist.json", {})
        if now - float(h.get("t") or 0) < 30:
            return
        h["t"] = now
        h["equity"] = (h.get("equity") or [])[-479:] + [[now, round(eq, 4)]]
        pxh = h.setdefault("px", {})
        for sym, d in snap.items():
            if d["px"] > 0:
                pxh[sym] = (pxh.get(sym) or [])[-479:] + [[now, d["px"]]]
        state.save(rt + "/hist.json", h)

    # -------------------------------------------------- guard：保证金守护 + 孤儿仓兜底
    # ---------------------------------------- hf_state 读-改-写原子化
    def _mutate_meta(self, fn, *, deleted=None) -> bool:
        """在 _META_LOCK 内完成 open_meta 的「load最新 → 改 → save」。

        fn(st) 收到**锁内刚读出的最新** state，直接原地修改即可。
        返回 True 表示「确实发生了改动且已落盘」；返回 False 表示无需写
        （fn 返回 False 时不调 _save，避免无谓的 2s 一次全量写）。

        deleted 语义与 HfAgent._save 完全一致：HfAgent._save 会把盘上最新
        open_meta 并进来（防 guard/decision 互相覆盖），**删除动作会被并集
        吃掉** → 调用方必须显式声明本轮要删的 key，才能在合并后强制剔除。
        这里原样透传，语义不变。
        """
        with _META_LOCK:
            st = self.hf._st()          # 锁内读最新（关键：读也在临界区内）
            if not fn(st):
                return False
            self.hf._save(st, deleted=deleted)   # 内部 state.save 取 _lock（内层）
            return True

    def _clear_meta_keys(self, deleted: set):
        """临界区内的删除动作：把 deleted 里的 key 从最新 open_meta 剔除。
        返回值直接喂给 _mutate_meta 的 fn 协议（True = 有东西要删）。"""
        def _fn(st):
            om = st.get("open_meta") or {}
            hit = [k for k in deleted if k in om]
            for k in hit:
                om.pop(k, None)
            return bool(hit)
        return _fn

    def _writeback_meta(self, sym: str, side: str, meta: dict, meta_orig: dict) -> bool:
        """把本轮 guard 对 meta 的改动安全写回 open_meta（返回是否真写入）。
        10-03 20:15 修「幽灵峰值被新仓继承」bug 根因②，两层保护：
          ① 换仓保护：盘上最新记录的开仓指纹（t_at_open，加减仓都不改它）与本轮
             开头快照不一致 → 说明窗口内同 sym|side 平了又开，本轮 meta 属于上一代，
             整条（哪怕只合并字段）都会把新仓的 entry/peak/tp_peak/sl_dist0 覆盖成
             旧值 → 新仓一出生就"已经赚过又回吐"，模型据此误判。
             实证：ZEC 20:00:40 开 → 20:00:52 按上一笔的"峰值1318.27"被平；
             SKHYNIX 15:48:35 开 → 15:48:47 按"峰值1370.49"被平（开仓日志 48 秒才打）。
             此时放弃回写（下一轮重读新仓，最多少写一轮峰值，安全侧）。
          ② 字段级合并：只提交本轮真正改动的字段，不整条覆盖盘上最新记录
             （10-03 01:15 G1 修复：整条覆盖会回滚 hf 线程的加仓/新开仓写入）。"""
        key_m = f"{sym}|{side}"
        fp_now = meta_orig.get("t_at_open") or meta_orig.get("entry")
        delta = {k: v for k, v in meta.items()
                 if meta_orig.get(k, object()) != v}
        if not delta:
            return False          # 本轮无字段变化，连锁都不必进（2s 一轮的最快路径）

        # 10-05：整段「读盘上最新 → 换仓指纹比对 → 字段级合并 → 写回」搬进
        # _META_LOCK 临界区。原来读在锁外、写在锁内，中间的比对+合并是基于
        # **陈旧快照**做的：此刻 hf 线程若正好加仓（写 entry/size/sl），
        # 本函数会用旧 entry 做指纹比对、拿旧 prev 做基底，回写时把对方的
        # 新字段覆盖回旧值（静默丢数据）。移进锁内后，读到的 prev 一定
        # 是「比对那一刻」的盘上最新值，指纹比对与合并基底同源。
        # 指纹比对逻辑、字段级合并语义、返回值语义全部保持原样，未改任何策略行为。
        def _fn(st):
            prev = dict(st.setdefault("open_meta", {}).get(key_m) or {})
            fp_prev = prev.get("t_at_open") or prev.get("entry")
            if (not prev) or (fp_prev != fp_now):
                LOG(f"  🧯 换仓保护 {sym} {side}：本轮快照属上一代仓"
                    f"（盘上指纹 {fp_prev} vs 本轮 {fp_now}）→ 放弃回写，"
                    f"防新仓继承旧峰值")
                return False
            st["open_meta"][key_m] = {**prev, **delta}
            return True
        return self._mutate_meta(_fn)

    def _atr15(self, sym: str) -> float:
        """现算策略周期 ATR（裸仓兜底补挂 SL 用）。周期取 `boll.hf_tf_min`，
        10-07 02:2x 由写死的 15 分钟改为跟随配置（改周期不再需要动这里）。

        为什么不读 scan.json（10-05 修）：scan.json 的唯一写入者是 hf 线程
        （60s 一轮），而本方法在 guard 线程（2s 一轮）被调用 → 单边急涨/急跌
        时读到的 atr15 最多已陈旧 60 秒，用它算出的保护性 SL 距离会偏
        （ ATR 放大时距离偏小 → 刚补上就被正常波动扫掉；ATR 收缩时偏大 →
        风险敞口长时间超出应有水平）。实测 live.json 里并无 atr15 字段
        （只有 t/tickers/positions/hf_keys/regime/bias），watcher 那条路走不通，
        故 guard 自己按需现算。

        口径与 hf 线程保持一致：同为 15 分钟周期、同为 ATR14。
        feeder.closed_bars 自带 45s 缓存（与 hf 线程共用同key）→ 命中缓存时
        本方法近乎零成本（实测亚毫秒），不会拖慢 guard 的 2s 轮询。

        **失败必须静默退化**：拿不到就返回 0.0，由调用方退回 entry×0.5%
        （与改造前 scan.json 缺失时的行为完全一致）。绝不向上抛异常——
        异常会中断整个裸仓补挂流程，那正是最需要它工作的场景。
        """
        try:
            from core.indicators import atr as _atr
            from market import feeder
            bars = feeder.closed_bars(
                sym, int(self.cfg["boll"]["hf_tf_min"]), days=7, n=45)
            # atr() 内部按尾部 n 根算 TR，至少要有 n+1 根才有意义
            return float(_atr(bars, 14)) if len(bars) >= 15 else 0.0
        except Exception as e:  # noqa: BLE001
            LOG(f"  ⚠ {sym} ATR15 现算失败（退回 entry×0.5% 兜底）: "
                f"{type(e).__name__}: {str(e)[:100]}")
            return 0.0

    # ---------------------------------------- 保护性 SL 补挂（唯一实现）
    # 裸仓巡检与孤儿仓兜底**共用**这一份放置逻辑（用户要求：不要另写一套，
    # 避免口径分叉）。`idx` 传入预取的挂单索引即可**省掉 has_price_order 的
    # 一次 HTTP** —— 孤儿兜底一次就能扫全部仓，必须走这条路。
    def _place_protective_sl(self, sym: str, side: str, entry: float,
                             sl_target: float = 0.0, tp_target: float = 0.0,
                             idx: dict = None, px_hint: float = 0.0) -> str:
        """补挂保护性 SL。返回新 SL 单 id（空串 = 已有/失败）。

        sl_target > 0 → 直接用调用方给的 SL 价（孤儿兜底：用 meta 里的 sl）
        sl_target == 0 → 按 1×ATR15 现算、兜底 entry×0.5%（裸仓巡检口径）
        """
        try:
            if idx is not None:
                # 用预取索引判断「是否已有 SL」——**零 HTTP**
                if "SL" in (idx.get(f"{sym}|{side}") or set()):
                    return ""
            elif ex.has_price_order(sym, "SL", side):
                return ""            # 已有 SL（多半是 open_meta 丢了但单还在）
            sl_ = sl_target
            if not sl_ or sl_ <= 0:
                atrv = self._atr15(sym)
                dist_ = atrv if atrv > 0 else entry * 0.005
                px_ = px_hint or feeder_px(sym)
                buf_ = entry * 0.001
                sl_ = (max(entry + dist_, px_ + buf_) if side == "short"
                       else min(entry - dist_, px_ - buf_))
            try:
                sl_ = ex._round_px(sym, sl_)
            except Exception:
                pass
            sid_, _t = ex.place_tpsl(sym, sl_px=sl_, tp_px=(tp_target or None),
                                     is_long=(side == "long"))
            return sid_ or ""
        except Exception as e:
            LOG(f"  ⚠ 补挂保护性SL 失败 {sym}: {str(e)[:100]}")
            return ""

    # ---------------------------------------- 裸仓补挂（保命，2s 轮询）
    # 10-05 用户拍板：裸仓巡检从 heavy 门控里拿出来，发现窗口 129s → ~2s。
    # 本方法**沿用**原有补挂实现（1×ATR15 保护性 SL，兜底 entry×0.5%），
    # 实际放置交给上面的 `_place_protective_sl`（与孤儿兜底同源）。
    #
    # 成本控制（用户要求 2：guard 已 215% 占用比，加这条路径必须实测）：
    #   判定所需数据（positions / open_meta / entry）**全部由调用方本轮取好**，
    #   本方法零额外读盘；唯一新增开销是 `has_price_order()` 的**一次 HTTP**。
    #   为防「某个 App 手动仓长期无 meta」导致每 2s 打一次 Gate，用 `_bare_cool`
    #   按 `sym|side` 限流：**首次发现立即处理**（这正是最危险的时刻），
    #   之后 60s 内不重复。稳态开销 = 1 次 HTTP / 60s / 裸仓。
    def _recover_bare_position(self, p: dict, sym: str, side: str,
                               entry: float) -> bool:
        """裸仓补挂保护性 SL。返回是否真的补挂了。

        调用位置：`for p in positions` 循环里 `sl <= 0 or entry <= 0` 的分支。
        调用方已保证：不在翻仓冻结期（`flip.is_frozen(sym)` 为假）。
        """
        key = f"{sym}|{side}"
        now = time.time()
        cool = float(self.cfg.get("bare_cool_s", 60))
        last = float(self._bare_cool.get(key) or 0)
        if now - last < cool:
            return False            # 限流命中：这一轮不打 Gate
        self._bare_cool[key] = now
        sid = self._place_protective_sl(
            sym, side, entry, sl_target=0.0, tp_target=0.0,
            px_hint=float(p.get("mark_price") or 0))
        if sid:
            # 🔴 10-05 修：`abs_size` 原来在**下方**才赋值，
            #   而这里先用到它 → NameError 被本try 的 `except` 吞掉 →
            #   **裸仓 🚨 告警与 journal 异常记录双双静默丢失**。
            #   取值：就用**本仓自己的 size**（`p` 在本参数里就是这一条腿）。
            _b_size = abs(float(p.get("size") or 0))
            journal.log("anomaly", kind="bare_position",
                        sym=sym, side=side, size=_b_size, entry=entry,
                        why="有仓但引擎无记录且无SL条件单")
            LOG(f"  🚨 裸仓巡检 {sym} {side} 引擎无记录且无SL → 补挂保护性SL"
                f"（不回填open_meta=不进动态管理，仅保命）")
            return True
        return False

    # ---------------------------------------- 孤儿仓兜底（外部删 SL）
    # 与裸仓巡检的本质差异（决定了它做不到「零成本 2s」）：
    #   裸仓：`meta.sl == 0` → **纯本地可判**，稳态根本不进分支 → 稳态 0 HTTP。
    #   孤儿：meta 完好、但**交易所侧**的 SL 条件单被外部删了（App 手删、
    #         「撤旧成功挂新失败」等）→ **本地无从得知**，只能问交易所。
    # 所以孤儿兜底的「检查频率」= 它的「发现延迟」，二者是同一个旋钮。
    #
    # 🔴 成本关键（用户要求 5，核心技术点）：**9 个仓共用 1 次 HTTP**。
    #   逐仓调 `has_price_order(sym,...)` 每次都是一次独立
    #   `GET /price_orders?contract=X` → 9 仓 = 9 次 HTTP。
    #   而 `list_price_orders()`（**不带 contract**）一次就返回全部挂单
    #   —— 拉一次、在内存里建索引、逐仓比对 → **9 仓 1 次 HTTP**。
    #   这是把成本压到 1/9 的关键（也是能敢把频率提到 10s 的前提）。
    def _orphan_sl_sweep(self, positions: list) -> int:
        """孤儿仓兜底：交易所侧 SL 单丢失 → 补挂。返回补挂数。

        每 `orphan_check_s` 秒跑一次（默认 10s，见 configs/config.yaml）。
        不改 `guard_heavy_every`（用户要求 5）—— 那是所有重逻辑的总闸，
        本函数是独立的、按自己的周期走。
        """
        per = float(self.cfg.get("orphan_check_s", 10))
        now = time.monotonic()
        if now < self._orphan_next:
            return 0                      # 还没到检查周期
        self._orphan_next = now + max(1.0, per)
        # 一次 HTTP 拿全部挂单（带超时保护：失败就跳过本轮，绝不影响 guard）
        try:
            orders = ex.list_price_orders()
        except Exception as e:
            LOG(f"  ⚠ 孤儿兜底本轮跳过：拉挂单失败 {str(e)[:80]}")
            return 0
        if not isinstance(orders, list):
            return 0
        # 建索引：{"CONTRACT|side": {"SL","TP"}}（**零 HTTP**，全在内存里比对）
        idx: dict = {}
        for o in orders:
            if not isinstance(o, dict):
                continue
            ini = o.get("initial") or {}
            c = ini.get("contract")
            if not c:
                continue
            txt = (ini.get("text") or "")
            auto = ini.get("auto_size") or ""
            sd = ("long" if auto == "close_long"
                  else "short" if auto == "close_short" else None)
            #⚠ tag 必须用 `split("-")[-1]`（`t-v2-SL` → `SL`）而**不是**
            #   `split("-", 1)[-1]`（那会得到 `v2-SL`，导致索引里永远没有 "SL"
            #   → 每个仓都被误判成孤儿 → 每周期补挂一次，恒定浪费 HTTP）。
            #   取末段顺带保证 `t-v2-TSL`（趋势吊灯单）解析成 `TSL` 而非 `SL`，
            #   不会被误当成 SL（与 has_price_order 的精确等值口径一致）。
            if sd is None or not txt.startswith("t-v2-"):
                continue
            idx.setdefault(f"{c}|{sd}", set()).add(txt.split("-")[-1])
        n = 0
        for p in positions:
            try:
                sym = p["contract"]
                side = "long" if float(p["size"]) > 0 else "short"
            except Exception:
                continue
            meta = (self.hf._st().get("open_meta") or {}).get(f"{sym}|{side}") or {}
            sl = float(meta.get("sl") or 0)
            if sl <= 0:
                continue          # 裸仓走另一条路径（_recover_bare_position）
            # 🔴 冻结闸（B3）：翻仓窗口期内一切退出/撤挂类动作都必须冻结
            if flip.is_frozen(sym):
                flip.note_suppressed(sym)
                continue
            # 交易所侧还有 SL → 无需干预；丢了才补挂。
            # （10-09 删机械护盈段后，`sl_missing` 本地标记已无写入方 —— 那条
            #   「本地已知缺失、免 HTTP 直补」的捷径随之删除，统一以交易所索引为准。）
            if "SL" in (idx.get(f"{sym}|{side}") or set()):
                continue
            # 用 meta 里的 SL 价补挂（不是 1×ATR15 —— 仓位本来就有既定 SL）
            tp_t = 0.0 if "TP" in (idx.get(f"{sym}|{side}") or set()) \
                else (float(meta.get("tp") or 0) or 0.0)
            sid = self._place_protective_sl(sym, side, float(p.get("entry_price") or 0),
                                            sl_target=sl, tp_target=tp_t, idx=idx)
            if sid:
                n += 1
                eng_meta = dict(meta)
                eng_meta["sl_id"] = sid
                self._writeback_meta(sym, side, eng_meta, meta)
                LOG(f"  🆘 孤儿仓兜底 {sym} {side} 补挂 SL {sl:g}（交易所侧 SL 单丢失）")
        return n

    def _tlm_mark(self, sym: str, side: str, meta: dict, p: dict, px: float):
        """开仓后按分段节奏持续采浮盈，写 journal `ev=mark`（看板盈亏曲线的数据源）。

        纯旁路遥测：**不参与任何决策**，写失败由 journal.log 自己吞掉。
        进度记在内存 `self._tlm_seen["sym|side"] = (trade_id, last_at, n)`：
          · 用 trade_id 校验 —— 同 sym|side 平了又开（翻仓）时自动从 0 重来；
          · **last_at = 上次采样的「名义秒数」**，不是真实时刻。目标时刻按
            `last_at + interval` 递推，**不拿真实 age 累加** → 长持仓不累积漂移
            （若用 age 累加，每轮多出的 0~2s 会让点越落越密、末段挤成一团）；
          · 不落盘 —— 见 __init__ 里该字段的注释（落盘会漏 → 重复采样）。
        重启后内存为空 → last_at=0 从头采；`age <= target + _TLM_GRACE_S` 把
        「重启后早已过期的档位」只推进不写（否则会补采出一堆同一现价的噪声）。
        宽容窗口取绝对量而非倍数：guard 单轮实测可达 4.5s（9 仓 × 查挂单），
        倍数窗口在 15s 档只有 22.5s，会被慢轮误伤丢样本。
        """
        tid = meta.get("trade_id")
        t0 = float(meta.get("t_at_open") or 0)
        if not tid or t0 <= 0:
            return
        key = f"{sym}|{side}"
        prev = self._tlm_seen.get(key)
        last_at, n = (prev[1], prev[2]) if (prev and prev[0] == tid) else (0.0, 0)
        age = time.time() - t0
        upnl = float(p.get("unrealised_pnl") or 0)
        marg = float(p.get("margin") or 0)
        pk = float(meta.get("tp_peak") or meta.get("peak") or 0)
        while n < _TLM_MAX_N:
            iv = _tlm_iv(age)
            if last_at > 0:
                target = last_at + iv
                if age < target:
                    break
                # 超宽容窗口 = 这档早就该采而没采（重启/进程卡住后的陈旧档位），
                # **只推进不写** —— 补出来的是同一现价的噪声，不是那时刻的真实观测。
                writable = age <= target + _TLM_GRACE_S
            else:
                # 🔴 重启后首次采样：**不等下一个整档位，立刻写一条起点**。
                #   向下对齐到最近档位只是为了让 offset 序列好看；早写是因为
                #   看板曲线需要起点 —— 而 iv 在长持仓段是 600s，等下去就是干等
                #   10 分钟才出现第一个点（实测老仓 age=11641s 卡在这）。
                #   中间被跳过的档位**不补**：补出来全是同一现价，见上面那条注释。
                if age < iv:
                    break
                target = int(age // iv) * iv
                writable = True
            if writable:
                journal.log(
                    "mark", trade_id=tid, sym=sym, side=side,
                    age_s=round(age, 1), offset=int(target), iv=iv,
                    px=round(px, 8), upnl=round(upnl, 4),
                    upnl_pct_margin=(round(upnl / marg, 4) if marg > 0 else None),
                    peak=round(pk, 8) if pk else None,
                    dd_from_peak_pct=(round(((pk - px) if side == "long"
                                             else (px - pk)) / px * 100, 4)
                                      if (pk and px > 0) else None),
                    sl=meta.get("sl"), tp=meta.get("tp"),
                    hold_min=round(age / 60, 2), _tlm_i=n + 1)
            last_at = target
            n += 1

        self._tlm_seen[key] = (tid, last_at, n)

    def _maint_rate(self, sym: str, row: dict = None) -> float:
        """维持保证金率 —— **逐仓动态取**，不写死（三级优先）。

        优先级（越靠前越准，全部来自交易所，零硬编码）：
          ① `row["maintenance_rate"]` —— **该仓实际适用的风险档**。Gate 会随
             仓位大小把所用档抬上去（实测：选 50x 时 DOGE/SKHYNIX/HYPE 的
             合约基础档 0.83% vs 持仓实际档 **1.20%**，TSLA/BNB 0.50/0.66% vs
             **1.00%**）。持仓行本来就取好了 → 零额外 IO。
          ② `ex.spec(sym)["maintenance_rate"]` —— 合约**基础档**（`spec()` 自带
             `_spec_cache` → 零额外 HTTP）。仅当 ① 缺失时用。
          ③ `guards.margin_guard_mr_fallback` —— 前两者都拿不到才用（配置项）。

        ⚠ 旧版这里（保证金守护段）是裸常数 `mr = 0.005`。而真实口径远不止 0.5%：
          实测有效档 ≈ 0.57%~0.93%，基础档 0.50%~1.00%，持仓档最高 1.20%
          → 旧常数**高估强平距离**，真实强平已进 SL 之内时本闸也不会报警，
          正好废掉它唯一的职责。注意交易所返回的是**字符串**（`'0.005'`），
          必须 `float()` 转换。
        """
        try:
            mr = float((row or {}).get("maintenance_rate") or 0)
            if mr > 0:
                return mr
        except Exception:
            pass
        try:
            mr = float(ex.spec(sym).get("maintenance_rate") or 0)
            if mr > 0:
                return mr
        except Exception:
            pass
        return float(self.risk.cfg["guards"]["margin_guard_mr_fallback"])

    def _guard(self):
        # 10-02 用户拍板：止损跟随提到 10 秒一轮（原 60 秒漏采分钟级尖峰、推进滞后）；
        # 重活（幽灵单巡检/保证金守护/孤儿兜底）按 guard_heavy_every 轮一次，防 API 放大 6 倍
        n = int(getattr(self, "_guard_n", 0)) + 1
        self._guard_n = n
        every = max(1, int(float(self.cfg.get("guard_heavy_every") or 6)))
        heavy = (n == 1) or (n % every == 0)
        # 🔴 10-05 修（空快照灾难链，用户批准）：改用 `positions_strict_rows()`。
        # 原 `ex.positions()` 在「HTTP 200 + 空 body」时会静默返回 `[]`
        # （gate_client.py:109 的 `or {}` → `or []`），此时本函数会：
        #   · `_sweep_ghost_orders(positions)` 的 `held={}` → 每条t-v2 条件单
        #     都判成幽灵 → **撤掉全部 9 仓的 SL/TP → 全部裸奔**（最坏结果）
        #   · `for p in positions` 空转 → 什么都不做（这个反而无害）
        # `ok=False` 时**整轮 return，一个撤单/写盘动作都不做**。
        # ⚠ 4xx/5xx/超时 抛 RuntimeError 被 `_loop` 接住 → 整轮跳过，本来就安全；
        #   只有「200+空 body」这条窄路会走到最坏结果，本守卫就是堵它。
        # ⚠ 用 `positions_strict_rows()`（保留原始行）而**不是** `positions_strict()`
        #   （legs）：本函数要用 `entry_price` / `mark_price` / `margin` /
        #   `leverage`，legs 里没有 → 硬塞会让 entry=0 → 每个仓都被当裸仓补挂 SL，
        #   **比原 bug 更糟**。字段清单见 executor.positions_strict_rows 的 docstring。
        # ⚠ 零新增 IO：与原来的 `ex.positions()` 是**同一次 HTTP**（同接口同参数），
        #   只多了返回值类型不同，guard 2s 轮询的热路径预算不受影响。
        _ok, positions, _perr = ex.positions_strict_rows()
        if not _ok:
            # 可检索告警（今天 0 次 = 路径可达但未观测到；将来真发生能统计）
            LOG(f"  🚫 guard 本轮中止：持仓快照不可信（{_perr}）"
                f"——绝不基于「拉不到」推断「没有仓」，本轮不撤任何条件单、"
                f"不动 open_meta（下一轮 {self.cfg.get('guard_interval_s')}s 后自然重来）")
            try:
                journal.log("anomaly", kind="positions_unavailable",
                            where="guard", err=str(_perr)[:200],
                            why="positions_strict ok=False，整轮跳过，不撤单不删meta")
            except Exception:
                pass
            return
        if heavy:
            # 顺手清过期的待认领平仓成因（_CAUSE 天然有 TTL，不清会无限增长）。
            # 纯内存 dict 扫描，µs 级；**不新起线程**（MEMORY：起线程必须在
            # 函数定义之后，且本模块的线程编排已定型，不为一个清字典新增线程）。
            try:
                _exp = flip.sweep_expired(
                    float((self.cfg.get("flip") or {}).get("cause_ttl_s", 120)))
                if _exp:
                    LOG(f"  🧹 翻仓成因清理：过期 {_exp} 条")
            except Exception:
                pass
            self._sweep_ghost_orders(positions)   # 幽灵单巡检（全量查挂单，降频）
        # 孤儿仓兜底（10-05 脱离 heavy 门控）：自己按 `orphan_check_s` 周期走，
        #   一次 HTTP 扫全部仓。放在主循环**之前**、独立于逐仓逻辑。
        try:
            self._orphan_sl_sweep(positions)
        except Exception as e:
            LOG(f"  ⚠ 孤儿兜底本轮异常（不影响 guard）: {str(e)[:100]}")
        # 遥测进度表只保留本轮真实持仓的 key（平仓/换仓留下的残条在这里清掉；
        # 9 个 key 的 dict 过滤，µs 级，不进任何决策路径）。
        _tlm_cur = {f"{x['contract']}|"
                    f"{'long' if float(x['size']) > 0 else 'short'}"
                    for x in positions}
        if len(self._tlm_seen) > len(_tlm_cur):
            for _k in [k for k in self._tlm_seen if k not in _tlm_cur]:
                self._tlm_seen.pop(_k, None)
        for p in positions:
            sym = p["contract"]
            side = "long" if float(p["size"]) > 0 else "short"
            meta = (self.hf._st().get("open_meta") or {}).get(f"{sym}|{side}") or {}
            # 本轮开头快照（10-03 20:15 修，幽灵峰值 bug 根因②）：两个用途 ——
            #   ① 回写前与盘上最新记录比"开仓指纹"，判定本轮 meta 是否已被换仓顶替；
            #   ② 回写时只提交本轮真正改动的字段，不再整条覆盖盘上最新记录。
            meta_orig = dict(meta)
            entry = float(p.get("entry_price") or 0)
            sl = float(meta.get("sl") or 0)
            if sl <= 0 or entry <= 0:
                # ---- 全局裸仓巡检（10-03 00:00 用户拍板修①，堵 23:16 ADA裸奔 20 分钟的洞）----
                # 有仓但引擎无记录（open_meta 丢失：maker 迟成交误判 0 张 + 引擎重启），
                # 原逻辑直接 continue = 完全不管。现补保护性 SL 并打 🚨。
                # SL 距离 = 1×ATR15（现算，见 _atr15），无数据兜底 entry×0.5%；
                # 并钳制到现价正确一侧（Gate 400 AUTO_TRIGGER_PRICE 防护）。
                # ⚠ 不回填open_meta：保持「账外仓不进引擎动态管理」的隔离语义，仅保命。
                #
                # 🔴 10-05 用户拍板：把裸仓巡检**从 heavy 门控里拿出来**。
                # 改前 `if heavy and ...` → `guard_heavy_every=30` × 2s = **60s**
                # 才查一次；叠加 guard 占用比 215%（实测一轮 heavy 4.3s）→ **最坏
                # 发现延迟 129s**。历史实盘实证：XRP 17:07:36 丢 SL、17:09:19
                # 才补挂，**裸奔 103s**（MEMORY 里挂了多日「未修，待拍板」）。
                # 现在**每轮（约 2s）都查** ⇒ 裸奔窗口从 129s 降到 ~2s。
                #
                # 为什么这样改**不会**把重逻辑拉进 2s 循环（用户要求 1）：
                #   · 本分支末尾就是 `continue` ⇒ D 段兜底 / 保证金守护
                #     **一个都不会跑**（它们在 `continue` 之后）。
                #   · 判定本身**零新增 IO**：`positions` 与 `open_meta` 在本轮
                #     主循环里**本来就已经取好了**（上面那行 `self.hf._st()` 是既有
                #     代码，每轮每仓都调），轻量路径只是复用，不额外读文件。
                #   · 唯一新增开销是 `has_price_order()`（一次 HTTP）——已用
                #     `_bare_cool` 按 symbol 限流，见 `_recover_bare_position`。
                # ⚠ **不**改 guard_heavy_every / guard_interval_s（用户要求 5）：
                #   那会把所有重逻辑的开销一起放大。
                if flip.is_frozen(sym):
                    # 翻仓/回滚窗口期内**必须跳过**（B3）：F1 已开新腿但 F4 未落账
                    # 时，新腿在 open_meta 里没有记录 → 会被误判成裸仓。此时补挂
                    # 会与 F2 自己的保护单打架，且冻结期的一切退出类动作都该冻结。
                    flip.note_suppressed(sym)
                else:
                    self._recover_bare_position(p, sym, side, entry)
                continue
            # ================= 🔴 翻仓/回滚冻结闸（B3 主防护）=================
            # 位置铁律：**在裸仓巡检之后、止盈兜底之前**。
            #   · 裸仓巡检是**保命**动作不是退出动作，冻结期**必须保留**——
            #     F1 刚开出新腿、meta 还没落F4，这一刻它看起来就是「账外仓」，
            #     若冻结掉它，新腿就真裸奔了（那比误撤SL 更糟）。
            #   · 兜底 / D 段 / 保证金守护全部是**退出类**动作，
            #     冻结期一律跳过 —— 它们会撤旧腿的条件单，而同contract 上
            #     新旧两腿的 SL 单 text **完全相同**（`t-v2-SL`），
            #     只按 text 撤就会**误撤新腿刚挂的 SL**（最高危点 B3）。
            #
            # 冻结期**允许**做的（PRD 3.3.2）：读持仓、更新 peak/tp_peak、
            # _writeback_meta。这正是「guard 冻结不是决策门控」的体现 ——
            # 它仍然在跟着这个仓的峰值，只是不做退出动作。
            # 判据是**一次纯 dict 查找**（`_FROZEN.get(sym, 0) > monotonic()`），
            # 无锁、无 IO、无JSON 序列化；guard 2s ×每仓多约 50ns，
            # 且**粒度=contract**，BTC 冻结不影响 ADA 的任何动作。
            if flip.is_frozen(sym):
                flip.note_suppressed(sym)
                _fsnap = dict(meta)
                _fpx = float(p.get("mark_price") or 0) or feeder_px(sym)
                _fchg = False
                if _fpx > 0:
                    _pk0 = float(meta.get("peak") or 0)
                    _tp0 = float(meta.get("tp_peak") or 0)
                    _tr0 = float(meta.get("trough") or 0)
                    _pk = max(_pk0 or entry, _fpx) if side == "long" \
                        else min(_pk0 or entry, _fpx)
                    _tp = max(_tp0 or entry, _fpx) if side == "long" \
                        else min(_tp0 or entry, _fpx)
                    _tr = min(_tr0 or entry, _fpx) if side == "long" \
                        else max(_tr0 or entry, _fpx)
                    if _pk != _pk0:
                        meta["peak"] = _pk
                        meta["peak_t"] = time.time()    # 遥测：峰值刷新时刻
                        _fchg = True
                    if _tp != _tp0:
                        meta["tp_peak"] = _tp
                        meta["peak_t"] = time.time()
                        _fchg = True
                    if _tr != _tr0:
                        meta["trough"] = _tr            # 遥测：最大不利偏移
                        _fchg = True
                if not meta.get("sl_dist0"):
                    meta["sl_dist0"] = round(abs(entry - sl), 8)
                    _fchg = True
                if _fchg:
                    # 只提交本轮真正改动的字段（peak/tp_peak/sl_dist0），
                    # 走与正常路径完全相同的换仓指纹保护，绝不整条覆盖。
                    self._writeback_meta(sym, side, meta, _fsnap)
                _t = flip.get(sym)
                LOG(f"  ❄️ 冻结中 {sym} {side}：翻仓/回滚窗口期"
                    f"（剩 {flip.freeze_left(sym):.0f}s，阶段 "
                    f"{_t.phase if _t else '?'}）→ 跳过全部退出类动作"
                    f"（兜底/保证金守护），"
                    f"仅更新峰值")
                continue
            sl_dist = abs(entry - sl)
            # 10-03 01:50 方案 B：**冻结初始 SL 距离**（sl_dist0）。
            #   开仓那一刻的 SL 距离，首次见到该仓时落盘、后续只读。
            #   机械跟随段（会推动 SL）已于 10-09 删除，现在它只作为**开仓时的
            #   风险基准**留档（前端「止损距离」与复盘口径用），不参与任何判定。
            changed = False                        # 本轮 meta 是否有改（含兜底补挂）
            # ---- meta 入场价对账（10-03 20:40 补，幽灵峰值的最后一个入口）----
            #   meta 里的 entry 与交易所实际入场价对不上 → 该记录属于**上一代仓**
            #   （典型：maker 挂单在引擎停机期间成交、或网页手动开的仓），它带着
            #   上一代的 peak/tp_peak → 新仓一出生就"已经赚过又回吐"（假峰值）。
            #   实测 20:34：ZEC 实际入场 1311.22，meta 却记着上一代的 1310.85 /
            #   峰值 1313.18（+0.15%）——正是这个洞。
            #   处理：按交易所实际入场价重建峰值与冻结 SL 距离（保守侧：宁可重新
            #   追峰值，也不能让假峰值污染模型看到的回撤）。容差 0.02%（覆盖 API 差异）。
            #
            # 🔴 10-06 修（先判「张数」再决定是不是换仓）：原实现只要均价失配就
            #   一律判「上一代仓」，重建峰值并把 `t_at_open` 刷成现在。实测今天
            #   9/9 条 `meta_entry_mismatch` 全是**误判**——`real_entry` 全带长尾
            #   小数（1.492857142857 / 380.676666666667）= 加权均价，即金字塔加仓
            #   摊平所致；hf 在 decision.py 里 `tot_avg=(entry0*size0+fill*add)/tot`
            #   写回 entry，**必经这里**。误判代价三条：
            #     ① `t_at_open` 被刷 → 换仓保护指纹 / hold_min / 前端交割行↔订单
            #        生命配对（行点不动）全毁；
            #     ② `peak` 被打回 entry → 真实峰值被吞，模型再也看不到"曾经赚过"；
            #   真判据只能是**张数**：加仓必让交易所张数变大。三种情形分开处理。
            _me = float(meta.get("entry") or 0)
            if _me and entry and abs(_me - entry) / entry > 0.0002:
                _msz = float(meta.get("size") or 0)
                _psz = abs(float(p["size"] or 0))
                if _msz > 0 and _psz > _msz:
                    # 交易所张数在涨 → 加仓刚成交、meta 还没落盘 = **同一代仓**。
                    # 只同步均价口径；`peak`/`t_at_open`（峰值与身份）一律不碰。
                    meta.update(entry=entry, size=_psz)
                    changed = True
                    try:
                        journal.log("anomaly", kind="meta_entry_add", sym=sym,
                                    side=side, meta_entry=_me, real_entry=entry,
                                    meta_size=_msz, real_size=_psz,
                                    why="张数变大→判同一代仓的金字塔加仓，只同步均价")
                    except Exception:
                        pass
                    LOG(f"  🧭 {sym} {side} meta 入场价 {_me:g} ≠ 实际 {entry:g}"
                        f"（差 {abs(_me - entry) / entry:.3%}），但张数 "
                        f"{_msz:g}→{_psz:g} 变大 → 判**加仓**（同一代仓），"
                        f"只同步均价，峰值/开仓时刻不动")
                elif _msz > 0 and _psz < _msz:
                    # meta 比持仓快照**新**（快照取自本轮开头、循环体可能晚几秒，
                    #   hf 已把加仓后的均价落盘）→ 纯时机差，一个字段都不动，
                    #   下一轮两边自然一致。此处**绝不能**拿旧快照覆盖新 meta。
                    LOG(f"  🧭 {sym} {side} 均价失配（{_me:g} vs 快照 {entry:g}）"
                        f"但 meta 张数 {_msz:g} > 快照 {_psz:g} → meta 更新在快照"
                        f"之后，本轮不动")
                else:
                    # 张数一致（或两侧缺 size）而均价不同 → 保持原判定：上一代仓。
                    _old_peak = meta.get("peak")
                    meta.update(entry=entry, peak=entry, tp_peak=entry,
                                size=_psz, t_at_open=time.time(),
                                # 身份重建 ⇒ 旧 trade_id 必然指向上一代，作废
                                #   （留 None 让前端落到「平仓时刻配对」那一级，
                                #   别拿旧 tid 配到错误的订单上）。
                                trade_id=None,
                                sl_dist0=round(abs(entry - sl), 8))
                    changed = True
                    try:
                        journal.log("anomaly", kind="meta_entry_mismatch", sym=sym,
                                    side=side, meta_entry=_me, real_entry=entry,
                                    meta_size=_msz, real_size=_psz,
                                    old_peak=_old_peak,
                                    why="张数一致但均价不同，判定属上一代仓，峰值重建")
                    except Exception:
                        pass
                    LOG(f"  🧭 {sym} {side} meta 入场价 {_me:g} ≠ 实际 {entry:g}"
                        f"（差 {abs(_me - entry) / entry:.3%}，张数同为 {_psz:g}）"
                        f"→ 判定属上一代仓，峰值按实际入场价重建")
            # 10-03 20:15 修：本段必须放在 changed=False **之后**——原写法先置
            #   changed=True 再被下一行重置为 False，sl_dist0 的首次落盘被吞掉，
            #   下一轮又是"首次见到"→ 每轮把当时的 sl_dist 当初始值写一遍。
            #   （decision 开仓时已写 sl_dist0，本处只兜底老仓/重启前的仓）
            sl_dist0 = meta.get("sl_dist0")
            if not sl_dist0:
                sl_dist0 = sl_dist
                meta["sl_dist0"] = round(sl_dist, 8)
                changed = True
            mult = float(ex.spec(sym).get("quanto_multiplier") or 1)
            abs_size = abs(float(p["size"]))
            # 真实强平距离：**优先用交易所自算的 `liq_price`**（10-06 14:1x 改）。
            #   回退写法 = margin/(size×mult) − mr×entry，其中 `mr` 由
            #   `_maint_rate(sym)` **按合约动态取**（10-06 去掉写死的 `mr = 0.005`）。
            #   为什么必须动态：真实口径远不止 0.5% —— TRX 强平价反推的有效维持率
            #   ≈ **1.485%**（档位表 0.66%~1.5%，且选 50x 会把所用档抬到
            #   `leverage_max=50` 那档）→ 旧常数实测**高估强平距离 +0.93%**
            #   （引擎 5.000% vs 交易所 4.075%）。估计偏乐观 ⇒ 真实强平已进 SL 之内
            #   时本闸也不会报警，正好废掉它唯一的职责。
            #   改用 liq_price（真实口径）+ 动态 mr（回退口径）后，它才真能兑现
            #   「爆仓点晚于止损点」。`liq_price` 缺失/为 0 → 走下面的动态回退公式。
            cur_margin = float(p.get("margin") or 0)
            _liq_px = float(p.get("liq_price") or 0)
            _mr = self._maint_rate(sym, p)         # 逐仓动态维持率（持仓档→合约档→配置）
            if _liq_px > 0:
                liq_dist = abs(entry - _liq_px)
            else:
                liq_dist = max(cur_margin / (abs_size * mult) - _mr * entry, 0.0)
            _gg = self.risk.cfg["guards"]          # 本闸全部阈值/帽一律读配置，不写死
            target = max(_gg["margin_guard_sl_x"] * sl_dist,
                         entry * _gg["margin_guard_floor"])
            # 硬上限：守护后总保证金 ≤ 名义 20% 且 ≤ 权益 15%（防公式异常无限灌注）
            # 10-03 21:30 加固（SL 固定 1.5% 的必要配套）：
            #   ⚠ 旧文「爆仓距离 = 2%−0.5% = 1.5%，与 SL 完全重合」的 **0.5% 是错的**
            #   （真实 ≈1.0%~1.5%，见上方 liq_price 段）；但「新仓贴着爆仓线」的
            #   结论反而更成立 —— 按开仓保证金 2.69U 反推真实强平距离仅 **0.53%~0.75%**，
            #   比 SL 0.8% 还近（Gate 实给 initial_margin 3.21U 时约 0.9%~1.1%）。
            #   原逻辑只在 heavy 轮（60s 一次）补，
            #   这 60 秒里只要反向走 1.5%（15m 单根波幅常 >1×ATR）就先被强平，
            #   止损条件单根本来不及成交。故新仓（meta 无 mg_done）不受 heavy 限制，
            #   guard 下一轮（≤2s）立即把保证金补到目标爆仓距离。
            if (heavy or not meta.get("mg_done")) and liq_dist < target:
                notional = abs_size * mult * entry
                need = min((target - liq_dist) * abs_size * mult,
                           notional * _gg["margin_guard_cap_notional_pct"] - cur_margin,
                           ex.equity() * _gg["margin_guard_cap_equity_pct"])
                need = max(need, 0.0)
                if need >= _gg["margin_guard_min_add_u"]:    # 低于此值不值得动
                    try:
                        ex.add_margin(sym, side, need)
                        meta["mg_done"] = True     # 已补过 → 退回 heavy 轮常规巡检
                        changed = True
                        LOG(f"  🛡 {sym} {side} 保证金守护 +{need:.4f}U"
                            f"（强平距离 {liq_dist/entry:.2%} → 目标 {target/entry:.2%}"
                            f"，维持率 {_mr:.4%}）")
                    except Exception as e:
                        LOG(f"  ⚠ guard 追加保证金失败 {sym}: {str(e)[:120]}")
            # 孤儿仓兜底：交易所侧无 SL 条件单则补挂（11:00 修复：不再信本地 sl_id，
            # 一律以交易所实际挂单为准）。
            # 🔴 10-05 用户拍板：从 heavy 门控里拿出来（129s → orphan_check_s，默认10s）。
            #   与裸仓巡检的本质差异：meta 完好但**交易所侧** SL 单没了 → 本地无从得知，
            #   只能问交易所，所以「检查频率」就是「发现延迟」，是同一个旋钮。
            #   成本已压到**全部仓共用 1 次 HTTP**（见 `_orphan_sl_sweep`）。
            #   下方原本的逐仓 heavy 门控实现已删除，逻辑迁进 _orphan_sl_sweep，
            #   补挂动作仍走同一份 `_place_protective_sl`（不另写一套）。
            # 本段其余逻辑（D 段止盈兜底）仍在下方，未被搬动。
            # ---- D：止盈兜底（2026-09-30 10:26 用户拍板）----
            px = float(p.get("mark_price") or 0) or feeder_px(sym)
            tp = float(meta.get("tp") or 0)
            if px <= 0 or tp <= 0:
                continue
            # 遥测：开仓后 +15/30/60/120/300/900 秒各采一次浮盈（**纯旁路**，
            # 零 HTTP、不参与任何决策；任何异常都不影响 guard 主流程）。
            try:
                self._tlm_mark(sym, side, meta, p, px)
            except Exception:
                pass
            gcfg = self.cfg["guards"]
            # ============ 峰值/谷值维护（10-08「让 AI 和看板看见浮盈」）============
            # 🔴 病根（10-08 复盘实证）：10-07 用户令把全部机械护盈段关掉之后，
            #   **没有任何一段还在推进 meta 的 peak / tp_peak / trough** —— 它们被永久
            #   冻结在开仓价上（开仓时 decision 写的是 `"peak": fill, "tp_peak": fill`）。
            #   连锁后果（当天 19 笔平仓单实测）：
            #     · 看板「MFE 最大浮盈 / 峰值出现时刻 / 峰值价」全是 0 / 入场价；
            #     · `journal.mark` 的 `dd_from_peak_pct` 退化成「距入场价的涨跌幅」；
            #     · exit 线程 `_brief` 的 `mfe_pct` / `give_back_pct` **恒为 0** →
            #       系统提示词的【平仓条件②「顺向涨势已吐光」永远不可能命中】，
            #       模型根本不知道这单曾经赚过，只能看它一路回吐到 SL。
            #   实证：10-08 的 12 笔亏损单里有 9 笔曾浮盈 >0.1U，峰值合计 +2.15U 全吐光。
            # 本段职责**只有遥测**：一个出场动作都不做（不撤单 / 不市价平 / 不推 SL）。
            #   唯一会写的是 meta 的 peak / tp_peak / trough / peak_t 四个键。
            #   ⚠ 10-09 起它是**唯一**维护峰谷的地方（机械护盈段已整体删除），
            #     关掉它就等于让模型的 ②a/②b 与停滞锁盈（⑤）全部失效。
            # 成本：只有出现新极值时才多一个 delta 字段、走 _writeback_meta 的常规
            #   回写；无新极值时 delta 为空，_writeback_meta 在读锁前就 return False，
            #   **零 IO**。
            # 口径：peak 与 tp_peak 是同一个「最有利价」，这里**两个一起维护**
            #   （前端与 exit 线程各读其中一个）；trough 是最大不利偏移（MAE 遥测，
            #   只朝不利方向推进）。peak_t 只在峰真的刷新时更新（它喂 `peak_reach_s`
            #   遥测，被 trough 的刷新带跑会把「峰值到达时刻」算晚）。
            # 回退：`guards.peak_track_enabled` 置 false = 逐字节回到「峰值冻结」旧行为。
            if gcfg.get("peak_track_enabled", True) and px > 0:
                _pk0 = float(meta.get("peak") or 0)
                _tp0 = float(meta.get("tp_peak") or 0)
                _tr0 = float(meta.get("trough") or 0)
                _pk = max(_pk0 or entry, px) if side == "long" \
                    else min(_pk0 or entry, px)
                _tp = max(_tp0 or entry, px) if side == "long" \
                    else min(_tp0 or entry, px)
                _tr = min(_tr0 or entry, px) if side == "long" \
                    else max(_tr0 or entry, px)
                if _pk != _pk0 or _tp != _tp0 or _tr != _tr0:
                    changed = True
                    if _pk != _pk0:
                        meta["peak"] = _pk
                    if _tp != _tp0:
                        meta["tp_peak"] = _tp
                    if _tr != _tr0:
                        meta["trough"] = _tr
                    if _pk != _pk0 or _tp != _tp0:
                        meta["peak_t"] = time.time()      # 遥测：峰值刷新时刻
            # D：TP 条件单已消失（触发落成限价）而仓位还在 →
            #    价已过 TP：市价 IOC 兜底 + 清残单；价未到 TP：补挂 TP 防裸奔
            if (gcfg.get("tp_fill_fallback", True)
                    and meta.get("tp_id") is not None
                    and not ex.has_price_order(sym, "TP", side)):
                hit = (px >= tp) if side == "long" else (px <= tp)
                if hit:
                    try:
                        msg = ex.market_close(sym, side)
                        ex.cancel_open_orders(sym)
                        LOG(f"  🎯 {sym} {side} TP 触发未成交，市价兜底：{msg}")
                    except Exception as e:
                        LOG(f"  ⚠ TP 兜底失败 {sym}: {str(e)[:120]}")
                else:
                    try:
                        _, new_tp_id = ex.place_tpsl(sym, sl_px=None, tp_px=tp,
                                                     is_long=(side == "long"))
                        if new_tp_id:
                            meta["tp_id"] = new_tp_id
                            changed = True
                            LOG(f"  🆘 {sym} {side} TP 条件单丢失（未到价），补挂 {tp:g}")
                    except Exception as e:
                        LOG(f"  ⚠ TP 补挂失败 {sym}: {str(e)[:120]}")
            if changed:
                # ⚠ 10-03 01:15 修复（第三路审查 G1，爆仓级）：本处的 meta 是本轮开头
                #   （约 4.5 秒前、含多次交易所 API 调用）取的快照，直接整条覆盖回
                #   open_meta，会回滚掉窗口内 hf 线程对**同一 key** 的金字塔加仓写入
                #   （entry/size/sl/sl_id），也���抹掉 hf 线程新开的**其它 key**——
                #   与23:49 XRP / 23:16 ADA 同型。00:35 只给 decision._save 加了并集
                #   合并，engine 这侧漏了。现在走同一条合并路径：先fresh load 盘上最新，
                #   再叠加本轮**实际变更的字段**（而非整条旧 meta）。
                # ⚠ 10-03 20:15 再加一层「换仓保护」（幽灵峰值 bug 根因②，致命）：
                #   仅做字段合并挡不住**同 key 换仓**——窗口内 hf 线程把旧的
                #   sym|side 平掉又新开，盘上已是新仓记录，而本轮 meta 还属于上一代。
                #   此时合并会把新仓的 entry/peak/tp_peak/sl_dist0 覆盖成上一代值 →
                #   新仓一出生就"已经赚过又回吐"，模型据此误判
                #   （ZEC 20:00:40 开 → 20:00:52 被平、SKHYNIX 15:48 同理）。
                #   判据：开仓指纹 t_at_open（加减仓都不改它）与盘上最新不一致 →
                #   本轮快照作废，放弃回写（下一轮重新读新仓，最多少写一轮峰值）。
                self._writeback_meta(sym, side, meta, meta_orig)


# ---- 辅助（避免循环 import 放底部）----
def feeder_px(sym: str) -> float:
    from market import feeder
    return feeder.live_price(sym)


def _pub_pos(p: dict) -> dict:
    return {"contract": p["contract"],
            "side": "long" if float(p["size"]) > 0 else "short",
            "size": abs(float(p["size"])), "entry": p.get("entry_price"),
            "upnl": float(p.get("unrealised_pnl") or 0),
            # 占用保证金：**原样取交易所持仓行的 `margin`**（看板据此显示
            #   「保证金」与「收益率 ROE ＝ 浮盈 ÷ 保证金」，与 App 显示同口径）。
            # ⚠ 禁止用 名义/杠杆 自算：实测 Gate 逐仓 `margin` 含手续费与维持
            #   保证金缓冲，比 value/lever 高约 4%（ETH 2.7083 vs 2.6055）
            #   → 自算会让看板 ROE 与交易所显示不一致（本项目「口径漂移」老坑）。
            "margin": float(p.get("margin") or 0),
            "lev": p.get("leverage")}


def _all_pos_keys(hf, positions) -> set:
    out = set()
    for p in positions:
        out.add(f"{p['contract']}|{'long' if float(p['size']) > 0 else 'short'}")
    return out
