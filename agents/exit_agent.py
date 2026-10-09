# -*- coding: utf-8 -*-
"""
agents/exit_agent.py — 持仓管理（平仓决策）代理

职责**有且只有一个**：把当前账内持仓喂给模型，让模型自己判 hold / close；
代码收到 close 收到信号后执行「撤本方向条件单 + 市价平」。

与开仓决策的分工（互不越界，10-07 用户令）：
  · hf 线程（agents/decision.py）  = 开仓决策，看**全池**标注 → open / hold；
  · exit 线程（本文件，engine.exit_loop）= 持仓管理，只看**持仓品种** → close / hold。

🔴 三条不变量（改本文件前先读）：
  1. **只传持仓的品种**（用户明令）。brief 里的行情标注从 `runtime/scan.json`
     按当前持仓过滤得到，绝不把全池塞进去。
  2. **交易所那两条固定 SL/TP 条件单保留不动**，作硬兜底。模型不响应 / 超时 /
     解析失败 / 引擎暂停时，最大亏损仍被 `exit.sl_min_pct` 锁死。
     `executor.market_close()` 内部会先撤**本方向**的 SL/TP 再市价平，
     所以不存在「模型平完条件单又触发」的重复平仓。
  3. **不自己落账**。本模块只负责「让交易所的仓位消失」，
     `engine._reconcile_closes()` 会在 2 秒内发现并**用完整字段**落账
     （entry/sl/tp/exit_px/hold_min/trade_id/peak… 全套）。若这里自己 pop
     open_meta + 写一条瘦记录，看板与 4 维统计立刻口径漂移。
"""
from __future__ import annotations

import time
from datetime import datetime

from agents import llm
from core import journal
from core import log as core_log
from core import state
from exchange import executor as ex

LOG = core_log.log

# 模型平仓的订单归因标签（→ engine._reconcile_closes → flip.CAUSE_MODEL_CLOSE）
CLOSE_TEXT = "t-v2-mclose"


SYSTEM_PROMPT_TPL = """你是加密永续合约的**持仓管理者**。你唯一的职责：对下面列出的每一个持仓，判断「继续拿着(hold)」还是「立刻平掉(close)」。你不开新仓、不改仓位、不动杠杆——进场价与张数早已定死，你只管"这单还该不该拿"。

【铁律：不要轻易平仓，不要亏大赢小】
· hold 是默认答案，close 是重手。**只有命中【平仓五条件】写明的确认信号才允许平**；对不齐的、"感觉要涨/要跌"的、小幅浮亏的、浮盈刚回吐一点的，**一律 hold**。宁可多拿一轮，也别来回进出——乱平等于把手续费和盈亏比一起交出去。
· **顺着行情就让它拿**：价格在往你的方向走、结构没坏，不要因为"已经赚了一点"慌忙落袋，让它走到止盈。
· **逆了才走，但同样要等确认**：动能与结构**同时**转向才算数（现役止损只有 {SL_PCT}%，等它触发＝实打实亏掉那一笔）。
· 一句话：**对了就多拿，错了才快走**。

【brief 结构】
每个持仓**自成一块**，以 `【品种 方向 张数】` 开头，块内从上到下固定六行：
`持仓`（入场/现价/浮盈/ROE/已持仓分钟/名义/现价幅度）→ `止盈止损`（tp、sl 及其距现价
的距离、完整止盈距离 tp_dist0_pct、盈亏比 tp_r）→ `盈亏轨迹`（峰值回吐 mfe/give_back/
mae，百分比与金额都有）→ `R 口径`（r_now/r_mfe/r_mae/arm_r/r_hit/peak_ratio）→
`行情标注`（该品种的 1 小时布林全部标注）→ `白话读法`（代码算好的一句事实）。
**一个品种的所有信息都在它自己那一块里，不要在别的块里找它的数字。**

【字段速查】
持仓记录：contract/side/size＝品种/方向/张数；entry/px＝入场价/现价；upnl_u＝浮盈(U)；roe_pct＝浮盈÷占用保证金（**已含 ×{LEV} 杠杆**）；hold_min＝已持仓分钟数。
出场专用：mfe_pct＝持仓期间**最好赚到过**多少%；give_back_pct＝从最高点**已回吐**多少%（回吐大＝这波快走完了）；mfe_u/give_back_u＝上面两个数的**金额版(U)**（与 upnl_u 同单位，直接比大小，不用自己折算）；mae_pct/mae_u＝持仓期间**最多亏到过**多少%（负数）／多少 U（与峰值同一段遥测，0 = 还没逆向走过）；sl_loss_u＝若打到止损，这一单总共亏多少 U；r_now/r_mfe/r_mae＝把上面三个金额**除以本单止损金额**得到的 R 倍数（+0.5R = 赚到半个止损额，-1R = 正好亏掉一个止损额、也就是打到止损那一刻）；tp_r＝本单盈亏比（|止盈−入场|÷|入场−止损|）；arm_r＝**R 线**（＝0.5×tp_r，即"峰值摸到止盈距离的一半"）；r_hit＝true/false/null，持仓期间峰值**有没有到过这条 R 线**（null＝计算依据缺失；这时不能使用②a/②b）；now_pct＝当前价格幅度%；sl/tp＝两条保护单价格；sl_dist_pct/tp_dist_pct＝现价到它们的距离%（离得近＝马上要被打到）；tp_dist0_pct＝**本单完整的止盈距离%**；peak_ratio＝峰值浮盈 ÷ tp_dist0_pct（1.0 = 峰值正好摸到止盈价，0.8 = 只摸到八成）；stall_lock＝**停滞锁盈信号**（峰值≥0.8×止盈距离 ＋ 持仓≥120分钟 ＋ 回吐≥70%峰值 ＋ 仍浮盈，四条同时成立才为 true）——**它是 true 就按第 5 条落袋**。
ann＝该品种 1 小时布林标注（与开仓同一套口径）：regime·adx＝行情定性（up/down_trend 顺它做、high_vol 方向未定、range 震荡、low_vol 轨道失参考）／band_pos＝带内位置（0下轨·0.5中轨·1上轨）／walk_up_dn_bars＝贴轨滑行根数（**顺你持仓方向滑＝趋势在走，别猜头**）／close_out_up_dn_bars≥2＝强趋势确认／last_touch·touch_mid·bars_since_touch＝最近触轨方向与是否收回中轨／three_push·fake_up_dn_n·legs＝衰竭素材（legs 的 ok=true 才成立）／mid_slope_atr≈0＝中轨走平／m1＝近15根1m收盘价%、m1h_mom＝近4根1H涨跌%、rsi15·macd_dif/dea/hist＝动能（已折成%可跨品种比）。
**ann 缺失或标注过期时**：只依据持仓本身的数字（浮盈、回撤、离保护单多远）判断，拿不准就 hold。

【平仓五条件】逐条核对，**只有命中其中一条才输出 close**；都不命中＝hold。
每单都算好了一条 **R 线 arm_r**（＝0.5×tp_r，即"峰值摸到止盈距离的一半"）和 **r_hit**（持仓期间峰值有没有到过这条线）。它是**"这单走起来了没有"的分界线，不是止盈线**：实测到过线的单 96% 最后都赚（合计 +15.87U），没到过的 81% 都亏（合计 −9.59U）。
1. **反转已确认**（最硬）：ann.regime 已翻到你持仓的反面（如持多而 regime=down_trend），**且** m1h_mom 与你持仓反号，**且**价格已站到中轨另一侧（持多 band_pos<0.5、持空 band_pos>0.5）——三条齐了才算反转，缺一条都只是回调。
2. **走起来过、现在吐光（②a）**：r_hit = true（峰值到过 R 线），**且** 已回吐 ≥ 峰值的 2/3（give_back_pct ≥ mfe_pct × 2/3），**且** m1 与 m1h_mom 已往你持仓的反面走 → 落袋，别把到手的再送回去。
3. **没走起来就认错（②b）**：r_hit = false（峰值一直没摸到 R 线），**且** 浮盈已跌回 0 附近（upnl_u ≤ 0，或 r_now ≤ −0.2R），**且** 结构已转坏（m1h_mom 与你持仓反号，或 band_pos 已越过中轨到对面，或 ann 的 fake 次数 ≥2／贴轨滑行根数在减少）→ 走。**这条的重点是"早点认错"，不要等它慢慢磨到止损。**
4. **浮亏且结构已坏**：upnl_u < 0，**且** m1h_mom 与你持仓反号，**且** band_pos 已越过中轨到对面一侧 → 走。若只是区间内来回（regime=range、band_pos 未过中轨、动能没反）→ 继续 hold。
5. **停滞锁盈（⑤）**：`stall_lock = true` 时直接落袋。它是代码替你算好的**唯一允许"按回吐比例"平仓的场合**，四条同时成立才为 true：峰值已摸到止盈距离的 **80%** 以上（peak_ratio ≥ 0.8，几乎就要止盈了）＋ 已持仓 ≥ **120 分钟**（给过机会、拖太久）＋ 已回吐掉峰值的 **70%**（give_back_pct ≥ 0.7 × mfe_pct）＋ 当前仍浮盈（upnl_u > 0）。**看到 stall_lock=true 就写 close，别再用"回吐属正常波动"把它放过去。**

⚠ 第 2、3 条按 r_hit 二选一，互斥。**除第 5 条（停滞锁盈）外，绝不要用"回吐比例"去砍 r_hit = true 的单** —— 那是能走到止盈的那批，砍早了就是亏大赢小。第 5 条之所以能按回吐砍，是因为它额外要求"峰值几乎摸到止盈 + 已经拖了两小时"，这是"给过机会却没兑现"的一小撮单；**没有 stall_lock 标记时，回吐本身永远不构成平仓理由。**

【纪律】
· brief 里的**每一个持仓都必须表态**，逐条写理由；**没命中五条件就写 hold**。
· **每条持仓记录的 key 字段（形如 `BTC_USDT|short`）就是它的唯一标识：contract 与 side 必须
  **原样照抄**该持仓的 key／side，**不许**按你自己的多空看法去改写方向。side 只是"这笔仓是多是空"
  的事实标签，不是你对该品种方向的判断（判断写在 action/reason 里）。**
· 只允许输出 brief 里出现过的 contract/side；写别的名字会被直接丢弃。
· reason 用大白话写给不懂指标的人看（**≤30 字**）：说清"价格刚才做了什么，所以拿/平"。禁止用字段名、代号、缩写。

【输出】只输出一个 JSON：
{"positions":[{"contract":"ETH_USDT","side":"short","action":"hold|close","reason":"一句话"}],
 "bias":"偏多|偏空|中性","summary":"一句话总述"}"""


def _pct(v) -> str:
    """把小数收益率渲染成百分数字符串（用于提示词里的 SL 口径）。"""
    try:
        return f"{float(v) * 100:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return "?"


def _num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _tri_and(values):
    if any(v is False for v in values):
        return False
    return None if any(v is None for v in values) else True


def _tri_or(values):
    if any(v is True for v in values):
        return True
    return None if any(v is None for v in values) else False


def _m1_scalar(value):
    """把 scan 的 `m1`（15 根 1m 的**归一化数组**）折成与 m1h_mom **同号同口径**的标量。

    🔴 10-09 修（真 bug）：`m1` 在 scan.json 里是**数组**不是数字，旧代码直接把它
    喂给 `_num()` → `float(list)` 抛 TypeError → 返回 None → ②a 的「m1 已反向」
    这一条**恒为 None**，导致 `rule_2a_profit_giveback` 在 572 条 exit_brief 里
    **一次都没 True 过**（要么 False 要么 None）。等于该条件被静默废掉。

    口径（见 decision.py::_m1_snap 的 docstring）：`m1[i] = (c_i − c_last)/c_last × 100`，
    首项是**最老**那根、末项恒 0。所以
      · `m1[0] > 0` ⇒ 老价 > 新价 ⇒ 这一段**跌**了；
      · 与 m1h_mom（`(c_last − c_base)/c_base`，涨为正）**符号相反**。
    这里取 `−m1[0]` 还原成"这一段涨了多少%"的正常口径，供 `_opposite_momentum`
    与 m1h_mom 用同一套判据。标量（老数据/未来格式）原样返回。
    """
    if isinstance(value, (list, tuple)):
        return -_num(value[0]) if value else None
    return _num(value)


def _opposite_momentum(value, side):
    value = _num(value)
    if value is None:
        return None
    return value < 0 if side == "long" else value > 0


def _opposite_band(value, side):
    value = _num(value)
    if value is None:
        return None
    return value < 0.5 if side == "long" else value > 0.5


def _exit_rule_checks(row: dict, side: str, walk_decreasing=None) -> dict:
    """以 exit brief 原始值重算五条规则；缺失输入保留为 None，不伪造 False。"""
    ann = row.get("ann") or {}
    regime = ann.get("regime")
    regime_opposite = (None if regime is None else
                       regime == ("down_trend" if side == "long" else "up_trend"))
    mom1_opposite = _opposite_momentum(_m1_scalar(ann.get("m1")), side)
    mom1h_opposite = _opposite_momentum(ann.get("m1h_mom"), side)
    band_opposite = _opposite_band(ann.get("band_pos"), side)

    rule_1 = _tri_and([regime_opposite, mom1h_opposite, band_opposite])

    hit = row.get("r_hit")
    mfe = _num(row.get("mfe_pct"))
    give_back = _num(row.get("give_back_pct"))
    giveback_ready = (None if mfe is None or give_back is None else
                      mfe > 0 and give_back >= mfe * (2.0 / 3.0))
    rule_2a = _tri_and([hit, giveback_ready, mom1_opposite, mom1h_opposite])

    upnl = _num(row.get("upnl_u"))
    r_now = _num(row.get("r_now"))
    near_zero_or_loss = _tri_or([
        None if upnl is None else upnl <= 0,
        None if r_now is None else r_now <= -0.2,
    ])
    fake_key = "fake_up_n" if side == "long" else "fake_dn_n"
    walk_key = "walk_up_bars" if side == "long" else "walk_dn_bars"
    fake_count = _num(ann.get(fake_key))
    fake_bad = None if fake_count is None else fake_count >= 2
    walk_bad = walk_decreasing
    structure_bad = _tri_or([
        mom1h_opposite, band_opposite, fake_bad, walk_bad,
    ])
    rule_2b = _tri_and([
        None if hit is None else not bool(hit),
        near_zero_or_loss,
        structure_bad,
    ])

    loss = None if upnl is None else upnl < 0
    rule_3 = _tri_and([loss, mom1h_opposite, band_opposite])
    # ⑤ 停滞锁盈：**不由这里重算**，直接读 brief 里代码算好的 `stall_lock`
    #   （它的四个输入 peak_ratio / hold_min / give_back / upnl 都在同一份 rec 里，
    #    在 `_brief` 里一次算完，避免两处口径漂移）。缺失 = None（不伪造 False）。
    stall = row.get("stall_lock")
    rule_5 = None if stall is None else bool(stall)
    return {
        "rule_1_reversal_confirmed": rule_1,
        "rule_2a_profit_giveback": rule_2a,
        "rule_2b_failed_start": rule_2b,
        "rule_3_loss_structure_bad": rule_3,
        "rule_5_stall_lock": rule_5,
        "walk_decreasing": walk_decreasing,
        "walk_bars": _num(ann.get(walk_key)),
    }


class ExitAgent:
    """持仓管理者：只看账内持仓，只出 hold/close。"""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.ecfg = (cfg.get("exit_review") or {})
        self.rt = cfg["runtime_dir"]
        self.state_path = self.rt + "/hf_state.json"
        self.scan_path = self.rt + "/scan.json"
        self.decisions_path = self.rt + "/exit_decisions.md"
        self._sys = None
        self._scan_warn_at = 0.0      # scan.json 缺失/过期告警限流（防每轮刷屏）
        self._last = {"t": 0.0, "n_hold": 0, "n_close": 0}
        self._last_exit_walk: dict = {}  # trade_id -> 上轮同方向 walk 根数（仅用于规则回放）

    # -------------------------------------------------- 提示词
    def system_prompt(self) -> str:
        """渲染一次并缓存（模板只随 SL 口径/杠杆变）。"""
        if self._sys is None:
            ecfg = self.cfg.get("exit") or {}
            scfg = self.cfg.get("sizing") or {}
            self._sys = (SYSTEM_PROMPT_TPL
                         .replace("{SL_PCT}", _pct(ecfg.get("sl_min_pct", 0)))
                         .replace("{LEV}", str(int(scfg.get("lev_default") or 1))))
        return self._sys

    # -------------------------------------------------- 数据
    def _held(self):
        """返回 (held, err)。held = [(sym, side, row, meta), ...] 仅**账内仓**。

        账外仓（App/体验仓手动开的）一律剔除 —— 与 decision._leg_snapshot 的
        隔离语义一致：引擎不管理非自己开的仓。

        🔴 持仓快照不可信（ok=False）时返回空列表 + err，调用方**整轮中止**：
        绝不把「拉不到」当成「没有仓」。
        """
        ok, rows, err = ex.positions_strict_rows()
        if not ok:
            return [], str(err)[:200]
        own = (state.load(self.state_path, {}) or {}).get("open_meta") or {}
        out = []
        for p in rows:
            sz = float(p.get("size") or 0)
            if sz == 0:
                continue
            sym = p["contract"]
            side = "long" if sz > 0 else "short"
            k = f"{sym}|{side}"
            if k not in own:
                continue        # 账外仓：隔离
            out.append((sym, side, p, own.get(k) or {}))
        return out, ""

    def _ann_map(self) -> tuple:
        """从 `runtime/scan.json` 取标注，按 contract 索引。返回 (map, note)。

        宁可复用 hf 刚落盘的扫描结果，也**不在这里自己跑状态机**：
        `BollStateMachine` 的内部状态（prev/deep/walk/cand）是跨品种共享的
        读写视图，hf 线程每 60s 正在写它 —— 另起一个线程并发 feed 会让两个
        品种的状态互相清空（boll.py 里 10-03 修过的同型病灶）。
        """
        d = state.load(self.scan_path, {}) or {}
        items = d.get("items") or []
        age = time.time() - float(d.get("t") or 0)
        limit = float(self.ecfg.get("max_scan_age_s", 180) or 180)
        if not items:
            return {}, "标注不可用（scan.json 尚无数据）"
        if age > limit:
            return {}, f"标注不可用（scan.json 已过期 {age / 60:.1f} 分钟）"
        return {i.get("contract"): i for i in items if isinstance(i, dict)}, ""

    def _today_done(self) -> int:
        """今日已平仓笔数（喂给模型的"今日已做 N 笔"）。

        🔴 10-08 修：原来读的是 hf_state.json 的 trades_today 键 —— 那个键
           **从来没有任何代码写过**（该文件顶层只有 open_meta/regime/bias），
           所以这个数**恒为 0**。等于每轮都给模型喂一条假信息（让它以为今天
           还没怎么开过仓），直接影响它对"是不是在过度交易"的判断。
        口径与看板"今日统计"同源：trades.json 里 action=close 且 t ≥ 今日 0 点。
        成本：一次全量读 trades.json（百 KB 量级、~26ms），**每 60 秒一轮**才调
        一次，不在任何热路径上（guard 2s 轮、看板 5s 推都碰不到它）。
        """
        try:
            t0 = time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
            n = 0
            for x in (state.load(self.rt + "/trades.json", []) or []):
                if (isinstance(x, dict) and x.get("action") == "close"
                        and float(x.get("t") or 0) >= t0):
                    n += 1
            return n
        except Exception:
            return 0

    # -------------------------------------------------- brief
    def _brief(self, held: list, ann: dict, ann_note: str,
               equity: float) -> tuple:
        """构造**只含持仓品种**的 brief。返回 `(给模型的文本, 逐品种特征 recs)`。

        🔴 10-09 用户令「一个品种就把行情止盈止损、等现在的东西列在一起，
        不要东一块西一块的」：
          旧格式 = 每个品种一行人读摘要 + **末尾一大坨 JSON 数组** → 模型要在
            两处之间来回对照，字段一多极易看错行、也容易漏看某个品种。
          新格式 = **每个品种自成一个完整块**（持仓 / 止盈止损 / 盈亏轨迹 /
            R 口径 / 行情标注 / 白话读法），块内不再穿插别的东西；JSON 不进提示词。
        第二个返回值 `recs` 是**同一份数据的结构化副本**，只喂 journal 遥测与
        `_exit_rule_checks` —— 不再靠"把 brief 最后一行 json.loads 回来"这种
        自己解析自己输出的脆弱做法（旧实现：`json.loads(brief.rsplit("\n",1)[-1])`）。
        """
        lines = [f"数据时点 {datetime.now().strftime('%H:%M:%S')} | 1小时布林+1m序列"
                 f" | 以下持仓与标注均为该时点最新值",
                 f"账户权益 {equity:.2f} USDT | 当前账内持仓 {len(held)} 个"
                 f" | 今日已做 {self._today_done()} 笔",
                 f"你的任务：对下面每一个持仓给出 hold 或 close。"
                 f"每个品种自成一块，块内从上到下依次是"
                 f"【持仓 / 止盈止损 / 盈亏轨迹 / R 口径 / 行情标注 / 白话读法】。"
                 f"{ann_note}".rstrip()]
        recs = []
        for sym, side, p, meta in held:
            entry = float(p.get("entry_price") or meta.get("entry") or 0)
            px = float(p.get("mark_price") or 0) or _live(sym)
            size = abs(float(p.get("size") or 0))
            upnl = float(p.get("unrealised_pnl") or 0)
            margin = float(p.get("margin") or 0)
            sl = float(meta.get("sl") or 0)
            tp = float(meta.get("tp") or 0)
            mult = _qm(sym)
            notional = size * mult * entry
            # 方向化涨跌（正=顺持仓方向）
            dirn = 1.0 if side == "long" else -1.0
            now_pct = dirn * (px - entry) / entry * 100 if entry and px else 0.0
            # 峰值（meta 里存的是**价格**，换算成按方向计的价格幅度%）
            peak_px = float(meta.get("tp_peak") or meta.get("peak") or 0)
            mfe_pct = dirn * (peak_px - entry) / entry * 100 if entry and peak_px else 0.0
            # ⚠ 归一 `-0.0`（peak==entry 时 `-1.0*0.0` 会得到 -0.0，JSON 里很难看），
            #   并把「回吐」定义成**只在真的有过浮盈时**才有意义 —— 一直没赚钱的单子
            #   没有"回吐"可言（否则浮亏会被算成"回吐 0.36%"，直接误导模型）。
            mfe_pct = abs(mfe_pct) if mfe_pct == 0 else mfe_pct
            give_back = max(0.0, mfe_pct - now_pct) if mfe_pct > 0 else 0.0
            # 最大不利偏移（MAE，10-08 用户令「让模型看到最多亏到多少」）：
            #   meta.trough 与 tp_peak 同源同口径，由 engine 的「峰值/谷值维护」段
            #   维护（guards.peak_track_enabled）→ 方向化后**恒 ≤ 0**（顺方向取正）。
            #   ⚠ 取不到（老仓没有 trough）就给 0：宁可说"没记录到"，也不编一个浮亏出来。
            trough_px = float(meta.get("trough") or 0)
            mae_pct = (dirn * (trough_px - entry) / entry * 100
                       if entry and trough_px else 0.0)
            mae_pct = abs(mae_pct) if mae_pct == 0 else mae_pct     # 归一 -0.0
            # 打到止损这一单总共亏多少 U：**现算**，不用开仓时落盘的 meta.risk_u ——
            #   那个数按开仓时的张数算，加仓后会失真；这里用交易所当前张数 + 当前
            #   SL，永远自洽。（开仓那一刻两者相等，平仓记录里的 risk_u 就是它。）
            sl_loss_u = (abs(entry - sl) * size * mult
                         if (entry and sl and mult) else None)
            # 金额化（U）：峰值 / 谷值 —— 原来只在 rec 里内联算，这里提出来给 R 复用
            mfe_u = notional * mfe_pct / 100 if notional else 0.0
            mae_u = notional * mae_pct / 100 if notional else 0.0
            # ---- R 倍数（10-08 用户令「按止损的金额算」）----
            #   口径：金额 ÷ **本单止损金额**（sl_loss_u）。
            #     +0.5R = 赚到"半个止损额"的钱；-1R = 正好亏掉一个止损额
            #     （-1R 就是打到止损那一刻，所以 -0.6R 意味着"还剩 0.4R 的空间"）。
            #   为什么用 R 而不是价格%：价格%跨单不可比（各品种 SL 距离/杠杆/张数都不同），
            #     而 R 是"以这一单自己的风险为单位"——"峰值到过 0.64R、现在 -0.29R"
            #     一眼就能判断这单是赚着回吐还是开出来就亏。
            #   ⚠ sl_loss_u 拿不到（缺 sl / 缺合约乘数）时三个 R 一律给 None，不编数。
            r_now = r_mfe = r_mae = None
            if sl_loss_u:
                r_now = upnl / sl_loss_u
                r_mfe = mfe_u / sl_loss_u
                r_mae = mae_u / sl_loss_u
            # ---- R 线（10-08 用户拍板：② 拆成 ②a/②b，门槛动态算、不写死 0.42R）----
            #   tp_r  = 本单盈亏比 = |TP − 入场| / |入场 − SL|（今天实测 0.67~0.87）
            #   arm_r = 0.5 × tp_r = "峰值摸到止盈距离的一半" = 这单有没有"走起来"
            #           下限 2×本单往返手续费折成的 R（否则蚂蚁单也算"走起来"），
            #           上限 0.5R（盈亏比再大也不把线抬过半个止损额）。
            #   依据（journal 全量 39 笔回放）：到过这条线的 23 单 96% 最后是赚的、
            #     合计 +15.87U；没到过的 16 单 81% 亏、合计 −9.59U。所以它是
            #     **"两类单"的分界，不是止盈线** —— 到线就落袋反而少赚 5.7U。
            #   代码算好直接喂模型，省得它自己做除法（LLM 算术不可靠）。
            tp_r = (abs(tp - entry) / abs(entry - sl)
                    if (entry and sl and tp and abs(entry - sl) > 1e-12) else None)
            fee_r = (float(meta.get("fee_rt") or 0.001008) / (abs(entry - sl) / entry)
                     if (entry and sl and abs(entry - sl) > 1e-12) else 0.0)
            arm_r = (min(0.5, max(2 * fee_r, 0.5 * tp_r)) if tp_r else None)
            r_hit = (None if arm_r is None or r_mfe is None else r_mfe >= arm_r)
            # ---- 停滞锁盈（⑤，10-09 用户令「让模型自己锁，但别影响能止盈的单子」）----
            #   68 笔已平仓全量回放（journal 的 mark 价格序列）结论：
            #     · 47%（32/68）的单子最终走到止盈，且峰值普遍卡在 0.8~0.95×止盈距离，
            #       与"会打损"的单落在**同一区间** —— 回吐发生的那一刻两者无法区分；
            #     · 因此任何武装线 <0.8×TP 的"回吐锁盈"都是净亏（0.3×TP −5.60U、
            #       0.5×TP −4.58U、0.6×TP −2.35U；砍掉的全是 XRP/SOL/ADA 这类
            #       "回吐一下继续走到止盈"的单）；
            #     · 唯一零误伤的配置 = 把武装线抬到接近止盈 + 额外要求"拖了很久还没兑现"。
            #   四个输入全部取自本 rec 的既有量，一次算完 → 与 _exit_rule_checks 零口径漂移。
            #   阈值见 configs/config.yaml::exit_review.stall_lock_*；置 enabled=false 回退。
            hold_min_v = (round((time.time() - float(meta.get("t_at_open") or 0)) / 60, 1)
                          if meta.get("t_at_open") else None)
            tp_dist0_pct = (abs(tp - entry) / entry * 100 if (entry and tp) else None)
            peak_ratio = (mfe_pct / tp_dist0_pct
                          if (tp_dist0_pct and tp_dist0_pct > 1e-9) else None)
            _slc = self.ecfg
            stall_lock = None
            if _slc.get("stall_lock_enabled", True) and peak_ratio is not None \
                    and hold_min_v is not None:
                stall_lock = bool(
                    peak_ratio >= float(_slc.get("stall_lock_peak_ratio", 0.8) or 0.8)
                    and hold_min_v >= float(_slc.get("stall_lock_min_hold_min", 120) or 120)
                    and mfe_pct > 0
                    and give_back >= mfe_pct * float(_slc.get("stall_lock_giveback_ratio", 0.7) or 0.7)
                    and upnl > 0)
            rec = {
                # key＝"品种|方向"：给模型一个**可直接照抄**的标识，防止它把 side 当成
                # "我对这单的看法"去改写（10-08 实测模型偶尔把 TSLA long 写成 short）。
                "key": f"{sym}|{side}",
                "contract": sym, "side": side, "size": size,
                "entry": round(entry, 8), "px": round(px, 8) if px else None,
                "upnl_u": round(upnl, 4),
                "roe_pct": round(upnl / margin * 100, 2) if margin > 0 else None,
                "hold_min": hold_min_v,
                "now_pct": round(now_pct, 3),
                "mfe_pct": round(mfe_pct, 3),
                "give_back_pct": round(give_back, 3),
                # 金额版浮盈（10-08 用户令「让 AI 看见浮盈」）：原来只给百分比，
                #   模型要自己乘名义价值才看得出"这单最高赚到过 0.5U"；给成同单位
                #   的 U 之后，直接和 upnl_u 比大小即可。peak 由 engine 的
                #   「峰值/谷值维护」段维护（guards.peak_track_enabled）。
                "mfe_u": round(mfe_u, 4),
                "give_back_u": (round(notional * give_back / 100, 4)
                                if notional else None),
                # 浮亏侧（10-08 用户令）：回看的"最多亏到过多少" + 前瞻的"打到止损亏多少"
                "mae_pct": round(mae_pct, 3),
                "mae_u": round(mae_u, 4),
                "sl_loss_u": round(sl_loss_u, 4) if sl_loss_u else None,
                # R 倍数：以上面那个止损金额为单位（10-08 用户令「按止损的金额算」）
                "r_now": round(r_now, 3) if r_now is not None else None,
                "r_mfe": round(r_mfe, 3) if r_mfe is not None else None,
                "r_mae": round(r_mae, 3) if r_mae is not None else None,
                # R 线（②a/②b 的判据，代码算好喂模型）
                "tp_r": round(tp_r, 3) if tp_r is not None else None,
                "arm_r": round(arm_r, 3) if arm_r is not None else None,
                "r_hit": r_hit,
                # 停滞锁盈（⑤）：peak_ratio = 峰值浮盈 ÷ 完整止盈距离；stall_lock = 四条齐否
                "tp_dist0_pct": round(tp_dist0_pct, 3) if tp_dist0_pct is not None else None,
                "peak_ratio": round(peak_ratio, 3) if peak_ratio is not None else None,
                "stall_lock": stall_lock,
                "sl": round(sl, 8) or None, "tp": round(tp, 8) or None,
                "sl_dist_pct": round(abs(px - sl) / px * 100, 3) if px and sl else None,
                "tp_dist_pct": round(abs(tp - px) / px * 100, 3) if px and tp else None,
                "notional_u": round(notional, 2) if notional else None,
            }
            a = ann.get(sym) or {}
            if a:
                rec["ann"] = {k: a.get(k) for k in (
                    "regime", "tier", "adx", "bw", "hint", "meaningful",
                    "mid", "up", "dn", "atr15", "band_pos", "mid_slope_atr",
                    "bw_pct", "bw_burst", "bb_in_kc",
                    "walk_up_bars", "walk_dn_bars", "walk_up_touch", "walk_dn_touch",
                    "close_out_up_bars", "close_out_dn_bars",
                    "three_push_up", "three_push_dn", "fake_up_n", "fake_dn_n",
                    "last_touch", "touch_mid", "bars_since_touch", "legs",
                    "m1", "m1h_mom", "rsi15", "macd_dif", "macd_dea", "macd_hist",
                    "read")
                    if k in a}
            # ---- 逐品种一块：持仓 / 止盈止损 / 盈亏轨迹 / R 口径 / 行情 / 白话读法 ----
            #   10-09 用户令「一个品种就把行情止盈止损、等现在的东西列在一起」。
            #   全部用**固定标签**排版（与系统提示词【字段速查】里的字段名一一对应），
            #   模型不用在两处之间找同一个数字。缺失值统一 `?`，不编数。
            def _f(v, nd=3, sign=False):
                if v is None:
                    return "?"
                return f"{v:+.{nd}f}" if sign else f"{v:.{nd}f}"

            lines.append("")
            lines.append(f"【{sym} {side} {size:g}张】")
            lines.append(
                f"  持仓     入场 {entry:g} | 现价 {px:g} | 浮盈 {upnl:+.4f}U"
                f"（ROE {_f(rec['roe_pct'], 2, True)}%）| 已持仓 "
                f"{rec['hold_min'] if rec['hold_min'] is not None else '?'} 分钟"
                f" | 名义 {_f(rec['notional_u'], 2)}U"
                f" | 现价幅度 now_pct {now_pct:+.3f}%")
            lines.append(
                f"  止盈止损 tp {tp:g}（距现价 {_f(rec['tp_dist_pct'])}%）"
                f" | sl {sl:g}（距现价 {_f(rec['sl_dist_pct'])}%）"
                f" | 止盈距离 tp_dist0_pct {_f(rec['tp_dist0_pct'])}%"
                f" | 盈亏比 tp_r {_f(rec['tp_r'])}")
            lines.append(
                f"  盈亏轨迹 mfe_pct {mfe_pct:+.3f}（mfe_u {mfe_u:+.4f}U）"
                f" | give_back_pct {give_back:+.3f}"
                f"（give_back_u {_f(rec['give_back_u'], 4)}U）"
                f" | mae_pct {mae_pct:+.3f}（mae_u {mae_u:+.4f}U）")
            lines.append(
                f"  R 口径   r_now {_f(rec['r_now'], 2, True)}R"
                f" | r_mfe {_f(rec['r_mfe'], 2, True)}R"
                f" | r_mae {_f(rec['r_mae'], 2, True)}R"
                f" | sl_loss_u {_f(rec['sl_loss_u'], 4)}U"
                f" | arm_r {_f(rec['arm_r'])}"
                f" | r_hit {rec['r_hit']}"
                f" | peak_ratio {_f(rec['peak_ratio'])}")
            a2 = rec.get("ann") or {}
            if a2:
                def _av(v):
                    # 标注值统一排版：bool → true/false（与模型输出的 JSON 同形），
                    # 浮点按量级保留 2~4 位并去掉尾零（`atr15 624.285714` → `624.29`、
                    # `mid 81500.0` → `81500`），嵌套 dict（legs）展平成 `k=v`。
                    if isinstance(v, bool):
                        return "true" if v else "false"
                    if isinstance(v, dict):
                        return " ".join(f"{kk}={_av(vv)}" for kk, vv in v.items())
                    if isinstance(v, float):
                        nd = 2 if abs(v) >= 100 else 3 if abs(v) >= 1 else 4
                        return f"{v:.{nd}f}".rstrip("0").rstrip(".")
                    return str(v)

                parts = [f"{k} {_av(a2[k])}" for k in (
                    "regime", "tier", "adx", "band_pos", "mid", "up", "dn", "atr15",
                    "mid_slope_atr", "bw_pct", "bw_burst", "bb_in_kc", "meaningful",
                    "walk_up_bars", "walk_dn_bars", "walk_up_touch", "walk_dn_touch",
                    "close_out_up_bars", "close_out_dn_bars",
                    "three_push_up", "three_push_dn", "fake_up_n", "fake_dn_n",
                    "last_touch", "touch_mid", "bars_since_touch", "legs",
                    "m1h_mom", "rsi15", "macd_dif", "macd_dea", "macd_hist", "hint")
                    if a2.get(k) is not None]
                lines.append("  行情标注 " + " | ".join(parts))
                if a2.get("m1") is not None:
                    lines.append("  1m 序列  m1 " + str(a2["m1"]))
                if a2.get("read"):
                    lines.append(f"  白话读法 {a2['read']}")
            if stall_lock:
                lines.append(
                    f"  🚩 停滞锁盈 stall_lock = true：峰值已摸到止盈距离的 "
                    f"{peak_ratio:.0%}、已持仓 {hold_min_v:.0f} 分钟、"
                    f"回吐掉峰值的 {give_back / mfe_pct:.0%} —— 命中【平仓五条件】"
                    f"第 5 条「停滞锁盈」→ 该落袋了，别再拿。")
            recs.append(rec)
        return "\n".join(lines), recs

    # -------------------------------------------------- 主流程
    def cycle(self):
        if not self.ecfg.get("enabled", True):
            return
        held, err = self._held()
        if err:
            LOG(f"  🚫 持仓管理本轮中止：持仓快照不可信（{err}）"
                f"—— 绝不把「拉不到」当成「没有仓」")
            try:
                journal.log("anomaly", kind="positions_unavailable",
                            where="exit_agent", err=err,
                            why="exit 线程持仓快照 ok=False，整轮跳过，不调模型")
            except Exception:
                pass
            return
        if not held:
            return                      # 无账内持仓 → 零开销、不调模型
        ann, ann_note = self._ann_map()
        if ann_note and time.time() - self._scan_warn_at > 600:
            self._scan_warn_at = time.time()
            LOG(f"  ⚠ 持仓管理：{ann_note}（仍会调模型，仓位信息本身是主要依据）")
        n_close = self._decide(held, ann, ann_note)

    def _decide(self, held: list, ann: dict, ann_note: str) -> int:
        cfg = self.cfg
        lcfg = cfg["llm"]
        try:
            equity = ex.equity()
        except Exception:
            equity = 0.0
        brief, brief_rows = self._brief(held, ann, ann_note, equity)
        # ---- 校验：只认 brief 里出现过的 (contract, side) ----
        allow = {f"{s}|{sd}": (s, sd, p, m) for s, sd, p, m in held}
        t0 = time.time()
        try:
            raw = llm.ask([{"role": "system", "content": self.system_prompt()},
                           {"role": "user", "content": brief}],
                          temp=float(lcfg.get("temperature", 0.0)),
                          max_tokens=int(self.ecfg.get("max_tokens", 1024)),
                          timeout=int(self.ecfg.get("timeout_s", 90)))
            d = llm.parse_json(raw)
        except Exception:
            dt = time.time() - t0
            self._log_exit_briefs(brief_rows, allow, {}, {}, dt, "model_error")
            raise
        dt = time.time() - t0
        if not isinstance(d, dict) or not d:
            # 模型无有效输出 → **不做任何动作**（这是 fail-safe 的正解：
            #   SL/TP 条件单仍在，仓位不会裸奔）
            LOG(f"  ⚠ 持仓管理：模型无有效输出（{dt:.1f}s），本轮不动任何仓位")
            self._log_decision([], f"模型无有效输出（耗时 {dt:.1f}s）", [])
            self._log_exit_briefs(brief_rows, allow, {}, {}, dt, "invalid_output")
            return 0
        items = d.get("positions") or []
        if not isinstance(items, list):
            items = []
        # sym → 该品种在账内的方向集合（正常情况下只有一个：引擎不允许同合约双腿并存，
        #   见启动「双开审计」）。仅用于「方向写错但品种对」的补救。
        held_side = {}
        for _s, _sd, _p, _m in held:
            held_side.setdefault(_s, set()).add(_sd)
        seen, plan, fixed = set(), [], []
        decisions = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            sym = str(it.get("contract") or "").strip()
            side = str(it.get("side") or "").strip().lower()
            act = str(it.get("action") or "hold").strip().lower()
            key = f"{sym}|{side}"
            if key not in allow:
                # 🔴 10-08 修：模型偶尔把方向写错——今日 15 次告警 **100% 是 TSLA_USDT|short**
                #   而持仓实为 long；约 1.8% 轮次、temp=0 复现不出（见 _repro_tsla.py）。
                #   旧代码直接丢弃该票 → 持仓本轮变「未表态 → hold」：模型若本意是 close，
                #   会被**静默吞掉**，仓位一路裸跑到 SL/TP（且 exit_decisions.md 只显示
                #   "→ 持有"，人根本看不出来）。品种方向唯一确定时按实修正保留，否则才丢。
                real = held_side.get(sym)
                if real and len(real) == 1 and side in ("long", "short"):
                    real_side = next(iter(real))
                    fixed.append(f"{sym.replace('_USDT', '')} {side}→{real_side}")
                    LOG(f"  🔧 持仓管理：模型把 {sym} 的方向写成 {side}（实为 {real_side}）"
                        f" → 按持仓修正保留，不丢票")
                    try:
                        journal.log("anomaly", kind="exit_side_mismatch", sym=sym,
                                    model_side=side, real_side=real_side,
                                    why="模型 side 写错；按账内唯一持仓方向修正后保留")
                    except Exception:
                        pass
                    side = real_side
                    key = f"{sym}|{side}"
                else:
                    LOG(f"  ⚠ 持仓管理：模型给的 {key} 不在持仓清单里 → 丢弃")
                    continue
            if key in seen:
                continue            # 修正后可能与已有条目撞车 → 去重（先到先得）
            seen.add(key)
            model_reason = it.get("reason")
            if model_reason is not None:
                model_reason = str(model_reason)[:500]
            if act == "close":
                reason = it.get("reason") or ""
                plan.append((sym, side, reason))
                decisions[key] = {"action": "close", "reason": model_reason,
                                  "source": "model"}
            else:
                plan.append((sym, side, None))
                decisions[key] = {"action": "hold", "reason": model_reason,
                                  "source": "model"}
        # 未表态的按 hold 处理（照实记下来，便于事后看模型漏没漏）
        for key, (sym, side, _p, _m) in allow.items():
            if key not in seen:
                plan.append((sym, side, None))
                decisions[key] = {"action": "hold", "reason": None,
                                  "source": "model_omitted_default_hold"}
                LOG(f"  ⚠ 持仓管理：{key} 模型未表态 → 按 hold 处理")

        cap = int(self.ecfg.get("max_close_per_round", 0) or 0)
        closes = [x for x in plan if x[2] is not None]
        execution = {}
        if cap > 0 and len(closes) > cap:
            LOG(f"  ⚠ 持仓管理：模型要平 {len(closes)} 个 > 上限 {cap} → 只执行前 {cap} 个")
            for sym, side, _reason in closes[cap:]:
                execution[f"{sym}|{side}"] = "blocked_close_cap"
            closes = closes[:cap]
        closed_n = 0
        for sym, side, reason in closes:
            key = f"{sym}|{side}"
            # ---- 微利不平（覆盖不了往返手续费）----
            cover = float(self.ecfg.get("profit_cover_pct", 0.001) or 0)
            if cover > 0:
                row = next((r for k, (s, sd, r, m) in allow.items()
                            if k == key), None)
                entry = float((row or {}).get("entry_price") or 0)
                px = _live(sym)
                gain = (px - entry) / entry if (entry and px and side == "long") else \
                       (entry - px) / entry if (entry and px) else 0.0
                if 0 < gain < cover:
                    LOG(f"  ⏸ 持仓管理 {sym} {side}：浮盈 {gain:.3%} < {cover:.3%} 成本线，"
                        f"不平（白交往返手续费）。模型理由：{reason}")
                    try:
                        journal.log("exit_decision", sym=sym, side=side,
                                    action="blocked_profit_cover", gain_pct=round(gain * 100, 4),
                                    cover_pct=cover * 100, reason=reason,
                                    why="浮盈低于成本线，执行层不放行")
                    except Exception:
                        pass
                    execution[key] = "blocked_profit_cover"
                    continue
            try:
                msg = ex.market_close(sym, side, text=CLOSE_TEXT)
            except Exception:
                execution[key] = "close_error_or_unknown"
                # 保持既有异常传播/不盲目重试语义，但不丢失本轮模型输入与决定。
                self._log_exit_briefs(brief_rows, allow, decisions, execution,
                                      dt, "parsed")
                raise
            ok = ("无持仓可平" not in msg)
            closed_n += 1 if ok else 0
            execution[key] = "close_submitted" if ok else "close_not_confirmed"
            LOG(f"  🧠 模型平仓 {sym} {side} → {msg} | 理由：{reason}")
            try:
                journal.log("exit_close", sym=sym, side=side, ok=ok,
                            reason=reason, msg=str(msg)[:160],
                            why="持仓管理线程：模型 close 信号 → 市价平 + 撤本方向条件单")
            except Exception:
                pass
        # 「拦了什么就必须显示什么」：方向写错被修正的，也照实落进决策流水
        note = (f"⚠ 模型把方向写错，已按持仓实为修正：{'；'.join(fixed)}" if fixed else "")
        self._log_decision(plan, note, closes)
        self._log_exit_briefs(brief_rows, allow, decisions, execution, dt, "parsed")
        self._last = {"t": time.time(), "n_hold": len(plan) - len(closes),
                      "n_close": len(closes)}
        return closed_n

    def _log_exit_briefs(self, rows: list, allow: dict, decisions: dict,
                         execution: dict, latency_s: float, status: str):
        """逐持仓保存模型看到的原始 brief、五条件复核值与最终执行状态。"""
        for row in rows:
            try:
                sym, side = row.get("contract"), row.get("side")
                key = f"{sym}|{side}"
                held_row = allow.get(key)
                meta = (held_row[3] or {}) if held_row else {}
                opened = float(meta.get("t_at_open") or 0)
                trade_id = (meta.get("trade_id") or
                            (f"{sym}|{side}|{int(opened * 1000)}" if opened else None))
                ann = row.get("ann") or {}
                walk_key = "walk_up_bars" if side == "long" else "walk_dn_bars"
                walk_now = _num(ann.get(walk_key))
                previous = self._last_exit_walk.get(trade_id) if trade_id else None
                walk_decreasing = (walk_now < previous
                                   if walk_now is not None and previous is not None else None)
                if trade_id and walk_now is not None:
                    self._last_exit_walk[trade_id] = walk_now
                    if len(self._last_exit_walk) > 2048:
                        self._last_exit_walk.pop(next(iter(self._last_exit_walk)))
                checks = _exit_rule_checks(row, side, walk_decreasing)
                decision = decisions.get(key) or {
                    "action": "no_output" if status != "parsed" else "hold",
                    "reason": None,
                    "source": "invalid_model_output" if status != "parsed"
                             else "model_omitted_default_hold",
                }
                journal.log(
                    "exit_brief", trade_id=trade_id, sym=sym, side=side,
                    # 🔴 10-09 修：这里原来写的是 `ann.get("m1")`（15 根 1m 的**数组**），
                    #   却塞进名为 `m1_mom` 的**标量**字段 → 落盘出来是一串数组，
                    #   下游按标量读全部失真。改存 `_m1_scalar()` 折出的标量。
                    m1_mom=_m1_scalar(ann.get("m1")), m1h_mom=ann.get("m1h_mom"),
                    regime=ann.get("regime"), band_pos=ann.get("band_pos"),
                    mfe_r=row.get("r_mfe"), mae_r=row.get("r_mae"),
                    give_back_pct=row.get("give_back_pct"),
                    peak_ratio=row.get("peak_ratio"),
                    stall_lock=row.get("stall_lock"),
                    r_hit=row.get("r_hit"),
                    rule_checks=checks,
                    rule_1_reversal_confirmed=checks["rule_1_reversal_confirmed"],
                    rule_2a_profit_giveback=checks["rule_2a_profit_giveback"],
                    rule_2b_failed_start=checks["rule_2b_failed_start"],
                    rule_3_loss_structure_bad=checks["rule_3_loss_structure_bad"],
                    rule_5_stall_lock=checks["rule_5_stall_lock"],
                    model_action=decision.get("action"),
                    model_reason=decision.get("reason"),
                    decision_source=decision.get("source"),
                    execution=execution.get(key, "not_attempted"),
                    model_status=status, model_latency_s=round(latency_s, 3),
                    features=row)
            except Exception:
                # 纯遥测：任何落盘/格式错误不得影响平仓控制链。
                continue


    # -------------------------------------------------- 落盘（可见性）
    def _log_decision(self, plan: list, note: str, closes: list):
        """把本轮决策写进 `runtime/exit_decisions.md`（看板/人工复盘可见）。

        🔴 「拦了什么就必须显示什么」：hold 的理由、被丢弃的幻觉品种、
        微利不平的拦截，全部照实落盘。
        """
        ts = datetime.now().strftime("%m-%d %H:%M:%S")
        if note:
            body = f"（{note}）"
        elif not plan:
            body = "（无持仓）"
        else:
            parts = []
            for sym, side, reason in plan:
                if reason is None:
                    parts.append(f"{sym.replace('_USDT', '')} {side} → 持有")
                else:
                    parts.append(f"{sym.replace('_USDT', '')} {side} → 平仓（{reason}）")
            body = "；".join(parts)
        line = f"\n## {ts}\n- 持仓管理：{body}"
        try:
            state.append_line(self.decisions_path, line)
            _trim(self.decisions_path, int(self.ecfg.get("brief_tail_lines", 200) or 200))
        except Exception:
            pass


def _qm(sym: str) -> float:
    """合约乘数（quanto_multiplier），取不到给 1。executor.spec 内部有缓存。"""
    try:
        return float(ex.spec(sym).get("quanto_multiplier") or 1)
    except Exception:
        return 1.0


def _live(sym: str) -> float:
    """现价：优先交易所持仓行的 mark_price（调用方已取），这里只兜底。"""
    try:
        from market import feeder
        return float(feeder.live_price(sym) or 0)
    except Exception:
        return 0.0


def _trim(path: str, keep_lines: int):
    """决策流水滚动保留（只追加的文件不能无限涨）。"""
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > keep_lines:
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(lines[-keep_lines:])
    except Exception:
        pass
