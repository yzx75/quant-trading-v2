# -*- coding: utf-8 -*-
"""
risk/manager.py — 风控层：代码全权负责一切数字（价/量/SL/TP/杠杆）

模块职责：把「模型给的品种 + 方向」翻译成一组可执行的风控数字。
模型**不输出** price/size/sl/tp/lever（提示词明令），全部由本模块计算。

## 当前真实口径（🔴 10-07 16:3x 用户令：杠杆 30x / SL 1.2% / TP 1.0%）

> **ROE 口径速查**（用户口中的「交易所显示 X% 盈利」是 ROE，不是价格距离）：
> `ROE = 价格距离 × 杠杆`。现役 30x ⇒ TP 1.0% = **ROE 30%**、SL 1.2% = ROE 36%。
> 日后改 `lev_default` 必须回来重算这两个数。

止损 SL（`compute_sl`）
    = **入场价 × 固定 1.2%**（`exit.sl_min_pct`），多=向下、空=向上。
    唯一口径。沿用 10-03 21:30 确立的「固定百分比」原则（用户原话「什么对面轨道
    和 atr，就用固定百分比」），数值历经 1.5% → 0.8% → 0.68% → **1.2%**
    （10-07 16:2x 随杠杆降到 30x 而放宽，理由见 `compute_sl` docstring）。
    `band` 形参仅为签名兼容，布林数据不参与止损计算。

    🔴 **SL 上限由 `liq_guard` 决定，不由杠杆决定**（10-07 16:2x 实测）：
    约束 `(1/lev − mr) × liq_guard_ratio(0.6) ≥ SL距/px`。30x 下：
      · mr=0.8333%（DOGE/ZEC/SNDK/SKHYNIX/HYPE 基础档）→ 强平距 2.500% → 上限 1.500%
      · mr=1.0000%（**实测持仓档**：ETH 实盘 5 张时 Gate 把 mr 从 0.30% 抬到 1.00%）
        → 强平距 2.333% → 上限 1.400%
    现取 1.2% ⇒ 余量 0.30pp(基础档) / **0.20pp(持仓档)**。
    ⚠️ **口径陷阱**：`check_open` 的 liq_guard 校验读 `ex.spec()` 的**基础档** mr，
    而真实强平按**持仓档** mr（Gate 按仓位大小抬档）⇒ **「校验通过」≠「实际安全」**，
    定 SL 必须按持仓档算余量。

止盈 TP（`compute_tp`）
    = **入场价 × 固定 1.0%**（`exit.tp_fixed_pct`），与 SL 同源的固定百分比口径。
    对面轨 → 2×ATR → `tp_floor_pct` 三级**仅在 `tp_fixed_pct=0` 时**才会走到
    （保留为回退开关）。
    10-05 22:0x 由「布林对面轨」改为固定值，动因：实测 TP 中位仅 0.578%，远小于
    SL 1.5% → 盈亏比 0.48、保本线 67.7%，高于实际胜率 56%，**结构性必亏**。
    改为固定值后盈亏比转为正，配对 bootstrap z=+4.69 显著（`_bt_sl_tp_pair.py`，
    257 笔真实开仓；那组数据是 SL 0.8%/TP 1.5% ⇒ 盈亏比 1.26）。

    🔴 **口径：用户说的「交易所显示 X% 盈利」是 ROE，不是价格距离**。
    `ROE = 价格变动% × 杠杆`，故同一个 `tp_fixed_pct=1.0%` 的 ROE 随杠杆变：
      · 50x → ROE 50%（10-07 02:3x~16:2x 期间）
      · **30x → ROE 30%（10-07 16:2x 用户令「交易所显示 30% 盈利就平仓」）**
    ⇒ 降到 30x 时本键**无需改动**，自动变成用户要的 30%。
    ⚠ 日后若再改 `lev_default`，务必回来重算（= `tp_fixed_pct` × 杠杆）。
    ⚠ 旧教训仍适用：**改 SL 宽度必须连带重测 TP 口径**，二者强耦合。
      10-07 16:2x 把 SL 0.68%→1.2% 后盈亏比 1.47 → **0.83**（保本线 54.6%），
      该改动**尚未经回测验证**（见交付时的已知限制）。

杠杆
    = **全池统一 30x**（`sizing.lev_default=30`，`lev_override={}`），
    并按合约 `leverage_max` 钳制（`lev_for`）。10-02 20:0x 首定 50 → 10-07 02:2x
    降到 20 → 02:3x 回到 50 → **10-07 16:2x 用户令改 30**。
    仅当 SL 距极端过大（可容纳杠杆 < `lev_min_sl`=10）才拒开。
    ⚠ 杠杆**不改变费率**（费率按名义额收，降杠杆只是同保证金下缩小仓位、
      手续费同比例下降，**不是"费率变低"**）。
    ⚠ 30x 的收益是**强平距离拉开**：30x 下 ≥2.333%（持仓档口径）vs 50x 的 1.167%，
      宽了 1 倍 —— 这才是能把 SL 从 0.68% 放宽到 1.2% 的**唯一原因**。
      ROE 口径下**杠杆不改变盈亏金额**，改的是名义额与手续费金额。

定仓（10-07 用户令「统一 3U 保证金」，`sizing.fixed_margin_u=3.0`）
    = **固定 3U 保证金**：`size = int(3U / 单张保证金)`，下限 1 张。
      单张保证金 = `quanto_multiplier × px / lev`；张数为整数 ⇒ 小面值品种会偏离
      目标（实测 SKHYNIX/BNB 命中 3.00U 完美，多数在 ±3%，SOL 装不下只能 1 张）。
    三道**上界保护**（只当护栏，正常不触发；10-07 16:3x 起全部改为 fail-closed）：
      ① 名义帽   = 权益 × `notional_cap_x`(4.5)   → 装不下 1 张则**拒开**
      ② 风险帽   = 权益 × `risk_cap_pct`(3.7%)    → 装不下 1 张则**拒开**
      ③ 保证金占比 ≤ `margin_max_pct`(10.5%)      → 从**结果**反查，超了**拒开**
    ⚠️ **这三处曾是 `max(1, …)`**（10-07 16:3x 修）：`max(1,…)` 在「单张就已超帽」时
    返回的是**仍然超帽的 1 张**，钳制无效且保证金反向放大 —— 实测 30x/SOL 曾算出
    保证金 6.17U = 权益 15.7%（设计上限 8%）。**两道帽同时失效比不做保护更糟。**
    ⚠️ 仓位数上限 `limits.max_hf_positions`(9)，按**品种数**计（非仓腿数）。

仓名额
    = **只有高频池**，上限 `limits.max_hf_positions`（9）。按**品种数**计数
    （不是仓腿数，见 `agents/decision.py::_try_open`）。翻仓豁免名额
    （替换非新增，传 `len(legs)-1`）。
    ⚠ **趋势仓已于 10-03 删除**，不要再按「两池隔离」理解本模块。

出场
    SL/TP 条件单由交易所侧触发（`text=t-v2-SL` / `t-v2-TP`），两者都是**开仓即固定**
    的固定百分比（`exit.sl_min_pct` / `exit.tp_fixed_pct`）。引擎侧的机械护盈
    （A 保本 / B 阶梯追踪 / C 连续跟随 / E 动态止盈+保本兜底 / 分批止盈 / 时间止损）
    已于 10-09 按用户令**整体删除**，本模块不再提供 `dyn_rope` / `armed_line`。
    出场现在只有三条路：交易所条件单、D 段止盈兜底补单、持仓管理模型主动平仓。

## 已废止、不要按旧口径理解的东西

| 旧说法 | 现状 |
|---|---|
| 同向止损锁 60min | **已删**。`mark_stop` 零调用，纯记录；开仓不再被「刚止损过」拦 |
| 周末（UTC+8）只平不开 | **已撤销**（`is_weekend_utc8` 零调用，函数已删） |
| 高频≤3 / 趋势≤2 两池 | 趋势池已删，只剩高频池，上限 9 |
| SL = 对面轨 + ATR 缓冲 | 10-03 21:30 废止，改固定百分比（10-05 数值 1.5%→0.8%） |
| TP = 对面轨 | 10-05 22:0x 废止，改固定 1.5%（`tp_fixed_pct`） |
| 名义帽 3× ∩ 品种指定杠杆 | 杠杆已统一（`lev_override` 为空）；现役 **30x** |

## 🔴 方向判定的唯一依据（10-05 实测踩坑，写进来防重犯）

**Gate `GET /futures/usdt/positions` 的返回体里没有 `side` 字段。**
实测字段全集里只有 `size`（带符号）与 `long_price`/`short_price`，方向必须靠
**带符号的 size** 判定：`size > 0` = 多仓，`size < 0` = 空仓。

⚠ **`live.json` 的 `positions[].side` 是引擎 `_pub_pos()` 派生的**（按 size 正负
反推），且同一处的 `size` 已被 `abs()` 取成绝对值 —— 所以 **`live.positions`
不可用来回判方向**（size 永远是正数，判出来的方向会与真实持仓相反）。
要判方向必须用交易所原始行的**带符号 size**。

这个坑的代价：读 `live.positions` 判方向 → 空/多反转 → 后续动作打在错误的腿上。
"""
from __future__ import annotations

import math
from datetime import datetime

from core import log as core_log
from core import state
from exchange import executor as ex

LOG = core_log.log


class RiskManager:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        s = cfg["sizing"]
        # 🔴 10-07 用户令「统一 3U 开仓」：>0 = 固定保证金口径（主口径）；
        #   0/缺省 = 逐字节回退旧的「权益百分比档位」口径，可一键回退。
        self.fixed_margin_u = float(s.get("fixed_margin_u", 0) or 0)
        self.margin_min = float(s["margin_min_pct"])
        self.margin_max = float(s["margin_max_pct"])
        self.wr_high = float(s["wr_tier_high"])
        self.wr_mid = float(s["wr_tier_mid"])
        self.notional_cap_x = float(s["notional_cap_x"])
        self.lev_default = int(s["lev_default"])
        self.lev_override = {k: int(v) for k, v in (s.get("lev_override") or {}).items()}
        self.lev_min_sl = int(s.get("lev_min_sl", 10))          # 10-02：SL 反推杠杆的兜底下限
        self.risk_cap_pct = float(s.get("risk_cap_pct", 0.01))  # 10-02：单笔风险帽（占权益）
        g = cfg["guards"]
        self.liq_ratio = float(g["liq_guard_ratio"])
        self.liq_fallback = float(g["liq_safe_fallback"])
        # 注：max_trades_day（当日笔数上限）已于 10-02 17:4x 按用户拍板移除，
        #     计数仍照记（bump_trades）供复盘统计，但不再拦开仓。
        # 止损唯一口径 = 固定百分比（10-03 21:30 立规，10-05 数值 1.5%→0.8%）。
        #   原 sl_min_atr（1.5×ATR15）已废止，配置项从 config.yaml 删除；
        #   此处不再读取，保留注释防后人误加回。
        self.sl_min_pct = float(cfg["exit"].get("sl_min_pct", 0.015))
        # TP 固定百分比（10-05 22:0x 新增）：>0 = 唯一口径，=0 = 回退到
        #   下面的对面轨 / 2×ATR / tp_floor 三级。
        self.tp_fixed_pct = float(cfg["exit"].get("tp_fixed_pct", 0.0))
        self.tp_floor = float(cfg["exit"]["tp_floor_pct"])
        # 10-03 01:50 方案 B：TP 挪到 2×ATR 远兜底（正常永远轮不到，纯粹防引擎挂）
        self.tp_far_atr = float(cfg["exit"].get("tp_far_atr", 2.0))
        # 以下是「对面轨 SL」时代的参数，10-03 21:30 随固定 1.5% 口径一并停用。
        #   sl_band_buf 于 10-03 22:1x 被 compute_tp 复用为「TP 防贴轨缓冲」，
        #   语义已变（同一把尺子量 TP 侧），故下面另开 tp_band_buf 别名。
        #   （原sl_far_atr / sl_far_pct / sl_mid_buf 三行赋值与对应 config 键
        #     已于 10-05 删除：compute_sl 不再读它们，全项目零读取点。）
        self.sl_band_buf = float(cfg["boll"].get("sl_band_buf_pct", 0.0005))
        self.tp_band_buf = self.sl_band_buf          # TP 侧防贴轨缓冲（10-03 22:1x）
        self.max_hf = int(cfg["limits"]["max_hf_positions"])
        self.min_tp_pct = float(g.get("min_tp_pct", 0.002))   # TP 距离硬下限（≈2×往返手续费）
        self.state_path = cfg["runtime_dir"] + "/risk_state.json"

    # -------------------------------------------------- 状态
    def _st(self) -> dict:
        return state.load(self.state_path, {})

    def _save(self, st: dict):
        state.save(self.state_path, st)

    def new_day(self, equity: float):
        st = self._st()
        key = datetime.now().strftime("%Y-%m-%d")
        if st.get("day") != key:
            # 10-05：原close_votes 初始化已删——「平仓投票」机制早已撤销
            # （用户 07:42/07:48 拍板：模型只提建议、无平仓权，出场只认条件单），
            # 该键全项目零读取点，留着只会让人以为还有投票机制。
            st = {"day": key, "start_equity": equity, "trades": 0,
                  "peak": equity}
            self._save(st)
        st["equity"] = equity
        st["peak"] = max(float(st.get("peak") or equity), equity)
        self._save(st)

    def bump_trades(self):
        st = self._st()
        st["trades"] = int(st.get("trades") or 0) + 1
        self._save(st)

    # -------------------------------------------------- 杠杆
    def lev_for(self, contract: str) -> int:
        base = self.lev_override.get(contract, self.lev_default)
        try:
            cap = int(float(ex.spec(contract).get("leverage_max") or 0))
            if cap > 0:
                base = min(base, cap)
        except Exception:
            pass
        return base

    # -------------------------------------------------- 止损止盈（布林对称规则）
    def compute_sl(self, contract: str, side: str, px: float, band: dict) -> float:
        """SL = 入场价 ×(1 ∓ sl_min_pct)（**唯一口径 = 固定百分比**）。

        当前 sl_min_pct = **1.2%**（10-07 16:2x 随杠杆降到 30x 而放宽；此前 0.68%、
        0.8%、1.5%）。**只改数值不改原则**（仍是固定百分比），依据见模块 docstring
        与 config.yaml 同段注释。⚠ 数值受 `liq_guard` 上限约束
        （30x 下 1.4%~1.5%，按持仓档 mr 取 1.4%），**不是自由参数**。

        废止两套旧口径（用户原话「什么对面轨道和 atr，就用 1.5%」）：
          ① 对面轨（多=下轨 / 空=上轨）——带宽随行情剧烈变化，SL 距离在 0.1%~2%+
             之间乱跳，同一策略下每单风险敞口不一致，事后无法归因统计；
          ② 1.5×ATR15——窄带品种 ATR15 只有 0.1%，乘完反而比固定值更近，噪声照样
             扫掉；且"随波动自适应"意味着每单距离都不同，同样是归因障碍。
        固定值的好处：风险敞口可比、回测口径干净、参数含义唯一。
        band 形参保留仅为签名兼容（布林数据不再参与止损计算）。
        """
        d = px * self.sl_min_pct
        return round(px - d if side == "long" else px + d, 6)

    def compute_tp(self, contract: str, side: str, px: float, band: dict) -> float:
        """TP = 入场价 × (1 ± tp_fixed_pct)（10-05 22:0x 用户拍板，唯一口径）。

        当前 tp_fixed_pct = **0.010（1.0%）**，配现役 SL 1.2% ⇒ 盈亏比 0.83。
        （10-05 22:0x~10-07 16:2x 前是 0.015，SL 0.8% 时盈亏比 1.26。）>0 时本函数
        **只走固定距离**，下面「对面轨 → 2×ATR → tp_floor」三级全部短路；只有把
        tp_fixed_pct 置 0 才会走到它们，故旧逻辑原样保留作回退开关。

        实测（`_bt_sl_tp_pair.py`：257 笔真实开仓 × 5m K线重放，风险帽名义、
        往返费 0.10%、keep_r 0.8、分批 0.4）：
          SL1.5%+对面轨（改动前） 笔均 +0.0062U  盈亏比 0.48  保本线 67.7%
          SL0.8%+对面轨           笔均 +0.0091U  盈亏比 0.69  （+0.016U，不显著）
          SL0.8%+TP固定1.5%      笔均 **+0.0920U** 盈亏比 **1.26** 保本线 44.2%
            配对 bootstrap z=+4.69、95%CI[+0.056,+0.137]、P(优)=100% **显著**
        机制：胜率 69.0%→56.4% 下降，但均赢 +0.147→+0.421 涨 2.9 倍，盈亏比翻倍，
        保本线从 68% 降到 44%，**首次低于实际胜率** —— 这是本次改动的全部意义。

        —— 以下为 tp_fixed_pct=0 时的旧口径说明（TP = **对面轨**，10-03 22:1x 拍板）

        此前是 2×ATR15「远兜底」，被证伪为**比 armed 还近**的假兜底：
          · armed = max(手续费, 0.5 × sl_dist0)，SL 固定 1.5% → armed = 0.75%；
          · 2×ATR15 实测距离仅 0.21%~0.78%，15 品种里 8 个 < 0.75%
            （在仓 5 笔中 4 笔 TP < armed）→ 价格冲到 armed 之前就被
            交易所 TP 条件单打掉，**动态绳（E 段）根本没机会追峰值**。
          这就是「赚文字腿」在宽止损下的新机制。

        改对面轨的两份实证（同一份数据、两种口径互为交叉验证）：
          ① 真实链路（含 TP 硬下限过滤，脚本 _bt_tp_real.py，248 笔真实开仓、
             SL 固定 1.5%、名义按风险帽 26.7U、扣往返费 0.10%）：
               2×ATR(现状) 240 笔 笔均 +0.1704 / 均赢 +0.263
               对面轨      207 笔 笔均 **+0.2017** / 均赢 **+0.302**  ← 最优
               中轨        193 笔 笔均 +0.1572 / 均赢 +0.237  ← 全面垫底
          ② 纯机械信号（脚本 _bt_tp_compare2.py = 10-02 原脚本把 SL 从
             1.0×ATR 换成固定 1.5%，15 品种×500 根 15m）：
               对面轨 每笔 **+0.130%**、半轨宽 +0.142%、中轨 +0.137%
               1.0×ATR +0.051%、0.8×ATR +0.032%（高胜率陷阱：胜 77% 但每笔仅 0.032%）
          ⚠ 关键教训：10-02 原结论是「对面轨每笔 **-0.120%**、远不如中轨」，
            那是因为当时 SL = 1.0×ATR（下限 0.2%），窄止损下对面轨根本够不到。
            **SL 宽度决定远 TP 是否可行**——同为对面轨，SL 0.2% 时每笔 -0.120%，
            SL 1.5% 时每笔 +0.130%。改 SL 必须连带重测 TP 口径。

        对面轨无效（已越过/重合）→ 退回 2×ATR → 再退回现价 ±tp_floor。
        """
        if self.tp_fixed_pct > 0:                       # 唯一口径（10-05 22:0x）
            d = px * self.tp_fixed_pct
            return round(px + d if side == "long" else px - d, 6)

        dn, up = band.get("dn"), band.get("up")
        tgt = up if side == "long" else dn
        if tgt and ((side == "long" and tgt > px) or (side == "short" and tgt < px)):
            # 防贴轨：TP 正好压在上轨上，噪音一碰就打掉。往标的方向再推一点缓冲。
            buf = float(tgt) * self.tp_band_buf
            tp = float(tgt) - buf if side == "long" else float(tgt) + buf
            if (side == "long" and tp > px) or (side == "short" and tp < px):
                return round(tp, 6)
        atrv = float(band.get("atr") or 0)
        if atrv > 0:
            d = self.tp_far_atr * atrv
            return round(px + d if side == "long" else px - d, 6)
        return round(px * (1 + self.tp_floor) if side == "long" else px * (1 - self.tp_floor), 6)

    # -------------------------------------------------- 主校验
    def _liq_pct(self, contract: str, lev: int) -> float:
        """强平距离（逐仓近似 1/lev，扣维持保证金率）"""
        try:
            mr = float(ex.spec(contract).get("maintenance_rate") or 0.005)
        except Exception:
            mr = 0.005
        return max(1.0 / max(lev, 1) - mr, 0.005)

    def check_open(self, contract: str, side: str, win_rate: float, band: dict,
                   equity: float, hf_count: int,
                   same_sym_side_pos: bool, px: float = 0.0) -> tuple:
        """全护栏校验 + 数字生成。返回 (order or None, why)。
        order: {contract, side, price, sl, tp, size, lev, risk_u, margin_pct}
        px 不传则实时拉现价（可注入以便测试）。"""
        if contract not in self.cfg["universe"]:
            return None, "合约不在交易池"
        if px_ok := (band.get("mid") and band.get("up") and band.get("dn")):
            pass
        else:
            return None, "布林轨道数据缺失"
        if px <= 0:
            from market import feeder as _fd
            px = _fd.live_price(contract)
        if px <= 0:
            return None, "无现价"
        # 胜率档
        wr = float(win_rate or 0)
        if wr > 1:
            wr /= 100.0
        if wr < 0.50:
            return None, f"自估胜率 {wr:.0%} <50%，拒开（保证金档要求）"
        margin_pct = 0.03 if wr >= self.wr_high else (0.02 if wr >= self.wr_mid else 0.01)
        # SL / TP
        sl = self.compute_sl(contract, side, px, band)
        tp = self.compute_tp(contract, side, px, band)
        dist = abs(px - sl)
        if dist <= 0:
            return None, "止损距离为 0"
        if not ((sl < px < tp) if side == "long" else (tp < px < sl)):
            return None, "止损止盈方向错"
        rr = abs(tp - px) / dist
        # 10-05 删：原「SL 最小距离下限」整块已移除。
        #   原因：`compute_sl` 现返回**固定百分比**（当前 0.8%），所以 `dist` 与下限
        #   `px * sl_min_pct` 恒等——`dist < sl_floor` 只会因 `round(..., 6)`
        #   的取整误差成立（实测 17 次命中全部是 1.5e-6 < 1.85e-6 这个量级）。
        #   实盘日志因此刷了 **1912 条**"📏 SL 距离 1.50% 低于下限，推远至…（固定 1.50%）"
        #   ——纯噪音，且掩盖了真正需要关注的 SL 变动。
        #   盈亏比不设门槛（原 min_rr 门槛早已删除）。
        # TP 下限（10-02 12:05 拍板；10-09 随机械护盈段一并简化）：
        #   止盈必须「付得起往返手续费 + 留下一点净利」才有意义：
        #     手续费 = 兜底常数 `tp_fee_ref_pct`（= 实测往返双 taker 0.0010 × 1.2，
        #       刻意留 20% 缓冲；开仓前**还没有实单**、拿不到该单真实 fee_rt，只能保守估）
        #     最小净利 = `tp_min_net_pct`
        #   ⚠ 10-09：动态绳（`dyn_rope`）已随 A/B/C/E/分批/时间止损整体删除，
        #     本下限不再含「绳子」那一项 —— 原公式
        #     `max(min_tp_pct, 锁线 + 绳子 + 落袋净利下限)` 里的绳子项，本意是给
        #     「止盈线程提前落袋」预留空间；现在出场只由固定 TP/SL 条件单、
        #     D 段补单与持仓管理模型决定，没有线程会提前落袋，故该项一并去掉。
        #   ⚠ 与删前**不完全相同**：旧下限随 TP 距离浮动（tp_dist 1.0% → 0.402%），
        #     新下限是常数 0.27%（= tp_fee_ref_pct + tp_min_net_pct）。
        #     对现役 `tp_fixed_pct = 1.0%` **零影响**（两种口径都不拒开）；只有将来
        #     把 TP 收到 0.4% 以下时新口径才会更宽松。详见 config.yaml 同名键注释。
        g_ = self.cfg["guards"]
        tp_dist = abs(tp - px) / px if px else 0
        tp_need = max(self.min_tp_pct,
                      float(g_.get("tp_fee_ref_pct", 0.0012))
                      + float(g_.get("tp_min_net_pct", 0.0015)))
        if tp_dist < tp_need:
            return None, (f"TP 距离 {tp_dist:.2%} < 下限 {tp_need:.2%}"
                          f"（max({self.min_tp_pct:.2%}, 往返手续费+最小净利)），"
                          f"打到位也付不起手续费，放弃")
        # 爆仓护栏（10-02 用户拍板改造）：核心 SL「对面轨+缓冲」必须保住，
        # 不再直接把 SL 压进轨道内（旧逻辑 2 天触发 798 次，SL 落轨道内的单 79 笔净 -5.02U）。
        #   约束 (1/lev − mr) × liq_ratio ≥ SL距离/px
        #   → 可容纳该 SL 的最大杠杆 = 1/(dist/px/liq_ratio + mr)：降杠杆来腾出 SL 空间
        try:
            mr = float(ex.spec(contract).get("maintenance_rate") or 0.005)
        except Exception:
            mr = 0.005
        lev_base = self.lev_for(contract)
        lev_need = int(math.floor(1.0 / max(dist / px / self.liq_ratio + mr, 1e-9)))
        # 10-02 05:20 改回：不再因 SL 距降杠杆。实测依据——
        #   ① 降杠杆不省保证金：保证金守护仍会把保证金追加到
        #      max(margin_guard_sl_x×SL距, 5%)×名义，该目标由 SL 距决定，与杠杆无关；
        #   ② 降杠杆有实害：单张保证金 = 面值×价/杠杆 变大，SOL 等大面值品种
        #      装不进权益 1%~3% 档而彻底出局（100x 时 1.18U 可开，69x 时 1.71U 拒开）。
        # 现策略：保持基准杠杆 + SL 放对面轨外，强平距离交给保证金守护保证。
        # 仅当 SL 距极端过大（可容纳它的杠杆 < lev_min_sl）才拒开，避免巨亏敞口。
        if lev_need < self.lev_min_sl:
            return None, (f"对面轨 SL 距 {dist / px:.2%} 需杠杆 ≤{lev_need}x"
                          f" < 下限 {self.lev_min_sl}x，放弃")
        lev_sl = lev_base
        if same_sym_side_pos:
            return None, f"{contract} {side} 已有同向高频仓"
        # 高频仓名额（10-05改文案：原「（趋势仓另计）」已删——趋势仓 10-03 已删除，
        #   根本不存在第二个池；留着会让人以为还能再开 2 个趋势仓）。
        if hf_count >= self.max_hf:
            return None, f"高频仓已达上限 {self.max_hf}"
        # 当日笔数上限：10-02 17:4x 用户拍板**去掉**（老护栏，10-01 就有；
        #   今天 17:15 就把 80 笔烧完、导致 25 次拒开，把新加的亏后冷却锁顶死）。
        #   开仓频次现由「亏后冷却 2h + 仓位数 + 胜率闸门 + wide 禁逆向」约束。
        # 定仓（10-07 用户令）：**固定 fixed_margin_u 保证金**为主口径；
        #   名义帽 / 风险帽降级为**上界保护**（3U×30x=90U 正常都不触发）。
        mult = float(ex.spec(contract).get("quanto_multiplier") or 1)
        lev = lev_sl                       # 10-02 05:20：回到基准杠杆（不再按 SL 反推下调）
        per_margin = mult * px / lev       # 单张保证金
        if per_margin <= 0:
            return None, "合约规格异常"
        if self.fixed_margin_u > 0:
            # 名义 = fixed_margin_u × lev；张数 = 名义 ÷ 单张名义。
            # ⚠ 张数为整数 ⇒ 小面值品种会偏离目标（实测 SOL −21%、ETH −13%），
            #   这是合约粒度决定的、不是 bug，详见 configs/config.yaml 同段注释。
            size = max(1, int(self.fixed_margin_u / per_margin))
        else:
            # 旧口径（fixed_margin_u=0，逐字节回退）：保证金档 × 权益 + 名义帽
            cap_min = math.ceil(equity * self.margin_min / per_margin)
            cap_max = int(equity * self.margin_max / per_margin)
            if cap_min > cap_max:
                return None, (f"单张保证金 {per_margin:.3f}U 装不进权益 "
                              f"{self.margin_min:.0%}~{self.margin_max:.0%}（权益 {equity:.2f}U）")
            cap_notional = int(equity * self.notional_cap_x / (mult * px)) if mult * px > 0 else cap_max
            size = min(cap_max, cap_notional)
            size = max(size, cap_min)
        if size < 1:
            return None, "定仓不足 1 张"
        # 名义帽最终钳制（上界保护）
        # 🔴 10-07 16:3x 修「缩仓下限」缺陷：原 `max(1, …)` 在**单张就已超帽**时
        #   会返回「仍然超帽的 1 张」（钳制无效），且保证金可能远超设计档位。
        #   实测 30x/SOL：名义 185U > 帽 177.3U → 钳制无效 → 保证金飙到 6.17U(权益 15.7%)。
        #   改为 **fail-closed**：装不下就拒开，不发出超帽的仓。
        if size * mult * px > equity * self.notional_cap_x:
            fit_n = int(equity * self.notional_cap_x / (mult * px)) if mult * px > 0 else 0
            if fit_n < 1:
                return None, (f"单张名义 {mult * px:.2f}U 已超名义帽 "
                              f"权益×{self.notional_cap_x:g}={equity * self.notional_cap_x:.2f}U"
                              f"（{contract} 面值×价过大），装不下 1 张 → 拒开")
            size = fit_n
        margin = size * per_margin
        risk_u = dist * mult * size
        # 单笔风险帽（10-02）：名义×SL距离 ≤ 权益×risk_cap_pct。
        # 降杠杆后 SL 变远，须防单笔风险被动放大，超出则缩仓
        cap_u = equity * self.risk_cap_pct
        if cap_u > 0 and dist * mult > 0 and risk_u > cap_u:
            fit_r = int(cap_u / (dist * mult))
            if fit_r < 1:
                # 同上：缩到 0 张 = 装不下 ⇒ 拒开（原来会退化成超帽的 1 张）
                return None, (f"单张风险 {dist * mult:.3f}U 已超单笔风险帽 "
                              f"权益×{self.risk_cap_pct:g}={cap_u:.3f}U"
                              f"（{contract} 单张面值×SL距离过大），装不下 1 张 → 拒开")
            size = fit_r
            margin = size * per_margin
            risk_u = dist * mult * size
            LOG(f"  📐 {contract} 风险帽：缩仓至 {size} 张"
                f"（风险 {risk_u:.2f}U ≤ 帽 {cap_u:.2f}U）")
        # 🔴 10-07 16:3x 补最后一道兜底：两道帽各自都可能被「至少 1 张」架空，
        #   这里从**结果**反查保证金占比，超 margin_max_pct 直接拒开（而非再缩一次仓——
        #   缩仓只会回到上面的死循环）。实数 1 张都装不下时只有两条路：拒开 或 放宽帽，
        #   项目铁律是 fail-closed，故拒开并留日志。
        if equity > 0 and self.fixed_margin_u > 0:
            mp = margin / equity
            if mp > self.margin_max:
                LOG(f"  🚫 {contract} 保证金占比 {mp:.1%} 超上限 {self.margin_max:.0%}"
                    f"（{margin:.2f}U）—— 单张面值过大，拒开")
                return None, (f"保证金占比 {mp:.1%} 超上限 {self.margin_max:.0%}"
                              f"（1 张需 {margin:.2f}U = 权益 {mp:.1%}）")
        order = {"contract": contract, "side": side, "price": px, "sl": sl, "tp": tp,
                 "size": size, "lev": lev, "win_rate": wr, "rr": round(rr, 2),
                 "risk_u": round(risk_u, 3), "margin": round(margin, 3),
                 "margin_pct": round(margin / equity, 4) if equity else 0}
        LOG(f"  🎯 代码定仓 {contract} {side} {size}张 @{px:g} SL {sl:g} TP {tp:g} "
            f"RR {rr:.2f} 保证金 {margin:.2f}U({order['margin_pct']:.1%}) 杠杆 {lev}x")
        return order, "ok"
