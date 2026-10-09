# -*- coding: utf-8 -*-
"""
exchange/executor.py — 下单执行层（纯执行，不做任何信号判断）

继承旧系统的全部实盘教训：
  1. 逐仓 isolated + 双仓模式 pos_margin_mode 铁律
  2. IOC 幽灵成交防护：下单后查 left 终态，未成交绝不记成功
  3. 条件单触发价必须按 order_price_round 取整（否则 Gate 静默 400）
  4. TP 走 maker 限价（省手续费）、SL 走 taker 市价（保命优先）
  5. 追加保证金：query change 绝不带 + 号；双仓模式 dual_side=dual_long/dual_short
"""
from __future__ import annotations

import math
import os
import time

from core import log as core_log
from exchange import gate_client as g

LOG = core_log.log


def _dry() -> bool:
    """DRY_RUN=1：干跑模式——引擎全流程照跑，但不发任何写请求"""
    return os.environ.get("DRY_RUN") == "1"


# ---------------------------------------------------------------- 规格与缓存
_spec_cache: dict = {}


def spec(contract: str) -> dict:
    if contract not in _spec_cache:
        _spec_cache[contract] = g.request(
            "GET", f"/futures/usdt/contracts/{contract}", authed=False)
    return _spec_cache[contract]


def _round_px(contract: str, px: float, toward=None) -> float:
    """按合约 order_price_round 取整（BTC=0.1，否则条件单 400）。
    toward=-1 向下（多单 SL 更远更安全）、+1 向上（空单 SL）、None 最近。"""
    try:
        step = float(spec(contract).get("order_price_round") or 0.1)
    except Exception:
        step = 0.1
    if step <= 0:
        return px
    if toward is None:
        return round(round(px / step) * step, 8)
    f = math.floor if toward < 0 else math.ceil
    return round(f(px / step) * step, 8)


# ---------------------------------------------------------------- 账户
def accounts() -> dict:
    return g.futures_accounts()


def equity() -> float:
    try:
        return float(accounts().get("total") or 0)
    except Exception:
        return 0.0


def positions() -> list:
    return [p for p in (g.request_authed("GET", "/futures/usdt/positions") or [])
            if float(p.get("size", 0) or 0) != 0]


def _positions_raw() -> tuple:
    """🔴 内部共用：取持仓并做**三态可信度判定** → `(ok, raw_list, err)`。

    `positions_strict()`（聚合成 legs，给翻仓/单仓判定用）与
    `positions_strict_rows()`（保留原始行，给 guard/watch 用）**共用本判据**，
    保证两者的「可不可信」结论永远一致 —— 不会出现「一处认为可信、另一处
    认为不可信」的口径分叉。
    """
    try:
        raw = g.request_authed("GET", "/futures/usdt/positions")
    except Exception as e:  # noqa: BLE001 —— 异常必须收敛成 ok=False，不能往上抛
        return False, [], f"持仓查询异常: {str(e)[:160]}"
    # 🔴 判据必须是 isinstance(raw, list)：{} / None / dict 一律判不可信。
    #    （旧 `positions()` 的 `or []` 正是把这里的 {} 悄悄变成 [] 的元凶）
    if not isinstance(raw, list):
        return False, [], (f"持仓返回体不是 list（type={type(raw).__name__}, "
                           f"value={str(raw)[:80]}）—— 不可当作无仓")
    return True, raw, ""


def positions_strict_rows() -> tuple:
    """🔴 三态持仓快照 —— **保留交易所原始行**，返回 `(ok, rows, err)`。

    ## 为什么要单独一个「rows」版本（别把 `legs` 硬塞给guard）

    `positions_strict()` 返回的 `legs` 只有 `{contract: {side: size}}`，
    **丢掉了 guard 真正需要的字段**。实测 `_guard()` 对每条仓行用到的字段：

    | 字段 | 用在哪 | legs 有吗 |
    |---|---|---|
    | `contract` / `size` | 定位仓腿 | ✅ 有 |
    | `entry_price` | SL 距离、保证金目标爆仓距离、峰值重建 | ❌ **没有** |
    | `mark_price` | 裸仓补挂 SL 的正确侧钳制、止盈触发判定 | ❌ **没有** |
    | `margin` | 反推真实强平距离（保证金守护的核心输入） | ❌ **没有** |
    | `leverage` | A 段保本推进的 ROI 换算 | ❌ **没有** |

    若把 `legs` 硬塞进 `_guard()`，`entry_price` 会变成 0 →
    `sl<= 0 or entry <= 0` 恒成立 → **每个仓都被当成裸仓** →
    补挂一堆保护性 SL 覆盖掉真 SL。**比原 bug 更糟。**
    故本函数保留原始行，让调用点逐字段行为**逐字不变**。

    ## 为什么要严格版（`ok=False` 时调用方必须整轮 return）

    `positions()` 走 `g.request_authed(...) or []`，而 `gate_client.request()`
    是 `json.loads(raw) if raw else {}`（gate_client.py:109）：

    | 情形 | `positions()` 的结果 | 危险度 |
    |---|---|---|
    | HTTP 4xx/5xx / 超时 / 连接失败 |抛 RuntimeError | 低（`_loop` 接住 → 整轮跳过） |
    | **HTTP 200 + 空 body** | `{}` → `or []` → **`[]`** | 🔴 **静默「无仓」** |
    | `[]`（真的空仓） | `[]` | 正常 |

    **只有「200 + 空 body」这一条窄路会走到最坏结果**（`ok=False` 时
    `_sweep_ghost_orders` 的 `held={}` → 撤光所有 SL/TP；`_reconcile_closes`
    的 `now_keys={}` → 全部 key 进 `gone` → 强删 open_meta）。这就是本函数
    存在的意义：**把那条窄路也判成「不可信」，让调用方整轮跳过。**

    三态语义：
        ok=False → 异常 / 非 list / `{}` / None ⇒ **整轮 return，不做任何动作**
        ok=True  → list（可能是 []，真的是空仓）⇒ rows 可信
    """
    ok, raw, err = _positions_raw()
    if not ok:
        return False, [], err
    rows = []
    try:
        for p in raw:
            if not isinstance(p, dict):
                continue
            if float(p.get("size") or 0) == 0:
                continue                      # 与 positions() 同：滤掉 0 仓行
            rows.append(p)
    except Exception as e:  # noqa: BLE001 —— 解析异常同样判不可信
        return False, [], f"持仓解析异常: {str(e)[:160]}"
    return True, rows, ""


def positions_strict() -> tuple:
    """🔴 三态持仓快照（H4 核心）—— 返回 `(ok, legs, err)`。

    ## 为什么必须与 `positions()` 分开（**不能沿用宽松版的语义**）

    `positions()` 走`g.request_authed(...) or []`，而
    `gate_client.request()` 的返回逻辑是 `json.loads(raw) if raw else {}`
    （gate_client.py:109）。于是**返回体为空**时：

        positions()  →  {} 或 []  →  `or []` 吞掉 → 返回 []  = **静默「无仓」**

    「查不到」被当成「没有仓」→ 翻仓流程会在错误的事实之上开出一笔反向仓。
    这正是 H4/A11 要堵的洞：**绝不基于「拉不到」推断「没有仓」**。

    失败模式核实（gate_client.py:105-114）：
      · HTTP 4xx/5xx / 超时 / 连接失败 → 抛 RuntimeError（好办，异常能捕获）
      · 返回 `{}`（空 body）           → **静默 []**，最危险的一条
      · 返回 `[]`（真的空仓）          → 正常

    ## 三态语义

        ok=False → 异常 / 超时 / 返回体不是 list / 返回 {} / None
                ⇒ 调用方**必须在任何持仓确认之前 return**，绝不开新仓
        ok=True  → 返回 list（可能是 []，真的是空仓）→ legs 可信

    ## legs 结构

        {contract: {"long": size, "short": size}}   size = 绝对张数，0 = 该腿无仓

    只含size≠0 的腿。同一 contract 同时出现两条腿 = 双向持仓模式下的
    **异常遗留双开**（S2），由调用方决定怎么处理（代码不判方向、只报事实）。
    """
    ok, raw, err = _positions_raw()
    if not ok:
        return False, {}, err
    legs: dict = {}
    try:
        for p in raw:
            if not isinstance(p, dict):
                continue
            sym = p.get("contract")
            if not sym:
                continue
            sz = float(p.get("size") or 0)
            if sz == 0:
                continue
            d = legs.setdefault(sym, {"long": 0.0, "short": 0.0})
            if sz > 0:
                d["long"] = abs(sz)
            else:
                d["short"] = abs(sz)
    except Exception as e:  # noqa: BLE001 —— 解析异常同样判不可信
        return False, {}, f"持仓解析异常: {str(e)[:160]}"
    return True, legs, ""


def order_book_top(contract: str) -> tuple:
    """最优买卖价 (bid, ask)"""
    ob = g.request("GET", "/futures/usdt/order_book",
                   query={"contract": contract, "limit": "1"}, authed=False)
    bid = float(ob["bids"][0]["p"]) if ob.get("bids") else 0.0
    ask = float(ob["asks"][0]["p"]) if ob.get("asks") else 0.0
    return bid, ask


# ---------------------------------------------------------------- 杠杆与保证金
def set_lev(contract: str, lev: int):
    if _dry():
        LOG(f"  [DRY_RUN] 设杠杆 {contract} {lev}x（跳过）")
        return
    try:
        g.request_authed("POST", f"/futures/usdt/positions/{contract}/leverage",
                         query={"leverage": str(lev)})
        LOG(f"  ⚙ {contract} 杠杆 {lev}x")
    except Exception as e:
        LOG(f"  ⚠ {contract} 设杠杆失败: {str(e)[:120]}")


def add_margin(contract: str, side: str, amount: float):
    """逐仓追加保证金：只推远爆仓价，不改变盈亏（纯安全操作）。
    ⚠️ query 参数 change 绝不能带 + 号（%2B 破坏签名 → 401）；
    双仓模式必须 dual_side=dual_long/dual_short。"""
    if _dry():
        LOG(f"  [DRY_RUN] 追加保证金 {contract} {side} +{abs(float(amount)):.4f}U（跳过）")
        return None
    q = {"change": f"{abs(float(amount)):.4f}"}
    if side == "long":
        q["dual_side"] = "dual_long"
    elif side == "short":
        q["dual_side"] = "dual_short"
    g.request_authed("POST", f"/futures/usdt/positions/{contract}/margin", query=q)
    LOG(f"  🛡 {contract} {side} 追加保证金 +{abs(float(amount)):.4f}U")


# ---------------------------------------------------------------- 基础下单
def _submit_order(contract: str, signed_size: int, price: str, tif: str,
                  reduce_only: bool, text: str) -> dict:
    req = {"contract": contract, "size": str(signed_size), "price": price,
           "tif": tif, "text": text, "reduce_only": reduce_only}
    acc = accounts()
    if acc.get("in_dual_mode") and not reduce_only:
        req["pos_margin_mode"] = "isolated"
    return g.request_authed("POST", "/futures/usdt/orders", body=req)


def market_open(contract: str, side: str, size: int, slip_ticks: int = 2,
                text: str = "t-v2-entry") -> dict:
    """IOC 市价开仓（对手价 ± 2 跳）+ 幽灵成交防护。
    返回 {"ok": bool, "id", "fill_price", "reason"}——未成交绝不记成功。"""
    if _dry():
        bid, ask = order_book_top(contract)
        px = ask if side == "long" else bid
        LOG(f"  [DRY_RUN] 开仓 {contract} {side} {size}张 @~{px:g}（不真下单）")
        return {"ok": True, "id": None, "fill_price": px, "dry": True}
    bid, ask = order_book_top(contract)
    step = float(spec(contract).get("order_price_round") or 0.1)
    px = round(ask + slip_ticks * step, 8) if side == "long" else \
        round(bid - slip_ticks * step, 8)
    if px <= 0:
        return {"ok": False, "reason": "盘口不可用"}
    signed = size if side == "long" else -size
    r = _submit_order(contract, signed, str(px), "ioc", False, text)
    oid = r.get("id")
    # 幽灵成交防护：IOC 必须查终态（字段名是 left，不是 size_left）
    # 09-30 19:15 修复部分成交 bug：只要成交量>0 即成功并返回实际成交张数，
    # 不再要求全部成交（HYPE 裸仓事故根因：8/11 部分成交被误判失败→不挂 SL/TP）
    # 10-03 01:15 三处加固（第三路审查发现）：
    #  ① 空响应 od={} 时 left 默认 0 → filled=size → 假阳性"全量成交"。
    #     安全默认必须是**保守**的"没成交"：left 取 sz。
    #  ② 3 次查询共 2.4s 预算内若成交回报延迟/查询全异常 → 循环耗尽即按未成交返回，
    #     但订单可能已成交 → 23:16 ADA 裸仓事故的上游。返回前做 positions 终检。
    #  ③ 撤单带 contract（同上）。
    def _ioq(od: dict) -> int:
        """读 IOC 成交：left 缺失/异常时保守当作"一张没成交"。"""
        try:
            if not od:
                return abs(int(size))
            v = od.get("left")
            if v is None:
                v = od.get("size_left")
            if v is None:                      # 字段全缺（空体/未就绪）→ 保守
                return abs(int(size))
            return abs(int(float(v)))
        except Exception:
            return abs(int(size))
    for _ in range(3):
        time.sleep(0.8)
        try:
            od = g.request_authed("GET", f"/futures/usdt/orders/{int(oid)}") or {}
            left = _ioq(od)
            sz = abs(int(float(od.get("size") or size)))
            filled = sz - left
            if filled > 0:
                fill = od.get("fill_price") or px
                return {"ok": True, "id": oid, "fill_price": float(fill),
                        "filled": int(filled)}
            if (od.get("finish_as") or "").lower() in ("cancelled", "canceled", "ioc"):
                break
        except Exception:
            pass
    try:
        g.request_authed("DELETE", f"/futures/usdt/orders/{int(oid)}",
                         query={"contract": contract})
    except Exception:
        pass
    # ② 终检：撤单后若该方向真有仓，说明 IOC 其实成交了（只是回报没跟上）。
    #    此时必须按成交上报，否则调用方不记仓、不挂 SL/TP → 裸仓。
    try:
        for p in positions():
            if p["contract"] == contract and \
                    (float(p["size"]) > 0) == (side == "long"):
                LOG(f"  ⚠ {contract} {side} IOC 回报未成交但持仓存在 "
                    f"{abs(float(p['size'])):g}张 → 按成交补报（防裸仓）")
                return {"ok": True, "id": oid, "fill_price": px,
                        "filled": abs(int(float(p["size"]))), "late_fill": True}
    except Exception:
        pass
    return {"ok": False, "reason": f"IOC 未成交（id={oid}），已撤残余"}


def market_close(contract: str, side: str = None,
                 text: str = "t-v2-close") -> str:
    """市价平掉该合约（可指定方向）全部持仓。

    `text` = 订单归因标签，会被 Gate 原样带回 `position_close.text`，
    `engine._reconcile_closes` 据此判 close_cause。**必须逐条区分来源**，
    否则模型主动平仓会被算进 `guard_take`（止盈线程）的胜率/盈亏比：
      · `t-v2-close`  —— 引擎平仓/兜底（默认，保持原行为）
      · `t-v2-mclose` —— 持仓管理线程的模型主动平仓（10-07 新增）
    """
    poss = [p for p in positions() if p["contract"] == contract
            and (side is None or (float(p["size"]) > 0) == (side == "long"))]
    if not poss:
        return f"{contract} 无持仓可平"
    if _dry():
        return f"[DRY_RUN] 平仓 {contract} {abs(float(poss[0]['size'])):g}张（跳过）"
    out = []
    for p in poss:
        sz = float(p["size"])
        is_long = sz > 0
        r = _submit_order(contract, int(-sz), "0", "ioc", True, text)
        out.append(f"{abs(sz):g}张@{r.get('fill_price') or '?'}")
        # 只撤本方向的条件单（10-02 用户拍板「各是各的」：趋势/高频反向同品种时，
        # 平仓不撤对方方向的 SL/TP/吊灯单；早期的无tag 全撤实现会误伤反向仓，
        # 该实现已于 10-05 随`_cancel_price_orders` 一并删除）
        #10-05 复用 cancel_price_orders_side：把「按 auto_size 精确撤」抽成
        #   同一函数，避免两处口径漂移（该函数只认 SL/TP 精确 text，吊灯单
        #   t-v2-TSL 不在内——原逻辑只看 auto_size 会撤掉吊灯单，此处已收紧，
        #   吊灯单由趋势仓自己的路径管，高频仓不再误撤）。
        cancel_price_orders_side(contract, "long" if is_long else "short")
    return f"平仓 {contract} {'/'.join(out)}"


def market_close_partial(contract: str, side: str, size: int,
                text: str = "t-v2-split") -> dict:
    """【分批止盈用】IOC 市价平掉**指定张数**，返回实际成交张数。

    为什么不用 post_only_close 做第一段：
      maker 单要排队等成交，期间 SL 条件单（size=0 = 全部剩余仓位）仍在，
      若 SL 先触发 → 整仓被平；maker 单随后成交 → 双平，账目与持仓对不上。
      第一段必须在**同一轮内确定性完成**，故用 taker IOC（立即成交、立即成交完
      剩余仓位立刻减少，SL 的 size=0 自动跟随剩余数量）。
    返回 {"ok":bool,"filled":int,"reason":str}
    """
    if size <= 0:
        return {"ok": False, "filled": 0, "reason": "size<=0"}
    if _dry():
        LOG(f"  [DRY_RUN] 部分平 {contract} {side} {size}张（跳过）")
        return {"ok": True, "filled": int(size), "dry": True}
    try:
        pos = [p for p in positions() if p["contract"] == contract
               and (side == "long") == (float(p["size"]) > 0)]
        if not pos:
            return {"ok": False, "filled": 0, "reason": "无持仓"}
        cur = int(abs(float(pos[0]["size"])))
        want = min(int(size), cur)
        if want <= 0:
            return {"ok": False, "filled": 0, "reason": "持仓已空"}
        # ⚠ 符号必须与持仓方向相反（10-04 16:0x 修）：平多仓 = 卖出 = 负数。
        #   旧代码 `want if side=="long" else -want` 把方向写反了 → 平多仓发买单、
        #   平空仓发卖单，Gate 全部拒单 REDUCE_EXCEEDED/empty position。
        #   后果：分批止盈① 自 10-03 19:0x 上线起 92 次尝试 0 次成功（全部日志可查），
        #   且 guard 每 2s 重试一次空转。参照 market_close 的 int(-sz)。
        sz = -want if side == "long" else want
        r = _submit_order(contract, sz, "0", "ioc", True, text)
        filled = int(abs(float(r.get("filled") or 0)))
        if filled <= 0:
            # IOC 报文字段缺失 → 用实际持仓差值兜底（避免"以为没平"实际平了）
            #
            # 🔴 10-05 修（QA 复核发现第 6 处漏改）：原来这里用的是**宽松版**
            #   `positions()`。空 body 时它静默返回 `[]`（`gate_client.py:109`
            #   的 `or {}` → `or []`）→ `now = 0` → `filled = cur`（**虚高**）。
            #   为什么虚高会污染账本：下游 `_split_take`（`engine.py:832`）的守卫是
            #   `if pf < 1 or pf >= now`，**只在 `now == cur` 时挡得住**；当
            #   `now > cur`（两次查询之间仓位变小）时穿透：
            #     now=10, cur=8 → 报 filled=8 → `8 >= 10` 为 False → 放行
            #     → meta 写入 tp_split_filled=8 / tp_split_left=2 / tp_split_done=1
            #     → 系统以为「平了 8 张、剩 2 张推保本」，交易所其实一股没平
            #     → **账实不符固化进 open_meta**，且该仓分批止盈被永久关闭。
            #   ⚠ 触发**不需要空 body**：正常网络抖动、或 SL/TP 在这 0.3s 内打掉
            #     仓位，都会让 `cur < now` 而穿透。空 body 只是最极端形态
            #     （`now=0` → `filled=cur`）。所以这是**风险侧**缺陷，
            #     比「撤单/强删meta」那类更安静、也更难事后发现。
            #
            #   修法：差值兜底**只在快照可信时使用**—— 「查不到」≠「持仓为 0」，
            #   这正是 `_positions_raw()` docstring 的核心不变量。ok=False 时
            #   **返回 ok=False**（让上层下轮重试），绝不产出虚高 filled。
            _ok, _rows, _perr = positions_strict_rows()
            if not _ok:
                LOG(f"  ⚠ {contract} {side} 部分平的差值兜底跳过：持仓快照不可信"
                    f"（{_perr}）——不把「查不到」当成「持仓为 0」")
                return {"ok": False, "filled": 0,
                        "px": r.get("fill_price"),
                        "reason": f"平仓差值兜底跳过：持仓快照不可信（{_perr}）"}
            after = [p for p in _rows if p["contract"] == contract
                     and (side == "long") == (float(p["size"]) > 0)]
            # ⚠ 此处 `now` 为 0 只有一种合法含义：**交易所侧确认该腿已不存在**
            #   （ok=True 才走到这里），此时 filled=cur 是正确的。
            now = int(abs(float(after[0]["size"]))) if after else 0
            filled = max(0, cur - now)   # now > cur → 0 → ok=False，不穿透
        return {"ok": filled > 0, "filled": filled,
                "px": r.get("fill_price"), "reason": "" if filled > 0 else "IOC 未成交"}
    except Exception as e:
        LOG(f"  ⚠ 部分平失败 {contract}: {str(e)[:140]}")
        return {"ok": False, "filled": 0, "reason": str(e)[:140]}


def post_only_close(contract: str, side: str, size: int,
 text: str = "t-v2-tptrail") -> dict:
    """动态止盈专用：post-only 限价平仓（maker）。

    与 market_close（taker 吃单）的区别：
      · 多单平仓=卖出，挂 **ask**；空单平仓=买入，挂 **bid** → 排队等对方吃
      · tif="poc"（post-only）：若该价会立即成交则 Gate **拒单**（不会变吃单）
      · 收益双重：① maker 费率 -0.01%（**倒返佣**，而非 taker 的 0.0486%）
                  ② 成交在盘口价，比吃单多赚一个 tick
    返回 {"ok": bool, "id":, "px":, "reason":}。
    ⚠ ok=False 时调用方**必须**兜底转市价，否则盈利单会一直挂着不落袋。
    """
    bid, ask = order_book_top(contract)
    px = ask if side == "long" else bid
    if px <= 0:
        return {"ok": False, "reason": "盘口无价"}
    if _dry():
        LOG(f"  [DRY_RUN] post-only 平仓 {contract} {side} {size}张 @{px:g}（跳过）")
        return {"ok": True, "id": None, "px": px, "dry": True}
    try:
        px = _round_px(contract, px)
    except Exception:
        pass
    signed = -abs(int(size)) if side == "long" else abs(int(size))
    try:
        r = _submit_order(contract, signed, str(px), "poc", True, text)
        return {"ok": True, "id": r.get("id"), "px": px}
    except Exception as e:
        # ORDER_POC/ORDER_POC_IMMEDIATE = 挂价会立即成交 → 平仓方向本就预期
        # 会成交（要的是立刻落袋），这类拒单对止盈**不意外**：调用方（engine E段）
        # 见到 ok=False 会走market_close 兜底，故照实回报 ok=False 但把语义标出来，
        # 避免上层把它误判成"没触发"。其余异常同样照实上报。
        s = str(e)
        reason = ("poc 被拒(会立即成交)→应由调用方转市价" if
                  ("ORDER_POC" in s or "would have taken liquidity" in s
                   or "immediately match" in s or "would be matched" in s)
                  else s)
        return {"ok": False, "reason": reason[:140], "poc_reject": ("ORDER_POC" in s)}


def _entry_px_with_anchor(contract: str, side: str, bid: float, ask: float,
                          anchor_px, anchor_tol_pct) -> tuple:
    """按「再入场锚点」选开仓挂单价（10-06 用户拍板：全由代码负责）。

    返回 (挂单价, 是否锚定挂单)。

    语义（多单为例，空单镜像）：
      · 参考价 = ask（多单真正要付的那一侧；空单用 bid）
      · 容忍上限 cap = anchor_px × (1 + tol)
      · ask ≤ cap → 现价没比自己上次卖出的更贵 → 照常挂盘口一档
      · ask >  cap → 已经更贵了 → 挂限价在 cap 等回踩，挂着不成交即由调用方
        （cancel_on_timeout）决定撤单放弃，**不追价**
    """
    tol = max(0.0, float(anchor_tol_pct or 0.0))
    default = bid if side == "long" else ask
    if not anchor_px or float(anchor_px) <= 0:
        return default, False
    try:
        a = float(anchor_px)
        if side == "long":
            cap = a * (1.0 + tol)
            worse = ask > cap
        else:
            cap = a * (1.0 - tol)
            worse = bid < cap
        if not worse:
            LOG(f"  🎯 {contract} {side} 再入场：现价未劣于锚点 {a:g}"
                f"（容忍 {tol * 100:.2f}%）→ 照常挂盘口 {default:g}")
            return default, False
        LOG(f"  🎯 {contract} {side} 再入场：现价已劣于锚点 {a:g}"
            f"（容忍 {tol * 100:.2f}%）→ 改挂限价 {cap:g} 等回踩，不成交即放弃")
        return cap, True
    except Exception:
        return default, False


def post_only_open(contract: str, side: str, size: int, timeout_s: float = 5.0,
                   text: str = "t-v2-entry", anchor_px: float = None,
                   anchor_tol_pct: float = 0.0,
                   cancel_on_timeout: bool = False) -> dict:
    """maker 开仓（10-02 12:12 用户拍板：先挂单，长时间没挂上市价来）。

    · 挂「能挂上的点位」：多单挂 **买一**、空单挂 **卖一**（盘口一档，排队即成交区）
    · tif="poc"（post-only）：该价会立即吃单时 Gate 拒单 → 说明盘口在急走，
      直接转市价兜底（保成交优先，不追价）
    · 挂 timeout_s 秒未成交 → 撤单，已成交部分保留，剩余转市价补齐
      （market_open 自带幽灵成交防护）
    · 收益：开仓 maker -0.01%（返佣）而非 taker +0.0486%，单边省 0.0586% 名义
    返回 {"ok", "id", "fill_price", "filled", "reason"}，与 market_open 消费方兼容。

    10-06 新增「再入场锚点」（用户拍板：同品种同方向连续开单，挂单锚定由代码负责）：
    · `anchor_px` = 锚点价（上次同向**盈利出场**取那次的出场价；**亏损出场**取那次开仓价）
    · 现价**已不劣于** `anchor_px × (1 ± anchor_tol_pct)` → 照常挂盘口一档（原逻辑不变）
    · 现价**已劣于**容忍带 → 改为挂限价单在容忍带边界，等价格回踩；
      配合 `cancel_on_timeout=True`，超时**撤单不成交**（`ok=False` + `no_chase=True`），
      **绝不转市价追价**。已部分成交则如实按成交记账，不做市价补齐。
    · poc 被拒（挂价会立即吃单）仍走市价兜底 —— 那种情况说明现价**优于**我们的挂价，
      成交对我们有利，不是追价。
    """
    if _dry():
        bid, ask = order_book_top(contract)
        px, anchored = _entry_px_with_anchor(contract, side, bid, ask,
                                             anchor_px, anchor_tol_pct)
        LOG(f"  [DRY_RUN] maker 开仓 {contract} {side} {size}张 @~{px:g}"
            f"{'（锚点挂单）' if anchored else ''}（不真下单）")
        return {"ok": True, "id": None, "fill_price": px, "filled": size,
                "dry": True, "anchored": anchored}
    bid, ask = order_book_top(contract)
    if bid <= 0 or ask <= 0:
        return {"ok": False, "reason": "盘口不可用"}
    px, anchored = _entry_px_with_anchor(contract, side, bid, ask,
                                         anchor_px, anchor_tol_pct)

    try:
        px = _round_px(contract, px)
    except Exception:
        pass
    signed = abs(int(size)) if side == "long" else -abs(int(size))
    try:
        r = _submit_order(contract, signed, str(px), "poc", False, text)
    except Exception as e:
        # poc 拒单分两类，必须分开处理（10-02 17:45 修）：
        #   ① ORDER_POC / ORDER_POC_IMMEDIATE = 挂价会立即吃单（盘口在急走）
        #      → **可预期的业务结果**，不是异常；应走市价兜底保成交。
        #      此前被except 当异常吞掉 → 直接 return ok=False，这笔单就这么丢了，
        #      ZEC 17:41 那笔即此情况（label=ORDER_POC_IMMEDIATE）。
        #   ② 其他（余额不足/价格精度/签名错…）= 真异常，照实上报。
        #      Gate 的 post-only 拒单原文含"如实上报"，别篡改、别吞。
        s = str(e)
        if ("ORDER_POC" in s) or ("would have taken liquidity" in s) \
                or ("immediately match" in s) or ("would be matched" in s):
            LOG(f"  ⏱ {contract} {side} poc 被拒（挂价会立即成交，盘口异动）"
                f" → 转市价兜底")
            m = market_open(contract, side, int(size), text=text)
            return dict(m, maker=False, note="poc 被拒(会立即成交) → 市价")
        return {"ok": False, "reason": f"poc 挂单异常: {s[:100]}"}
    oid = r.get("id")
    if not oid:
        # poc 被拒 = 挂价会立即成交（盘口异动），转市价保成交
        m = market_open(contract, side, int(size), text=text)
        return dict(m, maker=False, note="poc 被拒(会立即成交) → 市价")
    deadline = time.time() + max(timeout_s, 1.0)
    filled, fill_px = 0, px
    while time.time() < deadline:
        time.sleep(0.5)
        try:
            od = g.request_authed("GET", f"/futures/usdt/orders/{int(oid)}") or {}
            left = abs(int(float(od.get("left") if od.get("left") is not None
                                  else od.get("size_left") or 0)))
            sz = abs(int(float(od.get("size") or size)))
            filled = sz - left
            fill_px = float(od.get("fill_price") or px)
            if filled >= size:
                LOG(f"  🪙 {contract} maker 开仓全量成交 {filled}张 @{fill_px:g}")
                return {"ok": True, "id": oid, "fill_price": fill_px,
                        "filled": filled, "maker": True}
        except Exception:
            pass
    # 超时：撤单 → 已成交部分保留 → 剩余市价补齐
    # ⚠ 必须带 contract（10-03 01:15修复）：Gate 的 DELETE /orders/{id} 以 contract 为
    #   必需查询参数，缺它→400→异常被吞→poc 残单存活→再发市价补单=双份仓位、
    #   实际敞口超记账 1.5~2 倍。同文件 market_open 的同类撤单本就带 contract，
    #   此处漏写即遗漏证据。maker 挂 5s 超时是**常态路径**，每单都踩。
    try:
        g.request_authed("DELETE", f"/futures/usdt/orders/{int(oid)}",
                         query={"contract": contract})
    except Exception:
        pass
    time.sleep(0.3)
    try:
        od = g.request_authed("GET", f"/futures/usdt/orders/{int(oid)}") or {}
        left = abs(int(float(od.get("left") if od.get("left") is not None
                              else od.get("size_left") or 0)))
        sz = abs(int(float(od.get("size") or size)))
        filled = sz - left
        fill_px = float(od.get("fill_price") or px)
    except Exception:
        pass
    if filled >= size:
        return {"ok": True, "id": oid, "fill_price": fill_px,
                "filled": filled, "maker": True}
    remain = int(size - filled)
    if remain <= 0:
        return {"ok": True, "id": oid, "fill_price": fill_px,
                "filled": filled, "maker": True}
    # 10-06：锚点挂单未成交 → 按配置撤单放弃（**不转市价追价**）。
    #   这正是用户要的语义：挂到上次盈利出场价/开仓价附近，挂不到就取消。
    #   已部分成交的照实保留、不补齐 —— 补一笔就等于在更差的价位再买一次。
    if anchored and cancel_on_timeout:
        if filled > 0:
            LOG(f"  🎯 {contract} {side} 锚点挂单部分成交 {filled}/{size}张 "
                f"@{fill_px:g} → 保留该部分，不补齐")
            return {"ok": True, "id": oid, "fill_price": fill_px,
                    "filled": filled, "maker": True, "partial": True}
        LOG(f"  🚫 {contract} {side} 锚点挂单 {timeout_s:.0f}s 未成交 → "
            f"撤单放弃（不转市价，避免追价）")
        return {"ok": False, "id": oid, "no_chase": True, "filled": 0,
                "reason": f"再入场挂单超时未成交（锚点 {anchor_px:g}），按配置放弃不追价"}
    m = market_open(contract, side, remain, text=text)
    if not m.get("ok"):
        if filled > 0:                     # 有部分成交就不算失败，按实际记账
            return {"ok": True, "id": oid, "fill_price": fill_px,
                    "filled": filled, "partial": True}
        return {"ok": False, "reason": f"maker 超时且市价兜底失败: {m.get('reason')}"}
    LOG(f"  ⏱ {contract} maker 开仓挂 {timeout_s:.0f}s 未成交 → "
        f"市价补 {remain}张（已成交 {filled}）")
    return {"ok": True, "id": m.get("id") or oid,
            "fill_price": m.get("fill_price") or fill_px,
            "filled": int(filled) + int(m.get("filled") or remain),
            "maker": False,
            "maker_filled": int(filled),                    # maker 成交部分（返佣费率）
            "taker_filled": int(m.get("filled") or remain)}  # 市价补部分（taker 费率）


# ---------------------------------------------------------------- 条件单
def list_price_orders(contract: str = None) -> list:
    try:
        q = {"status": "open"}
        if contract:
            q["contract"] = contract
        return g.request_authed("GET", "/futures/usdt/price_orders", query=q) or []
    except Exception:
        return []


def cancel_price_order(oid, contract: str = None):
    try:
        g.request_authed("DELETE", f"/futures/usdt/price_orders/{oid}",
                         query={"contract": contract} if contract else None)
    except Exception:
        pass


# 10-05 删除 `_cancel_price_orders(contract, tag_filter)`：
#   零调用点，且它是 B3 缺陷的**原始实现**——只按 text 匹配、不看方向，在双向
#   持仓模式下会把另一方向（新腿）的 SL 一起撤掉。所有调用点已改用下面的
#   `cancel_price_orders_side`（按 auto_size 精确撤 + text 精确等值）。
#   保留这条注释是为了留住教训，不是为了复活它。


def cancel_price_orders_side(contract: str, side: str, tags=None) -> int:
    """🔴 **方向精确**撤条件单（B4/B9/C9 核心原语），返回撤掉张数。

    ## 为什么不能只按 text 撤单

    早期有个 `_cancel_price_orders(contract, tag_filter)` 实现**只按 text 匹配、
    完全不看方向**（该函数已于 10-05 删除 —— 它是 B3 缺陷的原始实现，留着是地雷）。
    而 Gate 双向持仓模式下同一个
    contract 的两腿 SL 单 text **完全相同**（都叫 `t-v2-SL`），只有
    `initial.auto_size` 才是 `close_long` / `close_short` 的区别。
    翻仓窗口期内同contract 上同时挂着新旧两腿的 SL，只按 text 撤 →
    **把另一方向的那张也撤掉** → 新腿裸奔（本次最高危点 B3）。

    本函数按 `auto_size` 精确锁定方向 + text 精确等值（`t-v2-SL` 不会命中
    `t-v2-TSL` 吊灯单），结构上杜绝误伤。

    tags  : 要撤的 text 白名单，默认 `("t-v2-SL", "t-v2-TP")`（SL+TP 全撤，
            翻仓 F3/平仓收尾用这个）。传 `("t-v2-SL",)` 则**只撤 SL**，
            保留 TP —— 「按均价重挂 SL、TP 不动」的场景必须用这个
            （`_try_pyramid_add` / guard A 段保本推进）。

    side=None 时**不做任何事**并返回 0（不猜方向；`auto_size` 判不出方向的
    单不属于本函数职责）。
    """
    if side not in ("long", "short"):
        return 0
    auto = "close_long" if side == "long" else "close_short"
    want_text = tuple(tags) if tags else ("t-v2-SL", "t-v2-TP")
    n = 0
    for o in list_price_orders(contract):
        if not isinstance(o, dict):      # 防御：返回体异常时跳过，绝不整轮崩
            continue
        ini = o.get("initial") or {}
        if not isinstance(ini, dict):
            continue
        if (ini.get("auto_size") or "") != auto:
            continue                      # 另一方向：不碰
        if (ini.get("text") or "") not in want_text:
            continue                      # 吊灯单/其它标记：不碰
        cancel_price_order(o.get("id"), contract)
        n += 1
    return n


def place_tpsl(contract: str, sl_px=None, tp_px=None, is_long: bool = True,
               expiration: int = 86400 * 7, sl_text: str = "t-v2-SL") -> tuple:
    """挂止盈止损条件单（Gate price_orders）。返回 (sl_id, tp_id)。
    rule 实测语义：1=上涨触发、2=下跌触发。SL 市价平、TP 限价平（maker 省费）。
    ⚠ sl_text：SL 单的 text 标记，默认 t-v2-SL（高频）。趋势吊灯单必须传 t-v2-TSL
      —— 撤单靠 text 精确匹配，两类单混用会导致吊灯旧单撤不掉（10-02 修复）。"""
    if not sl_px and not tp_px:
        return (None, None)
    if _dry():
        LOG(f"  [DRY_RUN] 条件单 SL={sl_px} TP={tp_px}（不真挂）")
        return (None, None)
    stop_rule = 2 if is_long else 1
    take_rule = 1 if is_long else 2
    auto = "close_long" if is_long else "close_short"

    def _mk(px, rule, tag, limit=None):
        take = tag == "TP" and limit
        return {
            "initial": {"contract": contract, "size": 0,
                        "price": str(round(limit, 8)) if take else "0",
                        "tif": "gtc" if take else "ioc",
                        "reduce_only": True, "auto_size": auto,
                        "text": sl_text if tag == "SL" else "t-v2-TP"},
            "trigger": {"strategy_type": 0, "price_type": 0, "price": str(px),
                        "rule": rule, "expiration": expiration},
        }

    if sl_px:
        sl_px = _round_px(contract, sl_px, -1 if is_long else 1)
    if tp_px:
        tp_px = _round_px(contract, tp_px)
    out = []
    for px, rule, tag in ((sl_px, stop_rule, "SL"), (tp_px, take_rule, "TP")):
        if not px:
            out.append(None)
            continue
        try:
            r = g.request_authed("POST", "/futures/usdt/price_orders", body=_mk(px, rule, tag, px))
            out.append(r.get("id"))
            LOG(f"  ✅ [{tag}] {contract} 条件单 id={r.get('id')} 触发={px:g}")
        except Exception as e:
            out.append(None)
            LOG(f"  ❌ [{tag}] {contract} 挂条件单失败: {str(e)[:160]}")
    return tuple(out)


def has_price_order(contract: str, tag: str, side: str = None) -> bool:
    """该合约是否存在指定类型（SL/TP/TSL）的条件单。

    ⚠ 10-03 01:15 三处加固（第三路审查发现，原实现是致命子串匹配）：
    原：`tag in text` —— **"SL" 是 "t-v2-TSL"（趋势吊灯单）的子串**，趋势仓挂了
    吊灯单就会让"高频仓有没有 SL"的判断恒真 → 裸仓巡检（engine 裸仓巡检段）/
    孤儿仓兜底 / TP 补挂三处全部跳过补挂 → **该方向永久裸奔**。
    另：双仓模式下同合约可多空并存，两腿的 text 完全相同（都叫 t-v2-SL），
    只按合约判断会让**反向仓的 SL 挡住本方向**。

    现：① 按 auto_size（close_long/close_short）判方向，side=None 时不判；
       ② text 用 `f"t-v2-{tag}"` 精确等值匹配（t-v2-SL 不会命中 t-v2-TSL）。
    """
    want = f"t-v2-{tag}"
    for o in list_price_orders(contract):
        ini = o.get("initial") or {}
        if (ini.get("text") or "") != want:      # 精确等值，非子串
            continue
        if side is None:
            return True
        auto = ini.get("auto_size") or ""
        if (side == "long" and auto == "close_long") or \
           (side == "short" and auto == "close_short"):
            return True
    return False


def cancel_open_orders(contract: str):
    """撤掉该合约全部普通挂单（条件单触发后落成的限价单等），防 reduce_only 残单空挂"""
    try:
        g.request_authed("DELETE", "/futures/usdt/orders",
                         query={"contract": contract, "status": "open"})
    except Exception:
        pass


# ---------------------------------------------------------------- 状态键
# （趋势仓策略 10-03 已移除：原 trend_keys()/_rk() 随之删除）
