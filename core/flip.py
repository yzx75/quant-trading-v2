# -*- coding: utf-8 -*-
"""
core/flip.py — 翻仓事务状态（一个品种一个仓位 + 反向翻仓）

背景（2026-10-05 实盘事故）：Gate 账户是**双向持仓模式**
（`dual_side=dual_long/dual_short`），同contract 的多仓与空仓可以并存。
`_try_open()` 过去只用 `contract|side` 做 key，反向的那条腿根本不进判断 →
直接开出一笔反向仓 → SNDK / HYPE 双开，9 个名额里 4 个被对冲占掉、
净暴露归零却付双份手续费。

本模块只管**事务状态**，不下单、不判方向：
方向 100% 由模型给出，代码负责把「同 contract 已存在的反向腿」收敛掉
（先开新 →挂新保护 → 平旧 → 落账），失败则整体回滚。

## 铁律（三条，违反即作废）

1. **不落盘**：`_TXN` / `_FROZEN` / `_CAUSE` 全是纯内存。落盘会引入
   「事务中途进程崩溃 → 半条事务记录」的恢复问题；内存态崩溃即消失 =
   自动退化成「无事务」，与 PRD G 组口径天然一致，不需要额外恢复代码。
   ⚠ 因此**不要**把它塞进 `hf_state.json` 的 `open_meta` ——
   `state.save` 只保证单次写原子、「读-改-写」不原子（MEMORY 明载），
   把「要求全局唯一、必须立刻对 guard 可见」的状态塞进去等于再加一个丢失维度。

2. **锁序唯一**：`flip._TXN_LOCK` **绝不跨网络 IO 持有**。翻仓要发 3~5 次
   交易所请求（约 5~15s），这些必须在锁外做。因此它与 engine 的
   `_META_LOCK` 无嵌套关系 → 不存在死锁可能（这是本设计最重要的侵入性约束）。

3. **冻结粒度 = contract**：`_FROZEN[sym]` 只影响 `sym`，其它品种零影响。
   读点是纯 dict 查找（`_FROZEN.get(sym, 0) > time.monotonic()`），
   无锁、无 IO、无 JSON 序列化 —— guard 2s 轮询每仓多约 50ns。

## 三个读点

    watcher(2s)  —— 只读 _FROZEN —— 不动（它不碰条件单、不碰 SL）
    hf(60s)     —— 唯一写 _TXN —— 事务全程串行，同 contract 天然不重入
    guard(2s)   —— 只读 _FROZEN —— 冻结期跳过全部**退出类**动作
    review      —— 与事务无关 —— 只在 close 落库后被动触发
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from core import log as core_log

LOG = core_log.log

# ---------------------------------------------------------------- 常量
# phase 只写 journal，**不做python 枚举**（避免跨模块 import 环）。
PH_BEGIN = "begin"
PH_OPEN_NEW = "open_new"            # F1 开新仓中
PH_PROTECT_NEW = "protect_new"      # F2 挂新保护单中
PH_CLOSE_OLD = "close_old"          # F3 平旧腿中
PH_DONE = "done"                    # F4 落账完成（回到 S1）
PH_ROLLBACK = "rollback"            # F3 失败 → 回滚平新腿中
PH_ROLLBACK_DONE = "rollback_done"  # 回滚成功（回到 S1，**不自动重试**）
PH_ROLLBACK_FAILED = "rollback_failed"   # 回滚失败 → S5（只报警不动手）

# close_cause 枚举（唯一真源，engine.py / agents/review.py 共享字面量）。
# ⚠ 读取一律用 `.get()`，禁止 `[]` —— 老记录没有这个键。
CAUSE_SL_TP = "sl_tp"              # Gate 侧 ao- 条件单触发
CAUSE_FLIP_OUT = "flip_out"        # 翻仓 F3 平旧腿（含窗口期被 SL 打掉）
CAUSE_FLIP_ROLLBACK = "flip_rollback"   # 回滚平新腿
CAUSE_GUARD_TAKE = "guard_take"    # 引擎侧主动平仓（10-09 后仅剩 D 段 TP 兜底）
CAUSE_MANUAL = "manual"            # App/网页手动平仓
CAUSE_RISK_GUARD = "risk_guard"    # 强平/爆仓
CAUSE_MODEL_CLOSE = "model_close"  # 持仓管理线程模型主动平仓（10-07 新增）
# ⚠ 10-09 用户令删除全部机械护盈段（A 保本 / B 阶梯 / C 连续跟随 / E 动态止盈
#   + 保本兜底 / 分批止盈 / 时间止损）→ `CAUSE_TIME_STOP = "time_stop"` 已随
#   时间止损段一并删除。历史 trades.json 里仍可能存有该字面量，读取一律用
#   `.get()` 兼容，不要把它当成"代码里还有这条路"。
#⚠ CAUSE_RISK_GUARD **先定义但当前不产出**：实测 500 笔交易的 reason 里
#   没有一笔能识别为强平，不要凭空写识别规则（架构 Q-A 选项 A）。

# 每品种状态机（看板展示用）
ST_S0 = "S0"        # 无仓
ST_S1 = "S1"        # 单仓（稳态）
ST_S2 = "S2"        # 遗留双开（异常遗留态，只告警不自动动）
ST_S3 = "S3"        # 翻仓中
ST_S4 = "S4"        # 回滚中
ST_S5 = "S5"        # 回滚失败（双开残留，只报警）

# 🔴 全局「冻结闸」开关（对应 config.yaml 的 `flip.guard_freeze`）。
#   由 `engine.Engine.__init__` 从配置回填。设false 时 `is_frozen()` 恒返回
#   False —— guard 冻结闸与 hf 侧的三处冻结判定**同时**失效（口径统一，
#   不会出现「一边冻一边不冻」的半吊子状态）。
#   ⚠ 这是**并发控制**的总闸，不是方向判断：关掉它不会让代码多判方向，
#       只会让翻仓窗口期重新暴露 B3 风险。仅供紧急排障用。
#   🔴 QA 提出的安全改进：关掉时**必须打🚨 告警**。原因是这个开关失效时
#   症状为「新腿裸奔且日志无痕迹」—— guard 照常撤单/推进、hf 照常重入，
#   没有任何一行日志会说「本该冻结而没冻」。有了这条告警，事后能从日志
#   确认「当时闸是关着的」，而不是只能靠推测。
_FREEZE_ENABLED = True


def set_freeze_enabled(flag: bool) -> None:
    """由 engine 启动时按 `flip.guard_freeze` 回填一次。

    🔴 关闭时打🚨 告警（QA 要求）：冻结闸是 B3 的**唯一**防线，它全局失效
    的症状是「翻仓窗口期内 guard 撤掉新腿 SL → 新腿裸奔」，而这个症状在
    其它地方**不留任何痕迹**（guard 照常输出、hf 照常重入）。所以关闸动作
    本身必须留痕。开启时也打一条（确认生效，避免「以为开着其实没开」）。
    """
    global _FREEZE_ENABLED
    flag = bool(flag)
    if flag == _FREEZE_ENABLED:
        # 幂等：engine 只在启动时调一次；重复同值不刷屏。
        _FREEZE_ENABLED = flag
        return
    _FREEZE_ENABLED = flag
    if flag:
        LOG("  ✅ 翻仓冻结闸已开启（guard 将在翻仓/回滚窗口期跳过退出类动作）")
    else:
        LOG("  🚨🚨🚨 翻仓冻结闸已**关闭**（flip.guard_freeze=false）—— "
            "翻仓/回滚窗口期内 guard 将照常撤单与推进 SL，**新腿可能被误撤保护单"
            "而裸奔（B3 全面失守）**。仅供紧急排障，确认无翻仓后再考虑恢复。")


_ST_LABEL = {
    ST_S0: "无仓", ST_S1: "单仓", ST_S2: "遗留双开",
    ST_S3: "翻仓中", ST_S4: "回滚中", ST_S5: "回滚失败",
}

DEFAULT_TIMEOUT_S = 30.0     # 窗口期上限（PRD Q5：30s，留约 3 倍余量）
DEFAULT_CAUSE_TTL_S = 120.0  # _CAUSE 存活期（过期由 sweep_expired 清）
S5_FREEZE_S = 3600.0         # 回滚失败（S5）后的冻结时长（Q-D 选项 A：1 小时）

_AUDIT_MAX = 50              # 双开审计最多留几条（内存，滚动淘汰）


# ---------------------------------------------------------------- 状态容器
_TXN_LOCK = threading.RLock()          # 只保护下面三份字典的增删，µs 级
_TXN: dict = {}                        # contract -> FlipTxn（进行中的事务）
_FROZEN: dict = {}                     # contract -> monotonic 冻结到期时刻（**无锁**读）
_CAUSE: dict = {}                      # "sym|side" -> {"cause","ts"}
_AUDIT: list = []                      # 双开审计记录（滚动）
_CALLS: dict = {"suppressed": 0, "flip": 0, "rollback": 0,
                "rollback_failed": 0, "timeout": 0}


@dataclass
class FlipTxn:
    """一次翻仓事务的上下文（纯内存，进程退出即消失）。"""

    contract: str                # "SNDK_USDT"
    new_side: str                # 模型给的方向 D（long|short）
    old_side: str                # 反向腿 −D
    old_size: int                # F1 前查到的旧腿实际张数
    phase: str = PH_BEGIN        # 见上方 PH_* 常量
    t0: float = field(default_factory=time.monotonic)
    deadline: float = 0.0        # t0 + timeout_s；超期 → 🚨 + 不自动平任何一边
    legs_at_begin: dict = field(default_factory=dict)   # 开跑时两腿快照，事后对账
    last_error: str = ""         # 拒单原文（**不篡改**）
    retry_used: int = 0          # F3 已重试次数
    suppressed: int = 0          # 冻结期拦下的退出类动作数（A5 可观测）
    new_size: int = 0            # F1 实际成交张数（回滚要用）

    def overdue(self) -> bool:
        """窗口期是否已超期（超期后只告警，不自动平任何一边）。"""
        return bool(self.deadline) and time.monotonic() > self.deadline

    def left_s(self) -> float:
        """窗口期剩余秒数（看板显示用，最小 0）。"""
        return max(0.0, (self.deadline or 0.0) - time.monotonic())

    def elapsed_s(self) -> float:
        return max(0.0, time.monotonic() - self.t0)

    def to_dict(self) -> dict:
        return {"contract": self.contract, "new_side": self.new_side,
                "old_side": self.old_side, "old_size": self.old_size,
                "new_size": self.new_size, "phase": self.phase,
                "elapsed": round(self.elapsed_s(), 1),
                "left_s": round(self.left_s(), 1),
                "retry_used": self.retry_used,
                "suppressed": self.suppressed,
                "last_error": self.last_error[:200],
                "legs_at_begin": self.legs_at_begin}


# ---------------------------------------------------------------- 事务
def begin(contract: str, new_side: str, old_side: str, old_size: int,
          timeout_s: float = DEFAULT_TIMEOUT_S,
          legs_at_begin: dict = None) -> FlipTxn:
    """建事务 + **立即写冻结**。

    🔴 调用纪律（架构T03 必查点 1）：本函数必须放在 `_flip()` 的**第一行**、
    **早于任何 `ex.*` 网络调用**。否则 F1 的`post_only_open` 那5s 挂单窗口里
    guard 就可能撤掉新腿刚挂的 SL（最高危点 B3）。

    冻结用 `time.monotonic()`（单调钟）：不受系统时间调整/NTP 校时影响。
    """
    with _TXN_LOCK:
        # 理论上到不了这里（`_try_open` 入口已用 is_frozen 拦过），
        # 兜底丢弃同名旧事务，避免脏上下文被复用。
        _TXN.pop(contract, None)
        t0 = time.monotonic()
        to = float(timeout_s or DEFAULT_TIMEOUT_S)
        txn = FlipTxn(contract=contract, new_side=new_side, old_side=old_side,
                      old_size=int(old_size or 0), phase=PH_BEGIN, t0=t0,
                      deadline=t0 + to, legs_at_begin=dict(legs_at_begin or {}))
        _TXN[contract] = txn
        _FROZEN[contract] = t0 + to# 无锁写：单写者（hf 线程），dict 赋值是原子操作
        _CALLS["flip"] += 1
        return txn


def get(contract: str):
    """取进行中的事务（无锁读，只用于日志/快照；判定请用 is_frozen）。"""
    return _TXN.get(contract)


def set_phase(contract: str, phase: str, error: str = "") -> bool:
    """迁移 phase。error 非空时一并记进 `last_error`（**不篡改拒单原文**）。"""
    with _TXN_LOCK:
        txn = _TXN.get(contract)
        if txn is None:
            return False
        txn.phase = phase
        if error:
            txn.last_error = str(error)[:400]
        return True


def note_error(contract: str, error: str) -> None:
    """记一条错误原文到事务（不抛异常，调用点不必再套try）。"""
    with _TXN_LOCK:
        txn = _TXN.get(contract)
        if txn is not None and error:
            txn.last_error = str(error)[:400]


def bump_retry(contract: str) -> int:
    with _TXN_LOCK:
        txn = _TXN.get(contract)
        if txn is None:
            return 0
        txn.retry_used += 1
        return txn.retry_used


def set_new_size(contract: str, size: int) -> None:
    """记 F1 实际成交张数（回滚平新腿要用）。"""
    with _TXN_LOCK:
        txn = _TXN.get(contract)
        if txn is not None:
            txn.new_size = int(size or 0)


# ---------------------------------------------------------------- 冻结
def freeze(contract: str, seconds: float) -> None:
    """手动冻结某 contract（回滚失败 S5 用长冷却）。"""
    _FROZEN[contract] = time.monotonic() + max(1.0, float(seconds))


def is_frozen(contract: str) -> bool:
    """**热路径**读点：无锁、无 IO、无序列化（guard 2s 每仓约 50ns）。"""
    if not _FREEZE_ENABLED:
        return False
    return time.monotonic() < _FROZEN.get(contract, 0.0)


def freeze_left(contract: str) -> float:
    """冻结剩余秒数（看板/日志用；未冻结或全局闸关闭返回 0.0）。"""
    if not _FREEZE_ENABLED:
        return 0.0
    return max(0.0, _FROZEN.get(contract, 0.0) - time.monotonic())


def thaw(contract: str) -> None:
    """解冻（当前无内部调用方；保留给测试与人工介入）。"""
    _FROZEN.pop(contract, None)


def note_suppressed(contract: str, n: int = 1) -> None:
    """记一次「冻结期拦下的退出类动作」（A5 可观测口径）。

    通过线不是"这个数=0"，而是"窗口期内在冻结闸处被拦下的动作都记在这里，
    而 `cancel_price_orders_side` / `cancel_price_order` / `market_close_partial`
    对该contract 的调用数恒为 0"。本计数器为 0 只说明本轮没碰上需要拦的动作。
    """
    with _TXN_LOCK:
        txn = _TXN.get(contract)
        if txn is not None:
            txn.suppressed += int(n)
        _CALLS["suppressed"] += int(n)


# ---------------------------------------------------------------- 平仓成因登记
def _ck(contract: str, side: str) -> str:
    return f"{contract}|{side}"


def expect_close(contract: str, side: str, cause: str) -> None:
    """登记「我要求平掉这一腿」的成因，供 `_reconcile_closes` 认领。

    ⚠ 必须在**平仓动作之前**登记（否则 reconcile 可能先于登记发现腿消失，
    就会按 text 推导出 `sl_tp` 而不是 `flip_out`）。
    翻仓事务在 `begin()` 之后立刻登记旧腿成因 —— 这样连「F1~F3 之间旧腿被
    SL 打掉」（PRD B5）也能正确记成 `flip_out`。
    """
    with _TXN_LOCK:
        _CAUSE[_ck(contract, side)] = {"cause": str(cause), "ts": time.time()}


def take_close_cause(contract: str, side: str):
    """认领并删除一条成因。**必须返回 None 而非抛异常** —— 它由另一个线程
    （watcher 的 `_reconcile_closes`）消费，任何异常都会打断对账循环。"""
    with _TXN_LOCK:
        return (_CAUSE.pop(_ck(contract, side), None) or {}).get("cause")


def peek_close_cause(contract: str, side: str):
    """只看不取（诊断/测试用）。"""
    return (_CAUSE.get(_ck(contract, side)) or {}).get("cause")


def clear_close_cause(contract: str, side: str) -> None:
    """丢弃一条登记（事务失败、不再需要该成因时用）。"""
    with _TXN_LOCK:
        _CAUSE.pop(_ck(contract, side), None)


def sweep_expired(ttl_s: float = DEFAULT_CAUSE_TTL_S) -> int:
    """清过期的 `_CAUSE`（由 guard heavy 轮顺带调，**不新起线程**）。返回清理条数。

    🔴 QA 抓到的 P2：原实现是 `ttl_s: float = None` + 函数体内
    `ttl = float(ttl_s if ttl_s is not None else _CAUSE_TTL_S)`，而模块里
    定义的常量实名 **`DEFAULT_CAUSE_TTL_S`**，`_CAUSE_TTL_S` **从未定义**
    → 走默认路径（无参调用）必抛 `NameError`。
    之所以线上没炸：`engine._guard` 的调用显式传了参，且外层还套了
    try/except —— 但这意味着「签名声明的默认用法」根本不可用，一旦有人
    按常规写法调它，`_CAUSE` 清理会静默失效并堆内存。
    修法：把模块常量直接做**形参默认值**（常量在第 95 行、定义早于本函数，
    定义顺序可行），并保留「显式传 None 退回默认」的容错。
    """
    ttl = float(DEFAULT_CAUSE_TTL_S if ttl_s is None else ttl_s)
    now = time.time()
    n = 0
    with _TXN_LOCK:
        for k in [k for k, v in _CAUSE.items()
                  if (now - float((v or {}).get("ts") or 0)) > ttl]:
            _CAUSE.pop(k, None)
            n += 1
        for k in [k for k in _TXN if time.monotonic() > (_TXN[k].deadline or 0) + 900]:
            _TXN.pop(k, None)
            n += 1
    return n


# ---------------------------------------------------------------- 结束
def end(contract: str) -> None:
    """删事务 + 删冻结（正常终态：done / rollback_done / 未开始就中止）。"""
    with _TXN_LOCK:
        _TXN.pop(contract, None)
    _FROZEN.pop(contract, None)     # 无锁 pop：与 begin 的写入同为单写者语义


def keep_failed(contract: str, freeze_s: float = S5_FREEZE_S) -> None:
    """回滚失败（S5）：**保留**事务上下文 + 置长冻结，供 `/api/state` 暴露。

    为什么不 `end()`：S5 必须能被看板看到（架构 D2）。冻结 1 小时而不是
    无限期（Q-D 选项 A）——无限期会让该contract 彻底失去 SL 跟随/动态止盈，
    两条腿裸奔更危险。
    """
    with _TXN_LOCK:
        txn = _TXN.get(contract)
        if txn is not None:
            txn.phase = PH_ROLLBACK_FAILED
        _CALLS["rollback_failed"] += 1
    _FROZEN[contract] = time.monotonic() + max(1.0, float(freeze_s))


def count_rollback() -> None:
    with _TXN_LOCK:
        _CALLS["rollback"] += 1


def count_timeout() -> None:
    with _TXN_LOCK:
        _CALLS["timeout"] += 1


# ---------------------------------------------------------------- 双开审计
def audit_dual(contract: str, legs: dict, meta_keys=None, source: str = "",
               note: str = "") -> dict:
    """登记一条**双开审计**（S2 遗留双开 / 启动审计 / 翻仓中断指纹）。

    用户 10-05 拍板：存量双开（S2）**不拒开新仓**，只放行 + 告警 + 审计，
    由用户自己尽快手动清理多余的腿。故本函数**只记录、绝不动仓**。
    """
    rec = {"t": time.time(),
           "ts": time.strftime("%m-%d %H:%M:%S"),
           "contract": contract,
           "legs": {k: float(v) for k, v in (legs or {}).items() if v},
           "meta_keys": sorted(meta_keys or ()),
           "source": source or "",
           "note": note or ""}
    with _TXN_LOCK:
        _AUDIT.append(rec)
        if len(_AUDIT) > _AUDIT_MAX:
            del _AUDIT[:len(_AUDIT) - _AUDIT_MAX]
    return rec


def audit_records() -> list:
    with _TXN_LOCK:
        return list(_AUDIT)


# ---------------------------------------------------------------- 快照
def _state_of(contract: str, legs: dict, txn) -> str:
    """由「交易所有几���腿」+「事务 phase」推出每品种状态（看板展示用）。"""
    if txn is not None:
        if txn.phase == PH_ROLLBACK_FAILED:
            return ST_S5
        if txn.phase == PH_ROLLBACK:
            return ST_S4
        if txn.phase in (PH_BEGIN, PH_OPEN_NEW, PH_PROTECT_NEW, PH_CLOSE_OLD):
            return ST_S3
    lg = legs.get(contract) or {}
    n = sum(1 for k in ("long", "short") if float(lg.get(k) or 0) > 0)
    if n >= 2:
        return ST_S2
    return ST_S1 if n == 1 else ST_S0


def _contract_of(key: str) -> str:
    """把 `open_meta` 的 `"CONTRACT|side"` 键归一化成 `"CONTRACT"`。

    🔴 QA 抓到的 P1 根因：`snapshot()` 的三个数据源**键形态不同**——
      · `legs`  的键是 `"CONTRACT"`（`engine._watch` 从 `ex.positions()` 现算）
      · `meta_keys` 的键是 `"CONTRACT|side"`（`open_meta` 的原始键，直接透传）
      · `txns`  的键是 `"CONTRACT"`（`_TXN` 由 `begin(contract=...)` 写入）
    直接 `set(legs) | set(keys)` 会把 `ADA_USDT|long` 当成一个**独立 contract**
    混进states → 看板每个持仓品种多出 1~2 个假 S0 行（实盘 9 仓 → 面板约
    14~18 行，一半是垃圾），且「S0 无仓」语义被污染成误导。
    第二重失配：`k.split("|")[0] == c` 对 `c="ADA_USDT|long"` 恒为 False，
    于是这些脏行的 `meta_keys` 恒为 `[]`（自相矛盾的数据）。
    """
    return str(key).split("|")[0].strip()


def snapshot(legs: dict = None, meta_keys=None) -> dict:
    """给 `/api/state` + `live.json` 的**只读**快照（纯内存拼装，µs 级）。

    legs      : {contract: {"long": size, "short": size}}，由 engine._watch 从
                `ex.positions()` 现算；None = 只给事务/冻结部分。
    meta_keys : `open_meta` 的 key 列表（`"CONTRACT|side"` 形态）。
                ⚠ 内部会经 `_contract_of()` **归一化成 contract 形态**再参与
                并集 —— 详见该函数 docstring 的 P1 说明。
    """
    legs = legs or {}
    keys = list(meta_keys or ())
    with _TXN_LOCK:
        txns = {k: v.to_dict() for k, v in _TXN.items()}
        tx_objs = dict(_TXN)
    # 🔴 P1 修复（QA 抓出）：三个数据源先归一化成 contract 形态再求并集。
    #   修复前：`set(legs) | set(keys)` 会混进 "ADA_USDT|long" 这类脏键
    #   → 看板每个持仓品种多出假 S0 行、"S0 无仓"语义被污染。
    #   修复后："XRP_USDT"（有 open_meta 但交易所无仓 = 幽灵态）会正确
    #   归入 **S0 单行**（该 info 本来就要展示：引擎有记录但没仓 = 幽灵）。
    contracts = sorted({_contract_of(k) for k in keys}
                       | {_contract_of(k) for k in legs}
                       | {_contract_of(k) for k in txns})
    contracts = [c for c in contracts if c]
    states = {}
    for c in contracts:
        stt = _state_of(c, legs, tx_objs.get(c))
        states[c] = {"state": stt, "label": _ST_LABEL.get(stt, stt),
                     "legs": {k: float(v) for k, v in (legs.get(c) or {}).items()
                              if float(v or 0) > 0},
                     "meta_keys": sorted(k for k in keys
                                         if _contract_of(k) == c),
                     "frozen": is_frozen(c),
                     "freeze_left_s": round(freeze_left(c), 1)}
    frozen = {k: round(freeze_left(k), 1) for k in _FROZEN if is_frozen(k)}
    return {"ts": time.time(),
            "txn": txns,
            "frozen": frozen,
            "states": states,
            "dual_audit": audit_records()[-_AUDIT_MAX:],
            "counters": dict(_CALLS),
            "timeout_s": DEFAULT_TIMEOUT_S}
