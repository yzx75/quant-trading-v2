# -*- coding: utf-8 -*-
"""
agents/decision.py — 高频决策者（15 秒一轮）

职责边界（铁律）：
  模型 = 决策者：只输出 品种+方向(+win_rate/regime/bias/reason 状态字段)；
        提示词明令禁止输出 price/size/sl/tp/lever。
  代码 = 护栏+执行：布林候选标注 → brief → 模型决策 → 风控生成全部数字 → 执行。
平仓**不在本模块**（10-07 用户令）：已持仓的单子由 `agents/exit_agent.py`
（engine 的 exit_loop 线程）只看持仓品种单独调模型判 hold/close。
本模块只负责开仓（open/hold）；`cycle()` 里收到 close 只记一行日志、不执行。
（历史遗留：`_try_close()` 是 09-30 那版「模型说平就平」的实现，现无调用方。）
"""
from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from agents import llm
from core import journal
from core import flip
from core import log as core_log
from core import state
from core.indicators import macd, rsi
from exchange import executor as ex
from market import feeder
from risk.manager import RiskManager
from strategy import regime

LOG = core_log.log

# ---- 1H 动能参数（10-04 决策层升级）----
# 依据：用户 10-03 回测实证「顺 1H 动能胜率 69% vs 逆 46.5%，z=2.89, p=0.0038，
# 240 笔」；同批回测里 4H 方向无效，最优窗口 = 近 4 根 1H。
# 改造前scan() 只喂 15m标注 → **模型物理上看不到 1H 动能**，本次补上这个缺口。
MOM_1H_TF_MIN = 60                # feeder._TF_MAP 已支持 60 → "1h"
MOM_1H_WIN = 4                   # 回测实证最优窗口：近 4 根 1H
MOM_1H_BARS = 30                 # 拉 30 根 1H（窗口 4 够用，留冗余抗断档）
MOM_1H_RETRY_S = 60              # 拉取失败后的重试冷却（别每轮为同一失败品种打3 次重试 HTTP）
SCAN_FETCH_WORKERS = 8# scan() 阶段1 并发拉数的线程数。
                                  # 15 品种 ×3 次 HTTP（15m K线/1H K线/现价）= 45 次请求，
                                  # 串行实测 9~15s；8 并发按 IO 等待占比算≈2~3s。
                                  # 不设更高是怕撞 Gate 的限频（429 会被_get 的
                                  # 重试逻辑放大成更慢）；8 已能把串行压到 1/6 左右。

_MOM_CACHE: dict = {}            # sym -> (hour_bucket, expires_ts, mom_pct | None)

# ---- 1m 短周期序列（10-06 用户拍板：补「5 分钟」这一档动能盲区）----
# 依据：15m bar 每 15 分钟才推进一次，而模型每 60s 被调用一次 → 同一根 bar 内
#   14/15 次调用拿到的 bar 级信息逐字相同（今日实测「机器候选行」重复率 94%）；
#   缺的正是「这几分钟跌/涨得有多陡」。1H 动能太慢、15m 标注太粗，1m 补这一档。
# ⚠ 纯信息字段：只进 brief 供模型阅读，不写进任何拒开/校验路径（用户铁规）。
M1_TF_MIN = 1                    # feeder._TF_MAP 已支持 1 → "1m"
M1_BARS = 15                     # 喂给模型的根数（= 近 15 分钟）
M1_RETRY_S = 60                  # 失败后重试冷却（同 1H 口径）
_M1_CACHE: dict = {}             # sym -> (minute_bucket, expires_ts, list | None)


def _m1h_mom(sym: str) -> float | None:
    """近 4 根**已收盘** 1H K 线的涨跌百分比（%）。取不到数据返回 None。

    防前视（双保险）：
      ① feeder.closed_bars 内部已 `raw[:-1]` 丢掉最后一根未收盘 bar；
      ② 这里再按 `t + 60min <= now` 二次校验（feeder 的 t 是**毫秒**），
         防止将来 feeder 改动后把未收盘根混进来。

    缓存按**整点分桶**（hour_bucket = now // 3600），不是简单 TTL：
    1H K 线只在整点收盘，所以「跨整点」= 数据一定变了→ 必须重拉；而同一
    小时内的反复调用（scan 每 60 秒一轮、15 品种）全部命中缓存。
    效果：15 次 1H HTTP 集中在整点那一轮（约 7s），其余 59 轮≈0s，
    摊薄后平均每轮 +0.12s；同时**保证不会把整点前的老数据当成最新的用**。

    ⚠️ **纯信息字段**：本函数只给 scan() 拼标注用，返回值不参与任何
       开仓/拒开/仓位判断（用户铁规「判断权 100% 在模型」）。唯一副作用是
       上面那个 1H 行情缓存。
    """
    now = time.time()
    bucket = int(now // 3600)
    hit = _MOM_CACHE.get(sym)
    if hit and hit[0] == bucket and hit[1] > now:
        return hit[2]                # 同一小时内且未到重试冷却 → 直接命中
    mom: float | None = None
    try:
        bars = feeder.closed_bars(sym, MOM_1H_TF_MIN, days=7, n=MOM_1H_BARS)
    except Exception:
        bars = []
    # 已收盘判据：bar 的开始时间 + 1 根 1H 的时长 <= 现在（t 是毫秒）
    cutoff_ms = int((now - MOM_1H_TF_MIN * 60) * 1000)
    closes = [float(b["c"]) for b in (bars or [])
              if float(b.get("t") or 0) <= cutoff_ms]
    if len(closes) >= MOM_1H_WIN + 1:
        base = closes[-1 - MOM_1H_WIN]
        if base > 0:
            mom = (closes[-1] - base) / base * 100.0
    # 成功 → 本小时内一直有效；失败（None）→ 只缓存 60 秒后重试
    ttl = MOM_1H_TF_MIN * 60 if mom is not None else MOM_1H_RETRY_S
    _MOM_CACHE[sym] = (bucket, now + ttl, mom)
    return mom


def _m1_snap(sym: str) -> list | None:
    """近 M1_BARS 根**已收盘** 1m 收盘价，以**最新一根为基准**归一化成百分比列表。

    归一化理由（10-06 用户拍板「15 根 1m 收盘价数组」）：
      原始价格跨品种不可比（BTC 6e4 vs DOGE 0.095），也不便模型直接读"形状"。
      改存 `(c_i - c_last)/c_last*100`（单位 %）后：末项恒 0、首项 = 近 15 分钟
      累计涨跌幅，符号即方向、绝对值即陡峭度 —— 跨品种同尺度、逐根可比。
      精度取 **3 位小数**（不是 2 位）：实测 BTC 相邻 1m 收盘差常只有
      0.004%（86059.9 → 85995.2 → 85991.4），2 位小数会被 round 成连续的
      0.0、把 1m 尺度的信息整段抹平；3 位步长 0.001% ≈ BTC 0.86 USDT，
      已远细于 1m 波动，且每项只多 1 字符（14 品种 ×15 项 ≈ +210 字符/轮）。

    防前视（双重，与 `_m1h_mom` 同口径）：
      ① feeder.closed_bars 内部已 `raw[:-1]` 丢掉未收盘根；
      ② 这里再按 `t + 60s <= now` 二次校验（feeder 的 t 是**毫秒**）。

    缓存按**分钟分桶**（minute_bucket = now // 60）：1m K 线每分钟收盘，
    「跨分钟」= 数据一定变了 → 必须重拉；同一分钟内的重复调用全部命中。
    效果：14 品种 × 1 次 1m HTTP / 分钟，摊到 60s 轮询上约每轮 +1 次/品种。

    ⚠️ **纯信息字段**：返回值只给 scan() 拼标注用，不参与任何开仓/拒开/
       仓位判断（用户铁规「判断权 100% 在模型」）。唯一副作用是上面那个缓存。
    """
    now = time.time()
    bucket = int(now // 60)
    hit = _M1_CACHE.get(sym)
    if hit and hit[0] == bucket and hit[1] > now:
        return hit[2]
    snap = None
    try:
        bars = feeder.closed_bars(sym, M1_TF_MIN, days=1, n=M1_BARS + 1)
    except Exception:
        bars = []
    cutoff_ms = int((now - M1_TF_MIN * 60) * 1000)
    ok = [b for b in (bars or []) if float(b.get("t") or 0) <= cutoff_ms][-M1_BARS:]
    if len(ok) >= M1_BARS:               # 不足 M1_BARS 根 → 不给（不猜、不补 0）
        base = float(ok[-1]["c"])
        if base > 0:
            snap = [round((float(b["c"]) - base) / base * 100.0, 3) for b in ok]
    # 成功 → 本分钟内一直有效；失败（None）→ 只缓存冷却秒数后重试
    ttl = 60 if snap is not None else M1_RETRY_S
    _M1_CACHE[sym] = (bucket, now + ttl, snap)
    return snap


SYSTEM_PROMPT_TPL = """你是加密永续合约高频交易决策者。**本轮任务：先过一遍全池，挑出该开仓的品种与方向**——开不开、开哪个品种(contract)、哪个方向(side)由你定；**价格/张数/仓位/止损/止盈/杠杆全由代码按轨道与风控自动算，你不要管也不要输出。** 平仓不归你管：已持仓的单子由独立的「持仓管理」环节每轮单独判断（它只看持仓品种），你这里只输出 open/hold。
战法唯一：布林带均值回归（BOLL20,2，1小时线）——你的全部判断只允许依据每轮输入的标注数据。
【四个辅助字段】只读参考，不是开仓条件、不得单独决定开仓：
· m1h_mom＝近4根1H涨跌%（供【开仓前置】对齐动能）；null=拉不到，忽略即可。
· m1＝近15根1m收盘价（归一化成%，末项恒0）：看短周期动能陡不陡，急跌急涨最先显形。
· rsi15＝1H RSI(14)：70上偏超买、30下偏超卖、50中性。
· macd_dif/dea/hist＝1H MACD(12,26,9)，已折成%可跨品种比：dif在dea上方为正，hist放大=动能增强。

【brief 结构】每个品种**自成一块**，以 `【品种】 现价 X` 开头，块内固定几行：
`行情`（regime/tier/adx/meaningful/hint/band_pos/轨道 mid·up·dn/atr15/bw/bw_pct/bw_burst/bb_in_kc/mid_slope_atr）
→ `形态`（walk_*/close_out_*/three_push_*/fake_*_n/last_touch/touch_mid/bars_since_touch/legs）
→ `动能`（m1h_mom/rsi15/macd_dif·dea·hist）→ `1m 序列`（m1）→ `白话读法`（read）
→ `候选`（有则给 side/level/deep/seq/wm/confirm_ready/first/cand_t/age_bars，无则写「无」）
→ `已有持仓`（该品种的账内仓，没仓就不出现这一行）。
**一个品种的全部信息都在它自己那一块里**，不要在别的块里找它的数字；块内缺的字段＝本轮没有该项素材。

【数据】每轮给你 1 小时布林(BOLL20,2)全套标注。先记三个统计事实：①布林是波动率标尺——带宽=市场波动率，收窄=蓄势、张开=释放，波动率循环"静久必动、动久必静"；②±2σ 在加密上大约只罩住 85~90% 的价格（肥尾，比教科书 95% 少），所以收盘滑出轨道本身就值得注意；③触轨只是"tag"（此处价格相对高/低），永远不是买卖信号——它的含义完全取决于行情分段（趋势里=强劲，震荡里=回归）。
【轨道与位置】mid=中轨（20期均线，趋势中是动态支撑/阻力）、up/dn=上下轨、px=现价、atr15=一根K线的大致波幅。价格在中轨上方=多头占优、下方=空头占优；价格碰轨后总回中轨=区间市；趋势市里回踩中轨常是顺势者的入场位、不是反转。mid_slope_atr=中轨较上根移动的 ATR 倍数：接近 0（|值|<0.1）=中轨走平=震荡市前提；明显为正=中轨上斜（涨势）、明显为负=下斜（跌势）——中轨斜率是分"区间还是趋势"的第一判据。
【波动率四态（带宽维度）】bw_pct=带宽分位（0=历史最窄、1=历史最宽）：≤0.15 挤压——波动率压到历史低位，即将变盘但不给方向；≥0.85 最宽——波动率释放中，强趋势或临近衰竭，靠价格位置区分；中间=常态。bw_burst=带宽较上根张口倍数：≥1.35 是变盘点火，看收盘冲出哪条轨；挤压后第一冲常见 head fake，别追，等收回或第二冲。bb_in_kc=true=布林整条挤进另一条通道，极端挤压确认。
【趋势行为（中轨同侧维度）】walk_dn_bars＝**连续几根 1H 收盘价在中轨下方**（walk_up_bars 镜像）；**有一根收盘回到中轨另一侧就立刻清零**。⚠ 它量的是"收盘在中轨哪一侧"，**不是"贴不贴轨"**：walk_dn_bars=4 配上 band_pos≈0.5 ＝ 只是收盘还在中轨下方，**别写成"贴着下轨滑行"**；贴不贴轨只看 band_pos 与 last_touch。walk_dn_touch＝这段里有没有任何一根最低价碰过下轨（随 walk 一起清零）。连续收盘在中轨同侧是趋势顺向的依据、不是反转信号；强趋势里超买可以一直超买，别猜头猜底，walk 结束前只考虑顺向。close_out_up/dn_bars＝连续几根**收盘价滑出轨道外**：≥2 根=强趋势硬确认。last_touch=最近一次触轨的方向（up=碰上轨、dn=碰下轨）、touch_mid=触轨之后有没有收盘越过中轨到另一侧、bars_since_touch=距那次触轨过了几根：touch_mid=false=还挂在轨外没回来，趋势或张口没走完，别猜头；touch_mid=true=已经回来过=回归结构成立，这时碰轨才是回归机会。
【衰竭与反转素材】three_push_up/dn=同侧轨道累计碰轨次数：≥3 次要盯 M头/W底——判据是第二个高/低点够不够得到轨道（缩回带内=动能衰竭），第二次还破轨=形态失败。legs=两脚明细（仅第一脚确实触到轨外时才出现，缺=无双底/双顶素材）：legs.w={l1,l2,ok:第二脚是否缩回带内(true=W底成立),gap}，legs.m={h1,h2,ok,gap}镜像。位置归一化 (px-dn)/(up-dn) 越高=越靠上轨，l2>l1=第二脚更弱=衰竭；ok=false=第二脚又破轨，形态失败。fake_up/dn_n=6根内冲出轨又收回的假突破次数：连续假突破=那边没人接盘、动能耗尽，是反转素材。
【行情定性 regime】机器每轮实时分类（ADX14+带宽口径，与上面布林标注相互独立）：up_trend·down_trend=ADX≥25、方向看 +DI/−DI（强趋势进行中，别逆着做）／high_vol=ADX 不足但带宽>3%（波动释放、方向未定；代码里与 range 同档）／range=震荡（碰轨回归的主战场）／low_vol=带宽过窄、轨道不具参考（meaningful=false，该品种标注整体打折看）。另给 adx（趋势强度）、bw（绝对带宽）、hint（触轨素材）、meaningful（轨道可否参考）四个辅助量。
【选池与候选｜每轮先过全池】**你每轮看到的是全池所有品种的完整标注，不是只有候选。**
   · **每个品种都要自己过一遍**，挑出"哪些适合开仓"，它们彼此平等、没有主次。
   · **【机器候选】**是形态识别扫出的结构（deep／seq／wm，形态定义见【形态素材】），带 level=失效位、confirm_ready=回归方向已确认——**它是一份参考证据，帮你快速发现形态，但绝不是"只有候选才能开"的门票**。
   · **没被扫出候选的品种照样可以开**，只要你能在它的标注上找到开仓依据（【五态判据】任一条成立即可）。
   · 顺序建议：先扫一遍全池 → 心里过一遍"哪些符合开仓条件" → 再决定开哪几个（可以一个、多个、或者全不开）。
【五态判据】直接读标注判断开不开、开哪个方向：
· regime=range 且 walk_up_bars 与 walk_dn_bars 都≤2（干净横盘）+ px 贴近一轨 → 顺轨反抽到中轨，这是主战场；
· bw_pct≤0.15（挤压蓄势）：只等方向不提前埋伏——盯 burst≥1.35 且收盘冲出轨那一下，照突破方向做；
· regime=up_trend/down_trend 或 close_out_up/dn_bars≥2：强趋势进行中，只做顺向，不做"涨太多该回来"的逆向单。
  顺趋势有两种开法：①**顺动能**——价格在顺势一侧、m1h_mom 同向（下跌途中做空／上涨途中做多）；
  ②**【趋势反抽】**（10-09 用户令新增）——**down_trend 里价格反抽回中轨上方时，顺着跌势做空**，
  这条**不需要形态素材**：反抽回中轨上方本身就是趋势给的入场位。但它必须"已现衰竭"，
  判据见【开仓前置】②；判不出衰竭就还是 hold。
· regime=high_vol 或 bw_pct≥0.85：带宽最宽区，突破与衰竭赛跑。reason 能点名"这是衰竭不是突破"（deep／seq／wm 之一，见【形态素材】）时可考虑，win_rate 只报 0.55；
· **【形态素材】**：deep／seq／wm／legs.ok／fake_n≥2 任一成立 = 动能衰竭或没人接盘，是最该开的依据；"机器候选"扫的就是这几类结构。
  其**合法来源只认 brief 给出的证据，不许凭位置自行推断**：①该品种块里的「候选」行不是「无」→ 按候选里写的类型点名；②fake_up_n 或 fake_dn_n≥2 → 可点名 seq；③legs.ok=true → 可点名 wm；④以上都没有 → **不许点名**，该品种只能做"顺动能单"（动量同号＋位置合格）或"【趋势反抽】做空单"（见【开仓前置】②），否则不开。
· **【开仓前置】硬约束、不是参考**：side 必须与 m1h_mom 同号（多=正、空=负），|m1h_mom|<0.05% 视为侧向不受限。
  逆动能单**只允许**下面两类，reason 必须点名对应证据；点不出来就别开：
  ①**形态素材单**：deep 深砸穿轨后收回／wm·legs.ok 双底双顶／seq 连续假突破≥2 次（合法来源见上）；
  ②**【趋势反抽·做空】**（10-09 用户令新增；**只做空、只限 down_trend**）——同时满足：
     · regime=down_trend（中轨下斜、-DI>+DI，趋势确实向下）；
     · band_pos ≥ 0.6（价格已反抽回中轨上方 ＝ 这是"反弹"，不是"下跌途中"）；
     · **衰竭证据至少中一条**（reason 里必须写出是哪一条）：
       ▸ fake_up_n ≥ 2 —— 反弹反复冲上轨又被打回，上轨有人接盘；
       ▸ band_pos ≥ 0.8 —— 反抽已顶到上轨；
       ▸ rsi15 ≥ 70 —— 短周期超买；
       ▸ 1m 序列末端已掉头（最近几根由正转负）—— 反弹当场熄火。
     · win_rate 报 0.60；reason 用大白话说清"跌势里的反弹、价格已回到中轨上方、反弹在熄火"。
     ⚠ 本条**只放开"顺趋势的反抽空"**；up_trend 里价格回踩到中轨下方的顺势多**不在放开范围**，仍只按①判。
【教训的正确用法】brief 里的"最近教训"是**历史记录，不是当前指令**。它只告诉你"过去在类似场景下发生过什么"，**不能覆盖你依据当前标注做出的判断**。
   · 教训天然偏向亏损（只有亏了才写），所以**连续几轮教训会显得都在劝你不开仓**——这是正常现象，不代表现在就该不开。
   · **如果你依据当前标注（m1h_mom + 位置 + 【五态判据】）判断某品种该开，而教训讲的是别的品种或更早的行情状态**（比如上一轮挤压期的教训），**不要因此放弃**——只在"同品种、同位置、同 regime"时才当回事。
【其余】每个品种块里都有 band_pos（0=下轨 0.5=中轨 1=上轨）与全套标注，以及该品种的账内持仓（有仓才有那一行）；首行是数据时点；每轮独立判断，不存在"沿用上轮决策"——数据变了就按这轮重新决定。

【历史数据陈述】以下来自本系统自己的实盘流水，只作参考：
· 贴轨开仓与开在两轨中间结果不同，哪侧更好随品种与行情而变——数字见【按标签的真实战绩】"按位置"分档。
· 4H 方向无区分度。
· 趋势中反复碰轨是最大的亏钱来源（多为趋势强劲而非反转），故 regime=up_trend/down_trend 只做顺向；**"趋势反抽"做空（反抽回中轨上方后顺着跌势开空）属"顺向"范畴，已放开（见【开仓前置】②）**。

{PERF}
【开仓纪律】同时想开多个时，主字段放第一个，其余放 also_open（合计最多 3 个，可来自全池任意品种）。
win_rate=**信号质量的三档标签，不是概率**（代码只用它做一道 <0.50 的拒开闸；三档均 ≥0.55 → 本闸不会触发，也不参与仓位大小）。只许报这三个值：
· 0.70＝动量同号 **且**点名了形态素材（deep／seq／wm／legs.ok／fake_n≥2）——证据完整；
· 0.60＝动量同号、位置干净、离中轨有距离，但没有任何形态素材——只是顺势位置单；**或【趋势反抽】做空单**（顺趋势、反动能，但位置与衰竭证据齐备）；
· 0.55＝你自己都觉得勉强——**这时候该直接 hold**。报 <0.50 会被代码硬拒开，所以不要用低 win_rate 表达犹豫：拿不准就 hold。
【止损/止盈真实口径】止损距离=入场价×{SL_PCT}%、止盈距离=入场价×{TP_PCT}%（固定，全品种同口径）。
开仓前自己算 atr15/px：止损距离 ≥ 1×ATR15（常态）→ 一根K线抖不出止损；止损距离 < 1×ATR15（窄带/低波动品种）→ **一根K线的正常抖动就能打掉你**，这类品种宁可不做。
被 SL 打出不等于方向错，所以入场必须挑"离中轨还有足够距离"的位置。

【输出】只输出一个 JSON：
{"action":"open|hold","contract":"BTC_USDT","side":"long","win_rate":0.70,
 "regime":"up_trend|down_trend|range|high_vol|low_vol","bias":"偏多|偏空|中性","reason":"一句话",
 "checks":{"mom":0.42,"aligned":true,"atr_pct":0.55,"dist_mid_pct":0.61},
 "scan":[{"c":"BTC_USDT","v":"long"},{"c":"ETH_USDT","v":"skip"},{"c":"DOGE_USDT","v":"short"}],
 "also_open":[{"contract":"ETH_USDT","side":"short","win_rate":0.60,"reason":"...",
               "checks":{"mom":-0.31,"aligned":true,"atr_pct":0.48,"dist_mid_pct":0.72}},
             {"contract":"XRP_USDT","side":"short","win_rate":0.55,"reason":"..."}]}
- **scan＝你逐品种过筛的凭据，必须把上面列出的品种**一个不漏**地写进去**（顺序随意，数量要对得上）：c=品种，v=你对它的结论，只取 long／short／skip（skip＝这轮不开它）。代码会核对覆盖率，漏掉的品种会记入日志。
- 每行的 **read** 是代码按【轨道与位置】口径**算好的事实句**（位置·触轨·中轨斜率·4H动能）——**以它为准**，不要去重新解读 band_pos/hint，更**禁止**写出与它相矛盾的描述（例如 read 说"触下轨后已收回带内"，就不能说"贴着下轨滑行"）。
- checks 必须与标注数据一致、不得编造：mom=该品种 m1h_mom 原值；aligned=side 与 mom 是否同号（侧向一律 true）；
  atr_pct=atr15/px×100（两位小数）；dist_mid_pct=|px−mid|/px×100（两位小数）。hold 时 checks 可省略。
- also_open 可为空数组或省略；其中每一项的字段口径与主字段相同（含各自的 checks）。同时开多个的规则见【开仓纪律】。
- reason 必须用大白话，写给不懂指标的人看：说清"价格刚才做了什么，所以开/不开"。禁止用代号、字段名、缩写——❌"P1 squeeze bw_pct0.04"，✅"带宽收到历史最窄，赌它向上突破，做空反着不碰"。
- 不要输出 close：平仓由「持仓管理」环节负责，你这里只回 open 或 hold。
- regime/bias 是你对行情状态的判断，下轮会回喂给你。
{UNIVERSE}"""


def _read_trades_tail(path: str, tail_bytes: int = 32768) -> list:
    """只读 trades.json **尾部** tail_bytes 字节并解析成 list（不做全量读）。

    为什么不能全量读：trades.json 只追加、已 135KB 且在增长，而
    `_brief()` 每 60s 调一次、`/api/state` 每次刷新都调 → 全量 read+json.loads
    是纯浪费（实测 26ms → 0.3ms）。文件是**单行 JSON 数组**，所以做法是：
    seek 到 末尾往前 tail_bytes → 丢掉第一个不完整的 `{...}`（用第一个 `}`
    之后开始）→ 用 `[` + 段+ `]` 拼回去 json.loads。取不到就返回 []，
    绝不抛异常打断决策主流程。

    归一化：binary 读 + decode("utf-8","ignore")，再把 \r\n / \r 折成 \n
    （与 server.py:_build_payload_uncached 读 llm_decisions.md 同一坑：
    text 模式会做 universal-newline 归一，直接 binary 不会，不归一会让
    含中文的切片窗口错位）。
    """
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            f.seek(max(0, size - tail_bytes))
            raw = f.read().decode("utf-8", "ignore")
        raw = raw.replace("\r\n", "\n").replace("\r", "\n")
        # ⚠ 10-04 19:5x 两个坑都在这里（表现为「统计永远 n=0」且不报错）：
        #  ① raw 末尾那个 ']' 之后没有内容，但**最后对象的结尾 '}' 仍在**；
        #     早先用 find('}') 去头会把它当残片切掉 → 拼出非法 JSON。
        #  ② 不能用「从后往前找 '{' 再回退」的写法找起点——JSON 里的
        #     reason 字段是带 { } 的字符串，会把对象边界找错。
        # 正确做法：先切到最后一个 ']' 之前，再**顺着结构**从第一个
        # 「完整对象的开始」起截：定位**最后一个顶层对象的 '{'**，
        # 用 json.JSONDecoder().raw_decode 从那里解一次。
        end = raw.rfind("]")
        if end < 0:
            return []
        head = raw[:end]
        start = head.rfind("{")
        # 从 start 往后逐个试解码，直到某次成功（第一个成功的即最后完整对象）
        dec = json.JSONDecoder()
        idx = start
        out: list = []
        while idx >= 0:
            try:
                obj, nxt = dec.raw_decode(head, idx)
            except ValueError:
                nxt = -1
            if nxt <= 0:
                break
            if isinstance(obj, dict):
                out.insert(0, obj)     # 从后往前扫到的要倒序装回
            idx = head.rfind("{", 0, idx)
        return out
    except Exception:
        return []


def _reentry_anchor(runtime_dir: str, sym: str, side: str,
                    window_min: float = 0.0):
    """取该 sym|side 最近一笔平仓的「再入场锚点价」（10-06 用户拍板）。

    为什么要它：上一单被动态止盈平掉后，如果价格已经走远，再按现价开同品种
    同方向就是**追价**。今日实测（58 笔重复开仓）：新单入场价比上次出场价差
    >0.10% 的那组笔均 −0.33U、胜率 1/4；没变差的那组笔均 −0.04U。

    锚点规则（**全由代码负责**，模型只管品种 + 方向）：
      · 上次同向出场**盈利** → 锚 = 那次**出场价**（不接得比自己刚卖出的更贵）
      · 上次同向出场**亏损** → 锚 = 那次**开仓价**（不接得比自己的成本位更贵）

    数据源 = `runtime/journal-YYYYMMDD.jsonl`。**不能用 trades.json**：它的
    close 记录只有 pnl、没有 entry/px，取不到价格；journal 的 close 记录带
    entry/px/side/pnl/close_cause 全套。
    只读尾部 256KB（同 `_read_trades_tail` 的理由：每轮决策都要调，不能全量读）；
    顺带扫前一天的文件，避免每天开盘首个同向单失去锚点。

    `window_min>0` 时只认该分钟内的平仓，否则不限。
    返回 (anchor_px, 说明文本) 或 None。**任何异常一律返回 None**（绝不打断开仓）。
    """
    try:
        import glob
        pat = os.path.join(runtime_dir or ".", "journal-*.jsonl")
        files = sorted(glob.glob(pat))[-2:]
        last = None
        for fp in files:
            try:
                sz = os.path.getsize(fp)
                with open(fp, "rb") as f:
                    f.seek(max(0, sz - 262144))
                    raw = f.read().decode("utf-8", "ignore")
            except Exception:
                continue
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if (r.get("ev") == "close" and r.get("sym") == sym
                        and str(r.get("side") or "").lower() == side):
                    if last is None or (r.get("t") or 0) >= (last.get("t") or 0):
                        last = r
        if last is None:
            return None
        if window_min > 0:
            age = (time.time() - float(last.get("t") or 0)) / 60.0
            if age > window_min:
                return None
        pnl = float(last.get("pnl") or 0)
        if pnl > 0:
            px = float(last.get("px") or 0)
            why = "上次同向盈利出场"
        else:
            px = float(last.get("entry") or 0)
            why = "上次同向亏损出场"
        if px <= 0:
            return None
        return px, (f"{why} {px:g}（{last.get('ts')} "
                    f"{last.get('close_cause')} pnl={pnl:+.4f}）")
    except Exception:
        return None


def _perf_stats(runtime_dir: str) -> dict:
    """从 runtime/trades.json 的**真实平仓记录**统计战绩（注入 {PERF}）。

    为什么不写死数字（10-05 用户拍板）：提示词里原来那些 41.5% / 69% / 65%
    全是**别的时期、别的口径**的回测数字，市场和参数都变过了，写死会误导
    模型。这里改成每次决策时从自己的流水实时算——数字会随样本滚动更新，
    老数据自动退出。

    ⚠ **只统计带完整字段的记录**（判据：`tp_dist_pct` is not None）。
       engine.py 17:45 之前写的 close 记录没有 tp_dist_pct/sl_dist_pct/
       hold_min/peak 这组字段（只有 t/sym/side/action/pnl/reason），统计它们
       会得出错误结论（分不出是止盈还是止损、算不出持仓时长）。

    分桶维度（都是纯统计，不产生任何拦截）：
      · 出场方式 = reason 字段（SL/TP 条件单触发／平仓（引擎执行）／手动平仓）
      · 盈亏方向 = pnl>0
    返回 {"n","win_rate","avg_pnl","hold_avg_min","by_exit"}；读不到返回 n=0。
    """
    trades = _read_trades_tail(os.path.join(runtime_dir or ".", "trades.json"))
    closes = [t for t in trades
              if t.get("action") == "close" and t.get("tp_dist_pct") is not None]
    n = len(closes)
    if not n:
        return {"n": 0, "win_rate": 0.0, "avg_pnl": 0.0, "hold_avg_min": None,
                "by_exit": {}}
    wins = sum(1 for t in closes if float(t.get("pnl") or 0) > 0)
    avg = sum(float(t.get("pnl") or 0) for t in closes) / n
    holds = [float(t["hold_min"]) for t in closes if t.get("hold_min") is not None]
    by_exit: dict = {}
    for t in closes:
        k = str(t.get("reason") or "未标注")
        b = by_exit.setdefault(k, {"n": 0, "wins": 0, "pnl": 0.0})
        b["n"] += 1
        b["pnl"] += float(t.get("pnl") or 0)
        if float(t.get("pnl") or 0) > 0:
            b["wins"] += 1
    return {"n": n, "win_rate": round(wins * 100.0 / n, 1), "avg_pnl": round(avg, 4),
            "hold_avg_min": (round(sum(holds) / len(holds), 1) if holds else None),
            "by_exit": by_exit}


def _entry_src_of(open_row: dict) -> str:
    """从 open 记录还原形态来源（deep/seq/wm/touch/无）。

    优先用开仓当时落下的 `entry_src`；老数据没有该字段时，从 `sig` 反推
    （sig 是候选标记：cand_confirm/cand_raw/model），推不出才看 reason 里
    有没有形态关键词。
    """
    v = str(open_row.get("entry_src") or "").strip()
    if v:
        return v
    sig = str(open_row.get("sig") or "")
    if sig.startswith("cand"):
        return "touch"
    txt = str(open_row.get("reason") or "")
    for k in ("deep", "seq", "wm"):
        if k in txt.lower():
            return k
    return "无"


def _mom_dir_of(open_row: dict) -> str:
    """动能方向标签：顺动能 / 逆动能 / 侧向(≈0) / 无数据。"""
    m = open_row.get("m1h_mom")
    if m is None:
        return "无数据"
    try:
        v = float(m)
    except (TypeError, ValueError):
        return "无数据"
    if v > 0.05:
        return "顺动能"
    if v < -0.05:
        return "逆动能"
    return "侧向"


def _pos_zone_of(open_row: dict) -> str:
    """位置分档（band_pos）：贴下轨 / 中轨区 / 上轨区 / 追高 / 无数据。"""
    b = open_row.get("band_pos")
    if b is None:
        return "无数据"
    try:
        v = float(b)
    except (TypeError, ValueError):
        return "无数据"
    if v < 0.2:
        return "贴下轨"
    if v < 0.5:
        return "中轨区"
    if v <= 0.65:
        return "上轨区"
    return "追高"


def _regime_of(open_row: dict) -> str:
    """行情定性归类（10-07 随 regime 口径改 seine 五态）：保留
    range/high_vol/up_trend/down_trend 四档有对比意义的，low_vol（轨道失参考）
    统一归「其他」——格子太碎样本会被摊薄。"""
    r = str(open_row.get("regime") or "").strip()
    return r if r in ("range", "high_vol", "up_trend", "down_trend") else "其他"


def _paired_tagged_trades(runtime_dir: str) -> list:
    """配对出「每笔已平仓交易 + 它开仓时的标签」，返回 [(open_row, pnl), ...]。

    配对规则见 `_tag_stats` 的docstring（用 close 的 t_at_open 反查 open 的
    trade_id，只认能精确配上的）。**抽出来是为了让 `_tag_stats`（喂模型
    提示词）与 `_tag_stats_dims`（喂前端看板）共用同一套配对逻辑**——两边
    口径必须一模一样，否则看板和模型看到的数字会打架。
    """
    trades = _read_trades_tail(os.path.join(runtime_dir or ".", "trades.json"),
                               65536)
    opens = {}
    for t in trades:
        if t.get("action") != "open":
            continue
        tid = str(t.get("trade_id") or "")
        if tid:
            opens[tid] = t
    out = []
    for c in trades:
        if c.get("action") != "close" or c.get("pnl") is None:
            continue
        t_open = c.get("t_at_open")
        if not t_open:
            continue                # 老 close：无 t_at_open，跳过
        o = opens.get(f"{c.get('sym')}|{c.get('side')}|"
                      f"{int(float(t_open) * 1000)}")
        if o is None:
            continue                # 配不上（老 open 无 trade_id）
        out.append((o, float(c.get("pnl") or 0)))
    return out


def _tag_stats(runtime_dir: str) -> dict:
    """按标签维度统计**真实**胜率（{PERF} 的原版 4 维桶口径）。

    ⚠ 10-06：喂模型的 `_tag_clause()` 已改为**边际统计自算**、不再调本函数；
      本函数现服务于 `review.py` 的 4 维桶键反查与单测。桶键口径保持不变。

    配对方式（任务 E）：close 记录由 engine.py 写、**没有 trade_id**（那个
    文件不许改），但它带 `t_at_open`（开仓毫秒时间戳，从 open_meta 抄来）。
    所以这里的配对键是 `(sym, side, round(t_at_open))` —— 与 open 记录里的
    `trade_id` 后两段同源，比"最近的 open"可靠得多（SUI 今天开过 11 次仓，
    按时间就近配对会错位）。

    ⚠ **只统计 trade_id 能精确配上的记录**（open 侧 10-05 才开始写该字段，
       之前的老记录一律跳过）。这不是挑剔：老记录没有 m1h_mom/band_pos/
       regime/entry_src，统计它们只会产出一堆「无数据 × 无数据 × 其他」
       的假桶——n=71、胜率 64.8% 这种数字看着像结论，其实只是"没标签"
       的另一种写法，会把模型往错的方向带。宁可 n 小到没有，也不给假统计。
       （`build_recent_reviews` 是另一回事：它是给人/模型看的**单笔回顾**，
       老数据照样显示，只是省略缺失字段。）

    ⚠ **只统计标签齐全的记录**、**绝不拿标签去拦截开仓**——纯统计用途。
      调用方：`review.py`（4 维桶键反查，见该文件 `_tag_stats()` 一致性注释）
      与 `_test_flip_single_position.py`。10-06 起 `_tag_clause()` 不再调它。

    返回 {"n": 配对上的完整交易数, "buckets": {标签组合: {n,wins,pnl,sum}}}
    """
    trades = _read_trades_tail(os.path.join(runtime_dir or ".", "trades.json"),
                              65536)
    opens = {}
    for t in trades:
        if t.get("action") != "open":
            continue
        tid = str(t.get("trade_id") or "")
        if tid:
            opens[tid] = t
    buckets: dict = {}
    paired = 0
    for c in trades:
        if c.get("action") != "close" or c.get("pnl") is None:
            continue
        t_open = c.get("t_at_open")
        if not t_open:
            continue                       # 老 close：无 t_at_open，跳过
        o = opens.get(f"{c.get('sym')}|{c.get('side')}|"
                      f"{int(float(t_open) * 1000)}")
        if o is None:
            continue                       # 配不上（老 open 无 trade_id）
        paired += 1
        key = (f"{_mom_dir_of(o)} × {_pos_zone_of(o)} × "
               f"{_regime_of(o)} × {_entry_src_of(o)}")
        b = buckets.setdefault(key, {"n": 0, "wins": 0, "pnl": 0.0})
        b["n"] += 1
        p = float(c.get("pnl") or 0)
        b["pnl"] += p
        if p > 0:
            b["wins"] += 1
    return {"n": paired, "buckets": buckets}

def _tag_stats_dims(runtime_dir: str) -> dict:
    """标签统计的**看板版**：同时给「按单维度」与「按组合」两份（任务 J）。

    为什么要有这个而不能直接用 `_tag_stats()`：`_tag_stats` 把 4 个维度
    压成一串 label（"顺动能 × 贴下轨 × range × deep"），格子太散——总样本
    一分，每个格子 n 常常只有 1~2，胜率算不出任何东西。前端要画成
    **四张**独立小表（1H动能 / 位置 / 行情定性 / **入场源**），各自��� n
    才有意义。

    ⚠ 10-05 修正：这段docstring 原来写的是「三张」，漏了 `entry_src`。
    后端一直在算第 4 个维度、前端渲染循环也只映射了 3 个 → **两边一致地
    错，所以没人发现**（已同步修前端 + 此处）。

    返回结构（server.py 直接透给前端，字段名与前端渲染一一对应）：
      {"n": 配对总数,
       "dims": {"mom_dir": [行...], "pos_zone": [...], "regime": [...],
                "entry_src": [...]},
       "groups": [行...],          # 4 维组合，只给 n>=3 的（太碎的格子没意义）
       "note": "样本不足"}         # 仅 n<10 时出现

    ⚠ `n` 只统计**尾部 64KB** 里的配对记录（性能考虑，见 `_read_trades_tail`），
    而前端要显示的「平仓总数」由 `server.py` 从**全量** `trades` 列表另算后
    以 `total_closed` 注入 —— 两者口径不同是**故意的**：`n` 越小越保守（宁少勿错），
    而分母必须用真实全量，否则会把覆盖率算高（实测：尾部口径 28/83=34%，
    全量口径 28/233=12%——高估了近 3 倍，正是要防的那种误导）。

    行的结构：{"label","n","win_rate"(0~1),"avg_pnl","cum","thin"(bool)}
      · win_rate 存**小数**（0.875）不是百分数——前端自己乘100 格式化，
        避免"到底传 87.5 还是 0.875"这种两边猜的接口歧义。
      · thin = n<5，前端据此灰显。

    与 `_tag_stats()` **共用同一个配对函数** `_paired_tagged_trades()`，
    保证看板数字与提示词里的数字永远一致（否则模型和用户看到两个口径）。
    """
    pairs = _paired_tagged_trades(runtime_dir)
    n = len(pairs)
    dims: dict = {k: {} for k in ("mom_dir", "pos_zone", "regime", "entry_src")}
    groups: dict = {}
    for o, p in pairs:
        # 单维度：同一笔交易会同时进4 个维度的桶（这是"分组统计"的定义，
        # 不是重复计数 bug——每个维度独立成表，n 各不相同）
        for key, fn in (("mom_dir", _mom_dir_of), ("pos_zone", _pos_zone_of),
                        ("regime", _regime_of), ("entry_src", _entry_src_of)):
            b = dims[key].setdefault(fn(o), {"n": 0, "wins": 0, "pnl": 0.0})
            b["n"] += 1
            b["pnl"] += p
            if p > 0:
                b["wins"] += 1
        gk = (f"{_mom_dir_of(o)} × {_pos_zone_of(o)} × "
              f"{_regime_of(o)} × {_entry_src_of(o)}")
        g = groups.setdefault(gk, {"n": 0, "wins": 0, "pnl": 0.0})
        g["n"] += 1
        g["pnl"] += p
        if p > 0:
            g["wins"] += 1

    def _rows(src: dict, min_n: int = 1) -> list:
        """桶 dict → 前端行 list（按 n 降序，n 相同时按累计盈亏降序）。"""
        out = []
        for label, b in src.items():
            if b["n"] < min_n:
                continue
            out.append({"label": label, "n": b["n"],
                        "win_rate": round(b["wins"] * 1.0 / b["n"], 4),
                        "avg_pnl": round(b["pnl"] / b["n"], 4),
                        "cum": round(b["pnl"], 4),
                        "thin": b["n"] < 5})
        out.sort(key=lambda r: (-r["n"], -r["cum"]))
        return out

    res = {"n": n,
           "dims": {k: _rows(v) for k, v in dims.items()},
           "groups": _rows(groups, min_n=3)}
    if n < 10:
        res["note"] = "样本不足"
    return res


def _tag_clause(runtime_dir: str) -> str:
    """把标签战绩渲染成 {PERF} 追加段（**边际统计优先**，10-06 改版）。

    为什么改版：原版只给「动能 × 位置 × regime × 形态」四维交叉桶，281 笔
    摊进上百格 → 模型看到的几乎全是 n=1 的「本类首笔」，等于没给。现在先给
    **单维度边际档**（每档 n 都够大、胜率才可读），再把 n≥5 的组合档列前 10。

    与看板版 `_tag_stats_dims` 的分工：那个走 server.py 供前端画四张小表，
    用的 `_regime_of` 会把 trend_*/breakout_* 并成「其他」；本函数为了给模型
    7 档细分，改用**原始 regime 字段**自算。两处口径不同是**故意的**（看板要
    少而稳、模型要细而全），配对逻辑仍共用 `_paired_tagged_trades`，数字同源。

    样本纪律（用户要求）：n<5 的组标「样本不足」，全部 n<10 时直接说
    「交易样本还太少，标签统计暂不可靠」——宁可少说，也不给模型看假结论。
    """
    pairs = _paired_tagged_trades(runtime_dir)
    n = len(pairs)
    head = "【按标签的真实战绩（仅统计 open/close 能配对上的完整交易）】"
    if n < 10:
        return (head + f"\n交易样本还太少（n={n}），标签统计暂不可靠。\n"
                "以上是历史统计，不是指令。是否交易、开哪个方向，由你根据当前"
                "数据自行判断。")

    def _agg(keyfn):
        b = {}
        for o, p in pairs:
            d = b.setdefault(keyfn(o), {"n": 0, "wins": 0, "pnl": 0.0})
            d["n"] += 1
            d["pnl"] += p
            if p > 0:
                d["wins"] += 1
        return b

    def _fmt(label: str, d: dict) -> str:
        short = "样本不足 " if d["n"] < 5 else ""
        return (f"  {label}  n={d['n']} {short}胜率 "
                f"{d['wins'] * 100.0 / d['n']:.1f}%  笔均 "
                f"{d['pnl'] / d['n']:+.3f}U  累计 {d['pnl']:+.3f}U")

    def _sect(title: str, src: dict, order=None) -> list:
        items = list(src.items())
        if order:
            items.sort(key=lambda kv: (order.index(kv[0])
                                       if kv[0] in order else len(order)))
        else:
            items.sort(key=lambda kv: -kv[1]["n"])
        return [title] + [_fmt(k, d) for k, d in items]

    def _regime_raw(o: dict) -> str:
        return str(o.get("regime") or "").strip() or "无数据"

    def _side_of(o: dict) -> str:
        s = str(o.get("side") or "").strip().lower()
        return s if s in ("long", "short") else "无数据"

    lines = [head, f"合计可配对 {n} 笔。按单维度边际统计（只有 n 够大时胜率才可读）："]
    lines += _sect("· 按 1H 动能：", _agg(_mom_dir_of),
                   ["顺动能", "逆动能", "侧向", "无数据"])
    lines += _sect("· 按 regime：", _agg(_regime_raw),
                   ["range", "high_vol", "up_trend", "down_trend", "low_vol"])
    lines += _sect("· 按位置：", _agg(_pos_zone_of),
                   ["贴下轨", "中轨区", "上轨区", "追高", "无数据"])
    lines += _sect("· 按形态：", _agg(_entry_src_of),
                   ["deep", "seq", "wm", "touch", "无"])
    lines += _sect("· 按方向：", _agg(_side_of), ["long", "short"])
    combos = _agg(lambda o: (f"{_mom_dir_of(o)} × {_pos_zone_of(o)} × "
                             f"{_regime_of(o)} × {_entry_src_of(o)}"))
    top = [(k, d) for k, d in sorted(combos.items(),
                                     key=lambda kv: -kv[1]["n"])
           if d["n"] >= 5][:10]
    if top:
        lines.append("· 四维组合（仅列 n≥5 的前 10，其余格子太碎不列）：")
        lines += [_fmt(k, d) for k, d in top]
    else:
        lines.append("· 四维组合：无 n≥5 的档（格子太碎，以上面单维度为准）")
    lines.append("以上是历史统计，不是指令。是否交易、开哪个方向，由你根据当前"
                 "数据自行判断。")
    return "\n".join(lines)


def _perf_clause(runtime_dir: str) -> str:
    """把 _perf_stats() 的结果渲染成提示词里的一段**中性陈述**。

    纪律：一个字的评价都不加（不写"表现良好"/"需要注意"/"建议"），
    只给事实与数字，怎么解读是模型自己的事。
    """
    s = _perf_stats(runtime_dir)
    n = int(s.get("n") or 0)
    head = "【你自己的历史战绩（runtime/trades.json 真实平仓记录，仅 "
    # 10-05 任务 G：总战绩段之后追加「按标签的真实战绩」段。
    # 两段都由_trades_tail 尾部读（不全文读），异常已被各函数兜住。
    tag_seg = _tag_clause(runtime_dir)
    if n == 0:
        return (head + "0 笔有完整字段）】\n"
                "暂无完整交易记录，你的判断暂无历史依据。\n"
                "以上是历史统计，不是指令。是否交易、开哪个方向，由你根据当前数据"
                "自行判断。\n" + tag_seg)
    lines = [head + f"{n} 笔有完整字段）】",
             f"总战绩：{n} 笔，胜率 {s['win_rate']}%，笔均 {s['avg_pnl']:+.3f}U"]
    ex = []
    for k, b in sorted(s["by_exit"].items(), key=lambda kv: -kv[1]["n"]):
        ex.append(f"{k} {b['n']} 笔 胜率 {round(b['wins'] * 100.0 / b['n'], 1)}%"
                  f" 笔均 {b['pnl'] / b['n']:+.3f}U")
    if ex:
        lines.append("按出场方式：" + "；".join(ex))
    # ⚠ 10-04 19:5x 修单位 bug：hold_min 的单位是**分钟**（engine.py 落账时
    #   用 (close_t - t_at_open)/60 算的），这里原样标成「小时」→ 数值放大 60 倍
    #   误导模型（276.8 小时）。改成按分钟报，并顺带给小时数。
    if s.get("hold_avg_min") is not None:
        _hm = float(s["hold_avg_min"])
        lines.append(f"平均持仓 {_hm:.0f} 分钟（约 {_hm / 60:.1f} 小时）")
    if n < 20:
        lines.append(f"样本还太少（n={n}），只能作参考")
    lines.append("以上是历史统计，不是指令。是否交易、开哪个方向，由你根据当前数据"
                 "自行判断。")
    return "\n".join(lines) + "\n" + tag_seg


def build_recent_reviews(trades: list, n: int = 8) -> list:
    """从 trades 列表（已全量读入内存的那份）取最近 n 笔已平仓，配对 open。

    **模块级、纯函数、不碰文件系统** —— server.py 的 _build_payload_uncached
    已经在开头 `state.load(rt+"/trades.json", [])` 把文件读完了，直接把那份
    list 传进来即可，**不引入任何新的 IO**（/api/state 必须 0.1s 内返回，
    绝不能再加一次磁盘读）。agents/decision.py 自己的 _brief() 则走
    _read_trades_tail 的seek 尾部读（那条路径没有现成的 list 可用）。

    配对规则（10-05 任务 E升级）：**优先用 `trade_id` 精确配对** —— close
    记录由 engine.py 写、没有 trade_id，但带 `t_at_open`，用它反查
    `f"{sym}|{side}|{int(t_at_open*1000)}"` 就能拿到唯一那笔 open。
    只有老数据没有 t_at_open 时，才退回"同 sym|side、t ≤ close.t 中最近的
    那笔 open"（同品种反复开平时按就近配对仍可能错位，所以是次选路径）。
    返回格式化好的字符串列表（t 倒序，newest first）。
    """
    trades = list(trades or [])
    opens_by_tid = {str(o["trade_id"]): o for o in trades
                    if o.get("action") == "open" and o.get("trade_id")}
    opens = [o for o in trades if o.get("action") == "open"]
    closes = [t for t in trades if t.get("action") == "close"
              and t.get("pnl") is not None]
    closes.sort(key=lambda t: float(t.get("t") or 0), reverse=True)
    out = []
    for c in closes[:max(0, int(n))]:
        o = None
        t_open = c.get("t_at_open")
        if t_open:
            o = opens_by_tid.get(
                f"{c.get('sym')}|{c.get('side')}|{int(float(t_open) * 1000)}")
        if o is None:                      # 老数据回退：按时间就近
            ct = float(c.get("t") or 0)
            bt = -1.0
            for cand in opens:
                if (cand.get("sym") != c.get("sym")
                        or cand.get("side") != c.get("side")):
                    continue
                ot = float(cand.get("t") or 0)
                if ot <= ct and ot > bt:
                    o, bt = cand, ot
        out.append(format_trade_review({"close": c, "open": o or {}}))
    return out


def _band_tag(bp) -> str:
    """band_pos → 位置白话标签。None/非法值返回空串（调用方据此省略该片段）。"""
    try:
        v = float(bp)
    except (TypeError, ValueError):
        return ""
    if v < 0.2:
        return f"贴下轨({v:.2f})"
    if v > 0.8:
        return f"贴上轨({v:.2f})"
    if v < 0.5:
        return f"中轨下方({v:.2f})"
    if v > 0.5:
        return f"中轨上方({v:.2f})"
    return f"正中轨({v:.2f})"


# hint（strategy.regime.setup_hint）→ 大白话。
# 为什么要代码翻译：10-08 15:29 实测，模型把 8 个 band_fade_long（触下轨后**已收回带内**）
# 的品种集体描述成"价格贴着下轨滑行还没收回中轨"——与同一行里的 band_pos=0.39~0.70 直接
# 矛盾。原始字段 hint=band_fade_long 对模型不直观，故这里给成品句，杜绝自创叙事。
#
# ⚠ 10-08 16:2x 修订（初版措辞有缺陷，两处都改掉）：
#   ① 初版把 trend_cont 翻成"贴轨滑行中"＝**过度声称**。查 strategy/regime.py::_setup_hint，
#      trend_cont 只表示"最后一根 bar 既没下探回收也没上冲回落，且 regime 是趋势"，
#      **根本不含"贴轨"**。结果 ETH 跑到中轨(band_pos=0.44)也被标"贴轨滑行中"，
#      等于在"代码算好的事实句"里塞了个不准的事实，模型顺着它就写"全池贴下轨滑行"。
#   ② 初版用"反抽素材"，"素材"与提示词【形态素材】(deep／wm·legs.ok／seq·fake≥2) 的
#      严格定义撞车 —— 模型会拿它当**合法开仓依据**（ZEC 已中招：m1h_mom=-4.49 仍开多）。
#   现版本只保留**代码真能担保**的事实，不暗示方向、不暗示"素材合法性"。
_HINT_READ = {
    "band_fade_long": "触下轨后已收回带内（已收回，非下跌延续）",
    "band_fade_short": "触上轨后已收回带内（已收回，非上涨延续）",
    "trend_cont": "趋势延续中（本根未碰轨）",
    "range_inside": "带内运行（未碰轨）",
}


def _read_line(m: dict) -> str:
    """把位置/触轨/中轨斜率/4H动能压成一句**代码算好的事实句**（brief 里 key=read）。

    10-08 用户令：不让模型自己解读 band_pos/hint，代码给成品白话。
    ⚠ 纯信息字段，不参与任何开仓/拒开/仓位判定（用户铁规「判断权 100% 在模型」）。
    """
    parts = []
    tag = _band_tag(m.get("band_pos"))
    if tag:
        parts.append("位置" + tag)
    h = _HINT_READ.get(str(m.get("hint") or ""))
    if h:
        parts.append(h)
    try:
        v = float(m.get("mid_slope_atr"))
        parts.append("中轨上斜" if v > 0.1 else ("中轨下斜" if v < -0.1 else "中轨走平"))
    except (TypeError, ValueError):
        pass
    mom = m.get("m1h_mom")
    if mom is not None:
        try:
            f = float(mom)
            parts.append("4H动能%+.2f%%（%s）" % (
                f, "偏多" if f > 0 else ("偏空" if f < 0 else "持平")))
        except (TypeError, ValueError):
            pass
    return "·".join(parts)


def _trim(path: str, keep_lines: int):
    """决策流水滚动保留（只追加的文件不能无限涨）。

    10-08 实测 `runtime/llm_decisions.md` 已 1.0MB / 4848 行且**从来没有裁剪过**；
    本轮又给每轮加了 scan（逐品种判定，约 +400B/轮），不裁的话一天涨 ~0.6MB。
    """
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > keep_lines:
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(lines[-keep_lines:])
    except Exception:
        pass


def format_trade_review(pair: dict) -> str:
    """把一对 open/close 流水渲染成一行「事实回顾」。server.py 看板也用它。

    示例：
      10-04 17:50 LINK多 赚+0.066U｜当时：1H动能+0.64%、贴上轨(0.79)、
      模型理由"紧贴下轨做多"｜结果：5.6小时后止盈落袋

    纪律（用户铁规「判断权 100% 在模型」）：**只给事实与结果，不加评价**。
    不写"这笔做得好""下次注意"——解读权归模型。这里唯一带的主观词是
    出场方式的客观描述（止盈/止损），那来自 engine.py 落盘的 reason 字段。

    老数据缺字段时优雅降级：pnl 恒在（配对入选条件），其余（动能/位置/
    理由/持仓时长）缺失就整段省略，不显示 None/占位符。
    """
    c = pair.get("close") or {}
    o = pair.get("open") or {}
    try:
        ts = float(c.get("t") or o.get("t") or 0)
        stamp = datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts else "??"
    except Exception:
        stamp = "??"
    sym = str(c.get("sym") or o.get("sym") or "?").replace("_USDT", "")
    side = "多" if str(c.get("side") or o.get("side")) == "long" else "空"
    pnl = float(c.get("pnl") or 0)
    head = f"{stamp} {sym}{side} {'赚' if pnl > 0 else ('亏' if pnl < 0 else '平')} {pnl:+.3f}U"
    # ---- 当时：模型看到什么、怎么想的 ----
    then = []
    mom = o.get("m1h_mom")
    if mom is not None:
        try:
            then.append(f"1H动能{float(mom):+.2f}%")
        except (TypeError, ValueError):
            pass
    tag = _band_tag(o.get("band_pos"))
    if tag:
        then.append(tag)
    if o.get("regime"):
        then.append(f"行情{str(o['regime'])}")
    why = str(o.get("reason") or "").strip()
    if why:
        then.append(f"模型理由“{why[:40]}”")
    # ---- 结果：客观事实 ----
    tail = []
    hold = c.get("hold_min")
    src = str(c.get("reason") or "")
    if hold is not None:
        try:
            h = float(hold)
            tail.append(f"{h / 60:.1f}小时后" if h >= 60 else f"{h:.0f}分钟后")
        except (TypeError, ValueError):
            pass
    if "SL/TP" in src:
        tail.append("SL/TP 条件单打掉" if pnl < 0 else "SL/TP 条件单落袋")
    elif "App" in src:
        tail.append("手动平仓" if pnl < 0 else "手动平仓落袋")
    elif "引擎" in src:
        # engine.py 的 reason 是「平仓（引擎执行）」= 模型主动建议平/guard 止盈，
        # 引擎侧执行市价单。盈亏方向已由 pnl 给出，这里不再重复"（亏损）"。
        tail.append("引擎执行平仓")
    elif src:
        tail.append(src)
    else:
        tail.append("盈利落袋" if pnl > 0 else "亏损离场")
    mid = "｜当时：" + "、".join(then) + "｜" if then else "｜"
    return f"{head}{mid}结果：" + " ".join(tail)


def _pct_txt(v) -> str:
    """把小数比例写成提示词里的百分数字面量（0.010 → "1.0"、0.012 → "1.2"）。

    为什么不让提示词硬编码：SL/TP 口径改过一次就漂移一次——10-06 改 SL 0.8→1.0%、
    TP 0.8→1.2% 后，提示词里那五行仍写「止损 0.8% / 止盈 1.5%」，模型拿到的是
    **两个都已作废的数字**。改为从 `exit.sl_min_pct` / `exit.tp_fixed_pct` 现渲染。
    """
    s = f"{float(v) * 100:.2f}".rstrip("0")
    return s + "0" if s.endswith(".") else s


def _universe_clause(universe: list) -> str:
    """把交易池清单注入提示词（10-04 16:1x，根治模型幻觉品种）。

    背景：原SYSTEM_PROMPT 只说「全池 15 个品种」却从未列出是哪15 个，
    brief 里的标注又只含当轮扫到的品种 → 模型只能靠猜。实测 51 次输出
    池外品种（HYPER_USDT 46 次，还有 SNK/SKHYNX/HYPERliquid/HYPERUSDT 等
    错拼），HYPE_USDT 被稳定写成 HYPER。代码侧 `_try_open` 拦下不成交，
    属安全侧，但每轮白烧一次决策。

    清单**从 configs/config.yaml 的universe 动态生成**，加/退品种自动同步，
    不写在提示词里就不会和配置漂移。注意必须逐字照抄、不要改写或"纠正"
    拼写（历史上正是模型把 HYPE_USDT 自行"修正"成 HYPER_USDT）。
    """
    syms = list(universe or [])
    if not syms:
        return ""
    return (
        "\n【可交易品种白名单｜唯一合法取值】contract 字段只能从下面这 "
        f"{len(syms)} 个里选，**逐字照抄、一个字都不能改**：\n"
        + "、".join(syms)
        + "\n（报出不在上面这份清单里的 contract = 无效输出，那一轮的开仓会被"
          "直接丢弃、整轮白跑。别自行合并/拆分/改写后缀：池里是 HYPE_USDT 就写 "
          "HYPE_USDT，写成 HYPER_USDT 之类会被判为无效。）"
    )


class HfAgent:
    """高频决策代理"""

    def __init__(self, cfg: dict, boll_sm, risk: RiskManager):
        self.cfg = cfg
        self.sm = boll_sm
        self.risk = risk
        self.state_path = cfg["runtime_dir"] + "/hf_state.json"
        self.decisions_path = cfg["runtime_dir"] + "/llm_decisions.md"
        self.trades_path = cfg["runtime_dir"] + "/trades.json"
        self.lessons_path = cfg["runtime_dir"] + "/lessons.md"
        self._sys_prompt = None    # 白名单渲染后的系统提示词（懒渲染+缓存）
        # 10-05：本轮 scan() 的结果（contract → 标注 dict 列表）。
        # 开仓落账时要打标签（m1h_mom/band_pos/regime/形态来源），标签必须
        # 是**开仓那一刻**的值 —— 事后回填没有意义（行情早变了）。
        # 之前每次 _trade_log 都现读 scan.json（一次全量 state.load），
        # 改成直接读本属性：值同源（cycle() 刚落盘），省一次磁盘读。
        self._last_scan: list = []
        # 10-05 翻仓：Engine 实例引用（由 Engine.__init__ 在构造完本对象后回填）。
        # F4 落账需要 `Engine._mutate_meta`（复用 engine.py 的 _META_LOCK 临界区，
        # 保证「删旧 key + 写新 key」不可分割）。**绝不能写 `from engine import Engine`**
        # —— engine.py 已经 import 了本模块，那样会成环。用引用回填即可。
        self._engine_ref = None
        # 本轮（cycle）已开/已翻过的 contract → side，用于 E2「also_open 自相矛盾」
        # 的日志与二次翻仓提示（10-05 用户拍板 Q-B：允许翻两次仓，但每次必须打日志）。
        self._round_seen: dict = {}

    # -------------------------------------------------- 提示词
    def system_prompt(self) -> str:
        """渲染系统提示词（{UNIVERSE} 白名单 + {SL_PCT}/{TP_PCT} 真实口径 + {PERF} 实时战绩）。

        缓存策略**故意不对称**（10-05）：
          · {UNIVERSE} 缓存 —— 内容只随 universe 变（加/退品种才需重渲染）；
          · {PERF}   不缓存 —— 战绩每笔都在变，用户要求「每次决策都重算」。
            所以这里缓存的是**只填了 UNIVERSE 的半成品**，每次调用在返回前
            现填 {PERF}。多一次 str.replace（微秒级），换来数字永远不过期。
        """
        # SL/TP 口径**现渲染**（10-06：提示词曾把 0.8%/1.5% 硬编码，改 config 后
        # 模型仍读到作废数字）→ 连缓存键一起带上，改 config 重启即同步。
        ex_cfg = self.cfg.get("exit") or {}
        sl_pct = _pct_txt(ex_cfg.get("sl_min_pct", 0.0))
        tp_pct = _pct_txt(ex_cfg.get("tp_fixed_pct", 0.0))
        key = (tuple(self.cfg.get("universe") or ()), sl_pct, tp_pct)
        if self._sys_prompt is None or self._sys_prompt[0] != key:
            body = (SYSTEM_PROMPT_TPL
                    .replace("{UNIVERSE}", _universe_clause(self.cfg.get("universe")))
                    .replace("{SL_PCT}", sl_pct)
                    .replace("{TP_PCT}", tp_pct))
            self._sys_prompt = (key, body)
            LOG(f"  📋 系统提示词已渲染：白名单 {len(key[0])} 个品种"
                f"（{', '.join(key[0][:4])}…）｜SL {sl_pct}% / TP {tp_pct}%")
        body = self._sys_prompt[1]
        if "{PERF}" in body:
            body = body.replace("{PERF}", _perf_clause(self.cfg.get("runtime_dir") or "."))
        return body

    # -------------------------------------------------- 状态
    def _st(self) -> dict:
        return state.load(self.state_path, {"open_meta": {}, "regime": "", "bias": ""})

    def _save(self, st, deleted=None):
        # 写回前并入盘上最新 open_meta（10-03 00:35 竞态修复）：
        # guard 每 2 秒整份覆盖写与 decision 开仓写互相竞争——两边各自
        # load 快照→改→整份写回，**后写的旧快照会把刚写入的新仓抹掉**。
        # 实证两例：23:49 XRP（✅开仓日志在、SL/TP 挂了，meta 6 秒窗口内
        # 被 guard 旧快照覆盖丢失）、23:16 ADA（同型+重启放大）。
        # 并集规则：本地改过的 key 以本地为准（最新），盘上独有的 key（其他
        # 线程新开的仓）保留。
        #
        # ⚠ 10-03 20:30 修（幽灵峰值 bug 的**真正根因**）：原注释写着「已删 key
        #   会被带回 → 下一轮对账自然再删，可接受」——**这个假设是错的**。对账只
        #   在持仓集合变化时才跑，同 key 平了又开（集合不变）时永远不会再删；
        #   而且被带回的旧记录会带着上一代的 peak/tp_peak/entry，等新仓顶替同一
        #   key 时被继承 → 新仓一出生就"已经赚过又回吐" → 保本兜底秒平。
        #   实测今天 open_meta 积到 23 条，而交易所只有 2 个仓。
        #   现加 deleted 参数：调用方显式声明本轮删除的 key，合并后强制剔除。
        disk = state.load(self.state_path, {})
        dm = disk.get("open_meta") or {}
        if dm:
            merged = dict(dm)
            merged.update(st.get("open_meta") or {})
            for k in (deleted or ()):
                merged.pop(k, None)
            st["open_meta"] = merged
        elif deleted:
            for k in deleted:
                st.setdefault("open_meta", {}).pop(k, None)
        state.save(self.state_path, st)

    # -------------------------------------------------- brief 构造
    def _brief(self, candidates: list):
        """拼 brief。**持仓快照不可信时返回 None**（调用方须中止本轮）。

        🔴 10-05 修（空快照灾难链，用户批准）：原来 `ex.positions()` 在
        「HTTP 200 + 空 body」时静默返回 `[]`（gate_client.py:109 的 `or {}`
        → `or []`）→ 走到 `if not shown: lines.append("持仓：无")` →
        **给模型发一份「你没有持仓」的假 brief**。
        这与 QA 抓到的 P1（假 S0 行）是**同一类认知误导**：模型会以为
        9 个仓都不存在 → 可能重复开同品种 → 又是一笔双开/双份手续费。
        所以这里不是「显示空仓」而是**整轮中止**：宁可这一轮不出手，
        也不让模型基于假持仓清单做决策。`cycle()` 见下方处理。
        ⚠ 用 `positions_strict_rows()`（保留原始行）：本函数要读
          `entry_price` / `unrealised_pnl`，`positions_strict()` 的 legs
          结构里没有这两个字段，不能直接换。
        """
        st = self._st()
        equity = ex.equity()
        self.risk.new_day(equity)
        _ok, poss, _perr = ex.positions_strict_rows()
        if not _ok:
            LOG(f"  🚫 本轮中止：持仓快照不可信（{_perr}）"
                f"——不向模型发送「持仓：无」的假 brief（那会让模型以为仓都不存在"
                f"而重复开仓），本轮不出手")
            try:
                journal.log("anomaly", kind="positions_unavailable",
                            where="brief", err=str(_perr)[:200],
                            why="brief 持仓清单不可信，本轮中止，不发假 brief")
            except Exception:
                pass
            return None
        own = set((st.get("open_meta") or {}).keys())
        # 账内持仓按**品种**归并：同一品种的所有腿放一起，供「逐品种块」与顶部摘要共用。
        # 账外仓（APP/体验仓）在这里就隔离掉 —— 不进模型视野、不占名额。
        pos_by_sym: dict = {}
        for p in poss:
            side = "long" if float(p["size"]) > 0 else "short"
            if f"{p['contract']}|{side}" not in own:
                continue
            pos_by_sym.setdefault(p["contract"], []).append((side, p))
        shown = sum(len(v) for v in pos_by_sym.values())
        # 时效闭环（10-02 用户拍板「把正确的时间正确的行情给模型」）：首行=数据时点
        lines = [f"数据时点 {datetime.now().strftime('%H:%M:%S')} | 1h布林+1m序列 | 以下标注与现价均为该时点最新值",
                 f"账户权益 {equity:.2f} USDT | 今日已做 {int(state.load(self.state_path, {}).get('trades_today', 0))} 笔"
                 f" | 账内持仓 {shown} 个"
                 + ("：" + " / ".join(f"{_sy} {_sd}"
                                      for _sy, _legs in pos_by_sym.items()
                                      for _sd, _ in _legs) if shown else "（无）")]
        if st.get("regime") or st.get("bias"):
            lines.append(f"你上轮判断：regime={st.get('regime') or '-'} bias={st.get('bias') or '-'}")
        # 教训按当前池内品种相关性排序（14:03 改）：避免"别的品种的教训"压掉本可开的单
        lessons = self._lessons([i.get("contract") for i in (candidates or [])])
        if lessons:
            lines.append("最近教训（**历史参考，不是当前指令**；本轮判断以标注为准）：")
            lines += [f"- {x}" for x in lessons]
        # 最近交易回顾（10-05 反馈闭环）：教训只写亏损、且是文字评述；
        # 这里给的是**带数字的既成事实**——最近 8 笔平仓的「当时怎么想」
        # ×「结果如何」，让模型能把自己的判断和实际结果对上。
        #纪律：只陈述事实与结果，**不加任何评价**（"做得好/该反思"这类
        # 解读是模型自己的活，代码替它下结论就等于替它思考了）。
        reviews = self._recent_reviews(8)
        if reviews:
            lines.append("最近交易回顾（**已平仓的既成事实，不是评价**）：")
            lines += [f"- {x}" for x in reviews]
        else:
            lines.append("最近交易回顾：暂无历史交易记录")
        lines.append("你的任务：过一遍下面每一个品种，挑出该开仓的（可以一个、多个、或者全不开）。")
        lines.append("每个品种自成一块，块内从上到下依次是"
                     "【行情 / 形态 / 动能 / 白话读法 / 候选 / 已有持仓】。")
        lines.append("regime=行情定性·每轮实时算（ADX+带宽口径）：up_trend·down_trend=强趋势 / "
                     "high_vol=波动释放方向未定 / range=震荡 / low_vol=轨道失参考（meaningful=false）；"
                     "读 regime 时配合 adx（趋势强度）、meaningful（轨道可否参考）。")
        # ---- 逐品种一块：把该品种的行情 / 形态 / 动能 / 候选 / 持仓排在一起 ----
        #   10-09 用户令「给模型的时候一个品种就把行情…等现在的东西列在一起，
        #   不要东一块西一块的」。
        #   旧格式（10-03~10-09）有三个毛病：
        #     ① 全池 13 个品种的标注挤在**末尾一行** `json.dumps(candidates)` 里
        #        —— 实测 11059 字符的 brief 只占 11 行，其中 9659 字符全在一行；
        #     ② 持仓散在顶部几行，与它自己的行情标注隔着一整坨 JSON；
        #     ③ 候选嵌在标注 JSON 内部，与「该品种的位置/形态」不在一处读。
        #   现在每个品种自成一个 `【品种】` 块，块内固定几行、字段名与系统提示词
        #   的【字段速查】一一对应；模型找某个品种只需看它那一块。
        #   ⚠ 保留的历史教训：**绝不能**写 `lines += json.dumps(...)` ——
        #     `list.__iadd__` 接受任意可迭代对象，字符串也是可迭代的 →
        #     `lines += "abc"` 会按字符 extend 成 ['a','b','c']。10-06 18:1x 实测：
        #     11851 字符的标注 JSON 被拆成 **9930 行单字符**，token 膨胀 3~6 倍。
        #     下面一律用 `lines.append(...)`（加 str）或 `lines += [...]`（加 list）。
        def _v(x):
            """标注值统一排版：bool→true/false（与模型输出的 JSON 同形）、
            浮点按量级保留 2~4 位去尾零、嵌套 dict（legs）展平成
            `w(l1=…, l2=…, ok=true, gap=…)`（用括号分组，避免多层 `=` 串在一起）。"""
            if isinstance(x, bool):
                return "true" if x else "false"
            if isinstance(x, dict):
                return "(" + ", ".join(f"{k}={_v(y)}" for k, y in x.items()) + ")"
            if isinstance(x, float):
                nd = 2 if abs(x) >= 100 else 3 if abs(x) >= 1 else 4
                return f"{x:.{nd}f}".rstrip("0").rstrip(".")
            return str(x)

        pool = [i for i in (candidates or []) if isinstance(i, dict)]
        for m in pool:
            sym = m.get("contract") or "?"
            lines.append("")
            lines.append(f"【{sym}】 现价 {_v(m.get('px'))}")
            parts = [f"{k} {_v(m[k])}" for k in (
                "regime", "tier", "adx", "meaningful", "hint", "band_pos",
                "mid", "up", "dn", "atr15", "bw", "bw_pct", "bw_burst",
                "bb_in_kc", "mid_slope_atr") if m.get(k) is not None]
            lines.append("  行情     " + " | ".join(parts))
            parts = [f"{k} {_v(m[k])}" for k in (
                "walk_up_bars", "walk_dn_bars", "walk_up_touch", "walk_dn_touch",
                "close_out_up_bars", "close_out_dn_bars",
                "three_push_up", "three_push_dn", "fake_up_n", "fake_dn_n",
                "last_touch", "touch_mid", "bars_since_touch", "legs")
                if m.get(k) is not None]
            if parts:
                lines.append("  形态     " + " | ".join(parts))
            parts = [f"{k} {_v(m[k])}" for k in (
                "m1h_mom", "rsi15", "macd_dif", "macd_dea", "macd_hist")
                if m.get(k) is not None]
            if parts:
                lines.append("  动能     " + " | ".join(parts))
            if m.get("m1") is not None:
                lines.append("  1m 序列  m1 " + str(m["m1"]))
            if m.get("read"):
                lines.append(f"  白话读法 {m['read']}")
            c = m.get("candidate")
            if isinstance(c, dict):
                cp = [f"{k} {_v(c[k])}" for k in (
                    "side", "level", "deep", "seq", "wm", "confirm_ready",
                    "first", "cand_t", "age_bars") if c.get(k) is not None]
                lines.append("  候选     " + " | ".join(cp))
            else:
                lines.append("  候选     无（本轮没扫出机器候选；仍可按标注自行判断）")
            for _sd, _p in pos_by_sym.get(sym, []):
                lines.append(
                    f"  已有持仓 {_sd} {abs(float(_p['size'])):g}张"
                    f" | 入场 {_v(_p.get('entry_price'))}"
                    f" | 浮盈 {float(_p.get('unrealised_pnl') or 0):+.3f}U [高频仓]")
        # 账内有仓、但不在本轮标注池里的品种（universe 改过 / 拉数据失败）：
        #   也要让模型看见，否则它会以为这个品种没持仓。
        for sym, plist in pos_by_sym.items():
            if any(m.get("contract") == sym for m in pool):
                continue
            lines.append("")
            lines.append(f"【{sym}】 现价 ?")
            lines.append("  行情     不可用（该品种不在本轮标注池内）")
            for _sd, _p in plist:
                lines.append(
                    f"  已有持仓 {_sd} {abs(float(_p['size'])):g}张"
                    f" | 入场 {_v(_p.get('entry_price'))}"
                    f" | 浮盈 {float(_p.get('unrealised_pnl') or 0):+.3f}U [高频仓]")
        return "\n".join(lines)

    def _lessons(self, syms: list = None) -> list:
        """取最近教训（10-03 14:03 按品种相关性；10-05 21:2x 适配聚合行）。

        两类行要分开处理（教训文件里现在混着两种格式）：

        ① **老格式** `[时间] SYMBOL side pnlU → {...}`（10-05 之前写的，
           已被 reset_lessons 清空，只可能出现在备份里）——按品种相关性
           排序：同品种 > 同方向 > 其他。这套逻辑对老格式有效，因为一行
           就是**一个品种**的事。

        ② **新格式** `[聚合] 标签组合 | 战绩 | 时间 SYMBOL side pnlU → 正文`
           ——**不做品种相关性排序，只按时间倒序**。原因：聚合行的去重键是
           **标签组合**（mom_dir×pos_zone×regime）而不是品种，它天生是
           **跨品种**的汇总；但行尾仍带着**最后一次写入的那个品种名**
           （`_upsert_agg` 会写 `tr["sym"]`）。若继续按品种相关性排，一条
           "顺动能×贴下轨" 的跨品种教训会因为碰巧提到 LINK 而被判成
           "同品种"顶到最前，把真正该看的其它组合挤下去——实测 3 条聚合行
           的顺序被完全打乱。所以聚合行一律走时间倒序。

        另外必须**过滤掉文件头说明行**（`（只记录亏损单…`）：它是给人和
        看板看的口径说明，不是教训 它会被当成一条教训拼进
        brief，模型会看到一句莫名其妙的括号说明。
        """
        try:
            with open(self.lessons_path, encoding="utf-8") as f:
                all_lines = [x.strip() for x in f.readlines() if x.strip()]
        except Exception:
            return []
        # 过滤文件头口径说明行（以全角括号「（」开头）——它不是教训。
        all_lines = [x for x in all_lines if not x.startswith("（")]
        max_n = int(self.cfg["llm"]["max_lessons"])
        recent = all_lines[-max_n * 4:] if syms else all_lines[-max_n:]
        if not syms:
            return list(reversed(recent))[-max_n:]
        want = {s.replace("_USDT", "") for s in syms}
        # 教训行格式："- [时间] SYMBOL side pnl → {...}"
        scored = []
        for i, ln in enumerate(recent):
            if ln.startswith("[聚合]"):
                # 聚合行：不参与品种相关性排序，权重恒为 0（靠 i 保持时间序）
                scored.append((0, i, ln))
                continue
            up = 1
            if any(w in ln for w in want):
                up = 3                       # 同品种
            elif any(s in ln for s in ("long", "short", "多", "空")):
                up = 2                       # 含方向词
            scored.append((up, i, ln))
        # 稳定排序：相关性降序，同级保原序（新的优先）。
        # 聚合行权重 0 → 恒排在老格式之后，且彼此之间保持文件里的时间序
        # （recent 是文件顺序 = 写入顺序，越靠后越新）。
        scored.sort(key=lambda t: (-t[0], -t[1]))
        return [ln for _, _, ln in scored[:max_n]]

    def _recent_trades(self, n: int = 8) -> list:
        """取最近 n 笔**已平仓**交易（t倒序 newest first），供 brief 使用。

        走 seek 尾部读（见 _read_trades_tail）：brief 每 60s 一次，不能全量
        读 135KB+ 且在增长的文件。配对与格式化复用模块级
        build_recent_reviews / format_trade_review —— 与 server.py 看板
        同一份代码，杜绝两处口径漂移。
        """
        return build_recent_reviews(_read_trades_tail(self.trades_path, 65536), n)

    def _recent_reviews(self, n: int = 8) -> list:
        """brief 里的「最近交易回顾」文字行。"""
        return self._recent_trades(n)

    # -------------------------------------------------- 候选扫描
    def _fetch_all(self, syms: list, boll_cfg: dict) -> dict:
        """**并发**拉全池网络数据，返回 {sym: (bars, px, mom, m1)}。任何异常都收敛成 None。

        为什么要并发（10-04 性能修复）：
          改造前 scan() 串行拉 15 品种 ×(15m K线 + 1H K线 + live_price) ≈ 45 次
          HTTP，实测 9~15s。hf 周期 60s 虽扛得住，但这段串行阻塞会挤占同进程
          guard 守护线程的调度（guard 2s 一轮、要求低延迟），属于"隐性拖累"。

        线程安全（三条铁律，缺一不可）：
          ① **只并发纯网络 IO**。BollStateMachine.feed/snapshot_band/annotations
             一律留在主线程**串行**跑——`sm.cand` 是跨品种共享的读写视图
             （`BollStateMachine.cand / cands`），并发调用会让 15 个品种的 feed 交错、
             互相清空候选（与 10-03 01:15 修过的"全池单槽 cand"同型病灶）。
          ② **工作线程绝不写共享状态**。子线程只return 数据，全部结果先收进
             本地 dict，主线程再单线程取用；`m` 标注字典只在主线程里建。
          ③ **feeder 的缓存是模块级 dict**（`_cache`），并发写理论上有竞态。
             但 CPython dict 的 `__setitem__` 是单字节码原子操作，且这里
             写入的 value 是不可变的完整 bars 列表、最多出现"重复请求一次"
             而不会写出半截数据（不会损坏）。真正的风险是同一 key 被两个线程
             同时 miss 后重复拉取——代价仅是一次冗余 HTTP，可接受。
        """
        n_bars = int(boll_cfg["n"]) + 25
        tf_min = int(boll_cfg["hf_tf_min"])

        def _one(sym: str):
            """子线程：只做四件事——拉策略周期 K线、拉 1H 动能、拉 1m 序列、拉现价。
            不碰任何共享状态。

            ⚠️ `_m1h_mom` / `_m1_snap` 内部只读 feeder 行情 + 写各自的模块级缓存
               （`_MOM_CACHE` / `_M1_CACHE`）；dict 写入是原子操作，且这里每个
               sym 只由**一个**线程调用一次（pool.map 一对一），不存在同 key 竞态。
            """
            try:
                bars = feeder.closed_bars(sym, tf_min, days=7, n=n_bars)
            except Exception:
                bars = None
            try:
                mom = _m1h_mom(sym)
            except Exception:
                mom = None
            try:
                m1 = _m1_snap(sym)
            except Exception:
                m1 = None
            try:
                px = feeder.live_price(sym)
            except Exception:
                px = 0.0
            return sym, bars, px, mom, m1

        got: dict = {}
        with ThreadPoolExecutor(max_workers=SCAN_FETCH_WORKERS,
                                thread_name_prefix="scan_fetch") as pool:
            # 保持universe 原序返回，确保 brief 里品种顺序稳定（模型逐轮对照更省心）
            for sym, bars, px, mom, m1 in pool.map(_one, syms):
                got[sym] = (bars, px, mom, m1)
        return got

    def scan(self) -> list:
        """扫全池布林状态机，产出候选+标注列表"""
        boll_cfg = self.cfg["boll"]
        syms = list(self.cfg["universe"])
        # 上轨空单候选压制（10-07 用户拍板；依据见 configs/config.yaml 同段备注）：
        #   实测候选空单 4 天 460 笔期望 −0.061%、胜率 40.2%（大盘同期 −0.74% 的下跌市里
        #   依然亏 → 是负边际而非被漂移拖累）；实盘同向（short cand_* 4 笔 −0.51U）。
        # ⚠ 这里只**不把候选挂给模型**，状态机内部照旧维护候选 → 不改变 D/E 路径
        #   `if not self.cand` 的传播、不动 annotations 任何字段。模型仍可用
        #   fake_up_n≥2 / legs.ok / deep 自行点名做空（提示词【形态素材】口径不变），
        #   所以这是"证据瘦身"而不是决策护栏。
        _sup_short = bool(boll_cfg.get("suppress_short_cand", False))
        # ---- 阶段1：并发拉网络数据（IO 密集，并发收益最大）----
        got = self._fetch_all(syms, boll_cfg)
        # ---- 阶段2：串行做状态机与标注（CPU + 共享状态，必须单线程）----
        out = []
        for sym in syms:
            bars, px, mom, m1 = got.get(sym) or (None, 0.0, None, None)
            if not bars or len(bars) < boll_cfg["n"] + 2:
                continue
            for i in range(len(bars) - 3, len(bars)):     # 断档补喂
                self.sm.feed(sym, bars[:i + 1])
            closes = [b["c"] for b in bars]
            band = self.sm.snapshot_band(sym, closes)
            m = {"contract": sym, "px": px, "mid": round(band["mid"], 6),
                 "up": round(band["up"], 6), "dn": round(band["dn"], 6),
                 "atr15": round(float(band.get("atr") or 0), 6)}
            m.update(self.sm.annotations(sym, closes, px, bars))
            # 中轨斜率（布林官方趋势判据）：中轨较上根移动的 ATR 倍数，≈0=走平(区间市前提)
            mid_pv = float(band.get("mid_pv") or band["mid"])
            m["mid_slope_atr"] = round(
                (band["mid"] - mid_pv) / max(float(band.get("atr") or 0), 1e-9), 3)
            # 行情定性（10-07 用户令改用收网 Seine 口径：ADX14 + 布林带宽 → regime/tier）：
            #   每轮实时算、随标注喂模型；只定性不拦截（12:5x 用户令「不要加护栏」）。
            #   ⚠ 与「布林候选」并存：BollStateMachine 的形态候选（cand）与上面
            #     全部布林标注字段（bw_pct/walk/close_out…）都不受影响，只是 regime
            #     这一格换了口径（旧：breakout_*/trend_*/wide/squeeze；新：seine 五态）。
            _rr = regime.classify(bars, self.cfg)
            m["regime"] = _rr.regime
            m["tier"] = _rr.tier
            m["adx"] = round(_rr.adx, 1)
            m["atr"] = round(_rr.atr, 6)
            m["bw"] = round(_rr.bandwidth, 4)
            m["hint"] = _rr.setup_hint
            m["meaningful"] = _rr.meaningful
            # 1m 短周期序列（10-06 新增·纯信息）：有值才写，拉不到就不给该字段
            #   （不写 None —— 14 品种 × 每轮多 8 个字符纯属浪费；缺字段模型按
            #   提示词口径忽略即可）
            if m1:
                m["m1"] = m1
            # ============ 10-04 追加三个**纯信息**字段（不拦截、不参与任何判定）============
            # 用户铁规「判断权 100% 在模型」：以下三项只进brief 供模型阅读，
            # **不写进任何拒开/校验路径**（拒开原因、风控参数一律不含它们）。
            m.update(self._osc(closes))
            # mom 已在阶段1 并发拉好（拉不到数据时为 None，优雅降级不崩 scan）
            m["m1h_mom"] = round(mom, 3) if mom is not None else None
            # 10-08 用户令（B）：位置/触轨/中轨斜率/4H动能压成一句代码算好的白话事实
            #   挂在 key=read，模型以它为准，不必自己解读原始字段（见 _read_line）。
            m["read"] = _read_line(m)
            cand = self.sm.cand
            if cand and cand.get("sym") == sym and px > 0 \
                    and not (_sup_short and cand.get("side") == "short"):
                cand["confirm_ready"] = (px > band["mid"] and band["mid"] >= band.get("mid_pv", band["mid"])) \
                    if cand["side"] == "long" else \
                    (px < band["mid"] and band["mid"] <= band.get("mid_pv", band["mid"]))
                c = {k: cand.get(k) for k in
                     ("side", "level", "first", "deep", "seq", "wm", "confirm_ready")}
                # 候选时效信息（10-02 时效闭环·信息版）：诞生时间+已存在根数+失效位 level，
                # 模型开仓前自检死活——代码不弃单（用户明令不加护栏）
                tf_ms = boll_cfg["hf_tf_min"] * 60 * 1000
                c["cand_t"] = datetime.fromtimestamp(cand["bar_t"] / 1000).strftime("%H:%M") \
                    if cand.get("bar_t") else ""
                c["age_bars"] = int(max(0, bars[-1]["t"] - cand.get("bar_t", bars[-1]["t"])) // tf_ms)
                m["candidate"] = c
            out.append(m)
        return out

    @staticmethod
    def _osc(closes: list) -> dict:
        """算当前周期振荡指标标注（RSI14 + MACD 12/26/9），**纯信息字段**。

        返回 {"rsi15": float|None, "macd_dif": float|None,
               "macd_dea": float|None, "macd_hist": float|None}。
        MACD 三个值**按 close 归一化成百分比**（dif/close*100）——加密价格
        量级差极大（BTC 6e4、DOGE 0.1），不归一无法跨品种比较，也没法让
        模型把"动能量级"和价格区分开。
        数据不足时对应项给 None（不猜、不填 0，0 会被模型误读成"动能恰好为0"）。
        """
        blank = {"rsi15": None, "macd_dif": None,
                 "macd_dea": None, "macd_hist": None}
        if not closes:
            return blank
        out = dict(blank)
        try:
            out["rsi15"] = round(float(rsi(closes, 14)), 1)
        except Exception:
            pass
        # macd() 需要 slow+sig=35 根才给真值，不足时它返回 (0,0,0)——
        # 0 会被当成"动能为零"的真信号，故这里自己先卡长度。
        if len(closes) >= 26 + 9:
            try:
                dif, dea, hist = macd(closes, 12, 26, 9)
                base = float(closes[-1])
                if base > 0:
                    out["macd_dif"] = round(dif / base * 100, 3)
                    out["macd_dea"] = round(dea / base * 100, 3)
                    out["macd_hist"] = round(hist / base * 100, 3)
            except Exception:
                pass
        return out

    # -------------------------------------------------- 主流程
    def cycle(self):
        cfg = self.cfg
        candidates = self.scan()
        state.save(cfg["runtime_dir"] + "/scan.json",
                   {"t": time.time(), "items": candidates})
        # 缓存本轮扫描结果：_try_open 打标签时要用（值与刚落盘的 scan.json 同源）
        self._last_scan = candidates
        brief = self._brief(candidates)
        if brief is None:
            # 🔴 持仓快照不可信 → 整轮中止（不调模型、不执行任何决策）。
            # 绝不拿「持仓：无」的假 brief 去问模型 —— 见 _brief 的 docstring。
            return
        messages = [{"role": "system", "content": self.system_prompt()},
                    {"role": "user", "content": brief}]
        raw = llm.ask(messages, temp=float(cfg["llm"]["temperature"]),
                      max_tokens=int(cfg["llm"]["max_tokens"]),
                      timeout=int(cfg["llm"]["timeout_s"]))
        d = llm.parse_json(raw)
        self._record(d, candidates)
        if not d:
            LOG("  ⚠ 模型无有效输出，本轮跳过")
            return
        action = (d.get("action") or "hold").lower()
        st = self._st()
        st["regime"] = d.get("regime") or st.get("regime") or ""
        st["bias"] = d.get("bias") or st.get("bias") or ""
        self._save(st)
        if action == "open":
            # 本轮已处理集合（E2/Q-B：同 contract 出现第二个方向 → 打日志、两次都执行）
            self._round_seen = {}
            self._try_open(d)
            # 同时开多个（10-02 21:17 用户拍板）：主字段开完后，按 also_open 依次开。
            # 每个都独立过一次_try_open（含 win_rate 闸门/同向仓/仓位数），
            # 任一被拒不影响其余；只有 action=open 时才处理（hold/close 语义不变）。
            for extra in (d.get("also_open") or [])[:2]:
                if not isinstance(extra, dict):
                    continue
                sub = dict(extra)
                sub["action"] = "open"
                sub.setdefault("regime", d.get("regime") or "")
                sub.setdefault("bias", d.get("bias") or "")
                # ⚠ 本轮 also_open 自相矛盾（PRD E2 / 用户 10-05 Q-B 拍板）：
                #   同contract 先给一个方向又给反向 = **双开的真实来源**。
                #   用户拍板「允许翻两次仓，但每次必须打日志」——不丢弃第二次意图
                #   （丢弃就是替模型做决定 = 决策护栏），只把矛盾事实打出来便于
                #   事后统计抖动频率。两次都照常走 _try_open。
                _c = (d.get("contract") or "").strip()
                _s = (d.get("side") or "").lower()
                _c2 = (extra.get("contract") or "").strip()
                _s2 = (extra.get("side") or "").lower()
                if _c2 and _c2 == _c and _s2 and _s2 != _s:
                    LOG(f"  ⚠ 本轮 also_open 自相矛盾：{_c2} 主字段给{_s}、"
                        f"also_open 又给{_s2} —— 两次都执行（用户 10-05 拍板，"
                        f"仅记录抖动频率，不丢弃第二次意图）")
                    try:
                        journal.log("anomaly", kind="also_open_conflict",
                                    sym=_c2, main_side=_s, also_side=_s2,
                                    why="同轮同contract 两个方向，按用户拍板两次都执行")
                    except Exception:
                        pass
                self._try_open(sub)
        elif action == "close":
            # 10-07 起平仓权归「持仓管理」线程（agents/exit_agent.py，只持仓品种）。
            # 本处保留为**安全空转**：万一模型仍输出 close，只记录、绝不执行
            # （执行权单点归属，避免两处都能平导致重复平仓）。
            LOG(f"  💡 本轮模型给了 close（本环节无平仓权，只记录）: "
                f"{d.get('contract')} {d.get('side')} | {str(d.get('reason'))[:80]}"
                f" → 平仓由持仓管理环节负责")

    # -------------------------------------------------- 开仓
    # 用户 08:00 铁令：开仓只能由模型做（品种+方向）；08:14 追加：模型拍板谁也不拦
    def _try_open(self, d: dict):
        cfg = self.cfg
        sym = d.get("contract")
        side = (d.get("side") or "").lower()
        if sym not in cfg["universe"] or side not in ("long", "short"):
            LOG(f"  ❌ 模型给的品种/方向无效: {sym} {side}")
            return
        # 亏后冷却（10-02 17:0x 上线，**20:02 用户拍板删除**）：同 sym|side 有亏损
        # 平仓记录后 N 分钟内拒开同向新单；反向与其它品种不受影响。
        # 数据在 runtime/cooldown.json（engine 落账时写，guard 不会覆盖它）。
        #关闭判据读配置（loss_cooldown_min=0 即停用）——不依赖清空存量文件，
        # 避免"配置关了但旧记录还在拒开"的幽灵拦截（20:02 踩过）。
        cool_min = float((cfg.get("guards") or {}).get("loss_cooldown_min", 0))
        if cool_min > 0:
            cpath = (cfg["runtime_dir"] or "") + "/cooldown.json"
            cd = (state.load(cpath, {}) or {}).get(f"{sym}|{side}")
            if cd and time.time() < float(cd):
                left = (float(cd) - time.time()) / 60
                LOG(f"  🚫 拒开 {sym} {side}：亏后冷却中（还剩 {left:.0f} 分钟，"
                    f"至 {time.strftime('%H:%M', time.localtime(float(cd)))}）")
                return
        # ---- 冻结闸（E1）：翻仓/回滚窗口期内，同 contract 的新信号直接丢弃 ----
        # 事务不可重入：不排队、不打断。粒度 = contract，其它品种零影响。
        if flip.is_frozen(sym):
            LOG(f"  ⏸ 丢弃本轮 {sym} {side}：该品种正处于翻仓/回滚窗口期"
                f"（冻结剩 {flip.freeze_left(sym):.0f}s，事务不可重入，不排队不打断）")
            return
        closes = None
        try:
            bars = feeder.closed_bars(sym, cfg["boll"]["hf_tf_min"],
                                      days=7, n=cfg["boll"]["n"] + 25)
            closes = [b["c"] for b in bars]
        except Exception:
            pass
        if not closes:
            return
        band = self.sm.snapshot_band(sym, closes)
        # ---- 账内腿快照（H4/A11 + 单仓判定）----
        legs = self._leg_snapshot()
        if legs is None:
            return                      # positions_strict 不可信 → 本轮中止
        opp = "short" if side == "long" else "long"
        leg = legs.get(sym) or {}
        cur_side = float(leg.get(side) or 0)        # 同向腿张数
        opp_side = float(leg.get(opp) or 0)         # 反向腿张数（← 事故的漏判点）
        # ---- S2 遗留双开：放行 + 告警 + 审计（10-05 用户改判 Q-C）----
        # 架构文档原本写「S2 拒绝新开仓」，**用户已改判**：该 contract 不拒开，
        # 只打告警 + 记入双开审计，由用户自己尽快手动清理多余的腿（上线时**不自动
        # 收敛存量双开** —— 自动平仓等于代码替用户决定持有什么，动用真金白银）。
        # 注意「放行」不等于「在双开之上再叠一笔」：S2 同样走下面的翻仓事务，
        # 把模型所选方向的反向腿收敛掉 —— 方向仍是模型给的，代码只是把同contract
        # 的仓位收敛成一条腿。
        if cur_side > 0 and opp_side > 0:
            rec = flip.audit_dual(
                sym, {side: cur_side, opp: opp_side},
                meta_keys=[k for k in (self._st().get("open_meta") or {})
                           if k.startswith(sym + "|")],
                source="hf_open",
                note=(f"模型给{side}；该contract 已存在两腿"
                      f"（{side} {cur_side:g}张 / {opp} {opp_side:g}张）"))
            LOG(f"  🚨🚨 遗留双开 {sym}：{side} {cur_side:g}张 + "
                f"{opp} {opp_side:g}张 并存（S2）—— 按用户拍板**放行不拒开**，"
                f"记入双开审计，请尽快手动清理多余腿")
            try:
                journal.log("anomaly", kind="legacy_both", sym=sym,
                            model_side=side, model_size=cur_side,
                            other_side=opp, other_size=opp_side,
                            t=rec["t"],
                            why="S2 遗留双开：放行 + 告警 + 审计，不自动收敛")
            except Exception:
                pass
        # ---- 三态分支（10-05 修复漏判）----
        # 旧逻辑只看 `key = f"{sym}|{side}"` 在不在 → **反向的 sym|opposite 根本不进
        # 判断，直接开新仓**。这正是 10-05 SNDK/HYPE 双开的根因（双向持仓模式下
        # 交易所允许同 contract 多空并存）。
        if opp_side > 0:
            # ★ 反向腿存在 → 翻仓事务（先开新 → 挂新保护 → 平旧 → 落账）。
            #   代码在这里没有说一句「这个方向不对」，它说的是「这个 contract 已经有
            #   反向的仓了，我得先把它清掉，才能开你给的方向」。翻不翻由模型的
            #   `side` 决定，代码不参与。
            #
            # ---- 翻仓节流（10-06 用户拍板方案 A：**执行层节流，不是决策护栏**）----
            # 决策权 100% 仍在模型（下一轮照样能改主意），代码只在下单前拦两类坏形态：
            #   闸1 `min_interval_s`：距旧腿开仓不足 N 秒就反手 → 「2~3 分钟打脸」。
            #   闸2 `max_chase_pct`：反手价已越过旧腿入场价 N%（做多向上 / 做空向下）
            #        → 「越翻越追高」，接刀接在半山腰。
            # 两键默认 0 = 该闸关闭（逐字节回到加闸前的行为，可一键回退）。
            # 数据背景（journal 10-06）：当日 55 笔开仓中 43 笔 sig=flip；翻仓方向命中
            #   1/9=11%（比随机还差）；HYPE 10 笔里 8 次 flip，有 2~3 分钟内反手、
            #   入场价 92.636→94.267 单调追高 +1.76%。
            fcfg2 = cfg.get("flip") or {}
            min_iv = float(fcfg2.get("min_interval_s", 0) or 0)
            max_chase = float(fcfg2.get("max_chase_pct", 0) or 0)
            if min_iv > 0 or max_chase > 0:
                _om = (self._st().get("open_meta") or {}).get(f"{sym}|{opp}") or {}
                o_entry = float(_om.get("entry") or 0)
                t_open = float(_om.get("t_at_open") or 0)
                skip_why = None
                if min_iv > 0 and t_open > 0:
                    age = time.time() - t_open
                    if age < min_iv:
                        skip_why = (f"距旧腿开仓仅 {age:.0f}s < "
                                    f"min_interval_s={min_iv:.0f}s")
                if skip_why is None and max_chase > 0 and o_entry > 0:
                    px_now = float(feeder.live_price(sym) or 0)
                    if px_now > 0:
                        dev = px_now / o_entry - 1.0
                        chased = (dev > max_chase) if side == "long" \
                            else (dev < -max_chase)
                        if chased:
                            skip_why = (f"反手价 {px_now:g} 较旧腿入场 {o_entry:g} "
                                        f"偏离 {dev * 100:+.2f}% > "
                                        f"max_chase_pct={max_chase * 100:.2f}%")
                if skip_why:
                    LOG(f"  ⏳ 跳过翻仓 {sym} {opp}→{side}：{skip_why}"
                        f"（执行层节流，决策权仍属模型，下一轮可重试）")
                    try:
                        journal.log("reject", kind="flip_throttled", sym=sym,
                                    new_side=side, old_side=opp,
                                    min_interval_s=min_iv,
                                    max_chase_pct=max_chase,
                                    old_entry=o_entry, why=skip_why[:200])
                    except Exception:
                        pass
                    return
            self._flip(sym, side, opp, int(opp_side), band, d, legs)
            return
        if cur_side > 0:
            # ---- 同向已有仓 = 走「金字塔加仓」判定（10-02 19:37 用户拍板上线）----
            # 旧逻辑一律拒开，导致 18:27 ADA 28 秒内同向开两笔（且第二笔在亏损中加码，
            # 名义 415% 权益）。业界规则：只在满足加仓条件时放行，否则拒开。
            self._try_pyramid_add(sym, side, band, f"{sym}|{side}")
            return
        # ---- 无腿 = 首仓，走完整 check_open 风控 ----
        order, why = self.risk.check_open(
            sym, side, float(d.get("win_rate") or 0), band,
            ex.equity(), hf_count=len(legs),
            same_sym_side_pos=False,
            px=feeder.live_price(sym))
        if not order:
            LOG(f"  🚫拒开 {sym} {side}：{why}")
            return
        # 信号标签：以本周期 scan 结果里该品种的记录为准
        # （sm.cand 是全局单槽，末位扫描品种会顶掉前面的，15:28 修复误标 bug）
        # 优先用 self._last_scan（cycle() 刚落的内存快照，零磁盘读），
        # 为空时（单测/直接调_try_open 的场景）回退读 scan.json。
        scan_items = self._last_scan or (
            state.load(cfg["runtime_dir"] + "/scan.json", {}).get("items") or [])
        scan_item = next((it for it in scan_items if it.get("contract") == sym), None) or {}
        cand_item = scan_item.get("candidate")
        # 收敛代码闸已删（10-01 15:24 用户拍板"删掉，全放开"）：SL 自动推远下限
        # （max(0.2%, 0.3×ATR)）已兜住噪音扫损风险，squeeze 单放行，提示词铁律同步改五条
        # 14:56 回撤：首触 confirm 闸已撤销（全量回测 20 笔首触赢单全部产生于 confirm=false
        # 当轮，confirm=true 才开的 3 笔全亏——等确认=TP 空间被吃掉，RR 更差）。候选一律直开。
        px_now = float(order.get("price") or 0)
        if cand_item and cand_item.get("side") == side:
            confirmed = (px_now > band["mid"] and band["mid"] >= band.get("mid_pv", band["mid"])) \
                if side == "long" else \
                (px_now < band["mid"] and band["mid"] <= band.get("mid_pv", band["mid"]))
            sig = "cand_confirm" if confirmed else "cand_raw"
        else:
            sig = "model"
        self._round_seen[sym] = side
        self._execute(order, d, sig, scan_item)

    # -------------------------------------------------- 账内腿快照
    def _leg_snapshot(self):
        """`positions_strict()` + 按 `open_meta` 过滤账外仓 → `{sym: {side: size}}`。

        🔴 H4/A11 语义（本函数返回 None = 本轮必须中止，绝不开仓）：
        「持仓查询失败」**绝不能**被当成「没有仓」。`positions_strict` 对
        异常 / 超时 / 返回 `{}` / 返回非list 一律 `ok=False`；这里在**任何
        持仓确认之前**就return，不进入开仓路径、不发任何新开仓单。本轮不重试
        （下一轮 `cycle()` 自然重来）。

        账外仓（`sym|side` 不在 open_meta = App/体验仓手动开的）**被剔除**：
        不进单仓判定、不占名额、不进动态管理 —— 与改动前的隔离语义完全一致
        （PRD H1 / 用户 Q1 拍板：G1 不变量只约束引擎账内仓腿）。
        """
        ok, all_legs, err = ex.positions_strict()
        if not ok:
            LOG(f"  🚫 本轮中止：持仓快照不可信（{err}）"
                f"—— 绝不基于「拉不到」推断「没有仓」，不发任何新开仓单")
            try:
                journal.log("reject", kind="positions_unavailable", err=err[:200],
                            why="positions_strict ok=False，本轮中止在F1 之前")
            except Exception:
                pass
            return None
        own = set((self._st().get("open_meta") or {}).keys())
        out: dict = {}
        for c, d2 in (all_legs or {}).items():
            leg = {}
            if f"{c}|long" in own and float(d2.get("long") or 0) > 0:
                leg["long"] = float(d2["long"])
            if f"{c}|short" in own and float(d2.get("short") or 0) > 0:
                leg["short"] = float(d2["short"])
            if leg:
                out[c] = leg
        return out

    # -------------------------------------------------- 翻仓事务（核心）
    def _flip(self, sym: str, new_side: str, old_side: str, old_size: int,
              band: dict, d: dict, legs: dict):
        """反向翻仓：F1 开新 → F2 挂新保护 → F3 平旧 → F4 落账，失败整体回滚。

        🔴 **调用铁律**：`flip.begin()` 必须是本函数**第一行**、**早于任何
        `ex.*` 网络调用**。否则 F1 的 `post_only_open`那 5s 挂单窗口里，
        guard 的 SL 推进会按 text 撤单 → **误撤新腿刚挂的 SL** → 新仓裸奔
        （本次最高危点 B3，架构 T03 必查点 1）。

        ## 为什么这个流程不是决策护栏
        判据是「交易所持仓事实」（这个 contract 已经有反方向的腿了），不是行情。
        代码从没有说过「你这个方向不对」；翻不翻、翻过去之后拿什么证据，全是
        模型自己的事。**回滚后不自动重试、不记仇、不设冷却**（PRD P0-1）：
        回滚等于「本轮什么都没发生」，下一轮模型给什么方向就开什么方向。

        ## 成本（必须如实记 journal）
        成功 = 3 笔 taker 等价（开新 + 平旧 + 将来平新）；回滚 = 2 笔（开新 + 平新）。
        对比双开的 4 笔，翻仓省 1 笔，且净暴露不为 0。
        """
        fcfg = self.cfg.get("flip") or {}
        timeout_s = float(fcfg.get("timeout_s", 30) or 30)
        retry_max = int(fcfg.get("close_old_retry_max", 3) or 3)
        if not fcfg.get("enabled", True):
            # 总闸关闭 → 完全退回改动前行为（不翻仓 = 可能开双户；这是用户显式选择）
            LOG(f"  🚫 拒开 {sym} {new_side}：已有反向 {old_side} 仓，"
                f"但 flip.enabled=false（不翻仓以免双开）")
            return

        # 🔴🔴 第一行：建事务 + 写冻结（**早于任何网络调用**，见上文铁律）
        txn = flip.begin(sym, new_side, old_side, old_size, timeout_s,
                         legs_at_begin=legs)
        # 登记旧腿的平仓成因（B5：F1~F3 之间旧腿若被 SL 打掉，也要记成 flip_out）
        flip.expect_close(sym, old_side, "flip_out")
        LOG(f"  🔄 翻仓 {sym}: {old_side} {old_size}张 → {new_side}"
            f"（窗口期 {timeout_s:.0f}s，期间冻结该品种 guard 的全部退出类动作）")
        try:
            journal.log("flip", phase="begin", sym=sym, new_side=new_side,
                        old_side=old_side, old_size=old_size,
                        timeout_s=timeout_s, cost="up to 3x taker",
                        why="同contract 已有反向腿，收敛到单方向单仓")
        except Exception:
            pass

        # ================= F1开新仓 =================
        # 名额豁免：翻仓是**替换**不是新增 → hf_count 传「品种数 − 1」，
        # 否则满仓时模型想翻仓却翻不了（PRD §3.4）。
        flip.set_phase(sym, flip.PH_OPEN_NEW)
        hf_count = max(0, len(legs) - 1)
        order, why = self.risk.check_open(
            sym, new_side, float(d.get("win_rate") or 0), band,
            ex.equity(), hf_count=hf_count,
            same_sym_side_pos=False,      # 新腿对交易所尚不存在同向腿
            px=feeder.live_price(sym))
        if not order:
            # A5：拒开则**旧腿一个字节都不动**，回S1，本轮结束，不重试。
            flip.note_error(sym, why)
            flip.clear_close_cause(sym, old_side)     # 没平任何东西，别留成因登记
            flip.end(sym)
            LOG(f"  🚫 拒开 {sym} {new_side}：{why}（翻仓未开始，"
                f"{old_side} {old_size}张原封不动）")
            try:
                journal.log("reject", kind="flip_f1_rejected", sym=sym,
                            new_side=new_side, old_side=old_side, why=why[:200])
            except Exception:
                pass
            return
        self._round_seen[sym] = new_side
        self._execute(order, d, "flip", self._last_scan_item(sym))

        # F1 结果复查（A7）：以**开仓后的实际持仓**为准，方向/张数不符即中止，
        # 绝不叠加在未知态上。
        ok2, legs2, err2 = ex.positions_strict()
        if not ok2:
            flip.note_error(sym, f"F1 后持仓复查失败: {err2}")
            flip.set_phase(sym, flip.PH_ROLLBACK, err2)
            self._flip_rollback(sym, new_side, "F1 后持仓不可信", old_side, old_size)
            return
        new_now = float((legs2.get(sym) or {}).get(new_side) or 0)
        old_now = float((legs2.get(sym) or {}).get(old_side) or 0)
        if new_now <= 0:
            flip.note_error(sym, "F1 成交复查为 0 张（已按 A3 幽灵成交口径复核）")
            flip.clear_close_cause(sym, old_side)
            flip.end(sym)
            LOG(f"  ❌ 翻仓中止 {sym}：开新腿复查为 0 张 → 放弃本轮，"
                f"旧腿 {old_side} {old_now:g}张 未动")
            return
        flip.set_new_size(sym, int(new_now))
        if old_now <= 0:
            # B5：旧腿在 F1~F3 之间被交易所侧 SL 打掉 → **F3 视为成功**，继续 F4。
            LOG(f"  ✅ 翻仓 {sym}：旧腿 {old_side} 已被交易所侧打掉（剩 0 张）"
                f"→ F3 视为成功，继续落账（新腿 {new_now:g}张）")
            self._flip_commit(sym, new_side, old_side, int(new_now))
            return

        # ================= F2 挂新保护单 =================
        # `_execute` 内部已 place_tpsl(sl, tp)；这里**显式检查**返回值并按
        # B1/B2 分级告警，但**两种情况都继续 F3**（先把旧腿平掉：若此时停下，
        # 结果是「双开 + 新仓无保护」，风险敞口远大于「双开 + 旧腿已平」）。
        flip.set_phase(sym, flip.PH_PROTECT_NEW)
        _om = (self._st().get("open_meta") or {}).get(f"{sym}|{new_side}") or {}
        _sid, _tid = _om.get("sl_id"), _om.get("tp_id")
        if _sid is None and _tid is None:
            LOG(f"  🚨🚨 翻仓 {sym} {new_side}：新腿 SL 与 TP **都**没挂上！"
                f"继续 F3 平旧腿（否则停在双开且新仓完全无保护）")
            try:
                journal.log("anomaly", kind="flip_new_unprotected", sym=sym,
                            side=new_side, size=int(new_now),
                            why="F2 SL/TP 均挂失败，仍继续 F3")
            except Exception:
                pass
        elif _sid is None:
            LOG(f"  🚨 翻仓 {sym} {new_side}：新腿 SL 未挂上（TP 已挂）→ 继续 F3")
            try:
                journal.log("anomaly", kind="flip_new_sl_missing", sym=sym,
                            side=new_side, size=int(new_now), tp_id=_tid,
                            why="F2 SL 挂失败，仍继续 F3")
            except Exception:
                pass

        # ================= F3 平旧腿 =================
        flip.set_phase(sym, flip.PH_CLOSE_OLD)
        ok3, res = self._flip_close_old(sym, old_side, retry_max, timeout_s)
        if not ok3:
            flip.note_error(sym, res)
            flip.set_phase(sym, flip.PH_ROLLBACK, res)
            LOG(f"  ⚠ 平旧腿失败 {sym} {old_side}：{res} → 回滚平新腿")
            self._flip_rollback(sym, new_side, res, old_side, old_size)
            return
        # ================= F4 落账 =================
        self._flip_commit(sym, new_side, old_side, int(new_now))

    def _flip_close_old(self, sym: str, old_side: str, retry_max: int,
                        timeout_s: float) -> tuple:
        """F3：平掉旧腿。返回 `(ok, detail)`。

        🔴 **只能用 `market_close_partial(sym, old_side, remain)`**，
        **禁用 `market_close(sym)`** —— 后者的 `side=None` 分支
        （executor.py `market_close`）会**同时平掉两个方向**，正是 PRD A10
        明令禁止的误用。`market_close_partial` 强制传 side + 强制传张数，
        结构上杜绝。

        ## 拒单成败**一律重查交易所实际 size，绝不解析 `REDUCE_EXCEEDED` 文案**
        该文案二义：既可能是「真的没仓」，也可能是「有仓但被拒」。判据只有
        一个 —— 重查后该方向 size 是不是 0（C3/A7）。
        """
        last = ""
        for attempt in range(int(retry_max) + 1):
            ok, legs, err = ex.positions_strict()
            if not ok:
                return False, f"重查持仓失败: {err}"
            remain = int(float((legs.get(sym) or {}).get(old_side) or 0))
            if remain <= 0:
                return True, (f"旧腿 {old_side} 已无仓"
                              + ("（F1~F3 之间被 SL 打掉，对齐 B5）" if attempt else ""))
            if time.monotonic() > (flip.get(sym).deadline if flip.get(sym)
                                    else time.monotonic() + timeout_s):
                return False, f"窗口期 {timeout_s:.0f}s 已超期，剩 {remain} 张未平"
            if attempt > 0:
                flip.bump_retry(sym)
                LOG(f"  🔁 重试平旧腿 {sym} {old_side}（第 {attempt}/{retry_max} 次，"
                    f"按重查到的实际张数 {remain} 重发，不解析拒单文案）")
            # B4/C9：撤旧方向的条件单必须**按 auto_size 精确撤**。
            # 旧实现 `ex._cancel_price_orders(sym, "t-v2-SL")`已于 10-05 删除
            # ——它只按 text 匹配、不看方向，会把新腿的 SL 一起撤掉（B3 缺陷源）。
            ex.cancel_price_orders_side(sym, old_side)
            r = ex.market_close_partial(sym, old_side, remain, text="t-v2-flip")
            last = str(r.get("reason") or "")[:200]
            if not r.get("ok"):
                LOG(f"  ⚠ 平旧腿 {sym} {old_side} 未成 {remain}张：{last}"
                    f"（不解析文案 → 下一轮重查实际 size 判定）")
        return False, f"重试 {retry_max} 次后仍未平（最后一条拒单原文：{last or '无'}）"

    def _flip_commit(self, sym: str, new_side: str, old_side: str, new_size: int):
        """F4：删 `sym|old_side` + 写/刷 `sym|new_side`，**同一临界区**完成。

        复用 `Engine._mutate_meta`（engine.py 的 `_META_LOCK`）：`hf_state.json`
        有三个并发写入方，`state.save` 只保证单次写原子、「读-改-写」不原子，
        而「删旧 + 写新」必须不可分割，否则会出现「旧 key 已删、新 key 未写」的
        幽灵窗（换仓指纹对不上 → 新仓一出生就继承上一代的 peak → 被保本兜底秒平）。

        ⚠ 通过 `self._engine_ref` 调`Engine._mutate_meta`，
        **绝不能 `from engine import Engine`**（engine.py 已 import 本模块 → 成环）。
        """
        eng = self._engine_ref
        if eng is None:
            # 没有engine 引用（单测直接构造 HfAgent 时）→ 退化成本地 save，
            # 并如实告警，绝不假装成功。
            LOG(f"  ⚠ 翻仓 {sym}：无 engine 引用，F4 走本地 save（单测/异常路径）")
            st = self._st()
            om = st.setdefault("open_meta", {})
            om.pop(f"{sym}|{old_side}", None)
            self._save(st, deleted={f"{sym}|{old_side}"})
            flip.set_phase(sym, flip.PH_DONE)
            flip.end(sym)
            return
        new_key, old_key = f"{sym}|{new_side}", f"{sym}|{old_side}"

        def _fn(st):
            om = st.setdefault("open_meta", {})
            if new_key not in om and old_key not in om:
                return False              # 两边都不在 → 无改动，交给调用方重试
            m = dict(om.get(new_key) or {})
            if not m:
                return False              # 新腿记录不在（幽灵）→ 不盲目写
            m["size"] = int(new_size)     # 部分成交按实际张数（A2）
            om[new_key] = m
            om.pop(old_key, None)
            return True
        wrote = False
        for attempt in (1, 2):                       # F3：重试 1 次
            wrote = eng._mutate_meta(_fn, deleted={old_key})
            if wrote:
                break
            LOG(f"  ⚠ 翻仓 {sym} F4 落账第 {attempt} 次未生效（open_meta 无变更），"
                f"重试一次")
        if not wrote:
            # F3 耗尽 → S5 告警，**不动仓位**（仓位在交易所，本地账写不进去不该
            # 导致真金白银被平）。冻结 1 小时并暴露给看板。
            flip.note_error(sym, "F4 open_meta 落账失败（重试耗尽）")
            try:
                journal.log("anomaly", kind="flip_commit_failed", sym=sym,
                            new_side=new_side, old_side=old_side,
                            new_size=new_size,
                            why="F4 落账失败，仓位不动，等人工核对")
            except Exception:
                pass
            LOG(f"  🚨🚨 翻仓 {sym} F4 落账失败 → 状态存疑，"
                f"该品种 guard 冻结 1 小时，**仓位未做任何自动处理**，请人工核对")
            flip.freeze(sym, float((self.cfg.get("flip") or {}).get(
                "rollback_freeze_s", 3600)))
            flip.keep_failed(sym)
            return
        flip.set_phase(sym, flip.PH_DONE)
        txn = flip.get(sym)
        LOG(f"  ✅ 翻仓完成 {sym}: {old_side} 已平→ {new_side} {new_size}张 落账"
            f"（成本 3 笔 taker 等价：开新+平旧+将来平新）")
        try:
            journal.log("flip", phase="done", sym=sym, new_side=new_side,
                        old_side=old_side, new_size=new_size,
                        cost="3x taker",
                        elapsed_s=round(txn.elapsed_s(), 1) if txn else None)
        except Exception:
            pass
        flip.end(sym)

    def _flip_rollback(self, sym: str, new_side: str, why: str,
                       old_side: str, old_size: int):
        """D组：平新腿 → 回到「本轮什么都没发生」。

        成功（D1）→ 删 `sym|new_side`，**保留 `sym|old_side`**（旧腿从未被删过）
        → 回到 S1。失败（D2）→ S5：只报警，**禁止**自动重试/自动平一边/自动减仓。

        🔴 **P0-1 铁律**：回滚成功后**立即 return**，本轮不再有任何后续动作，
        不重试、不写 cooldown.json、不置任何阻拦标志。下一轮模型给什么方向就
        开什么方向 —— 代码不记仇、不阻拦。
        """
        flip.count_rollback()
        ok, legs, err = ex.positions_strict()
        new_now = 0
        if ok:
            new_now = int(float((legs.get(sym) or {}).get(new_side) or 0))
        else:
            LOG(f"  ⚠ 回滚前持仓复查失败 {sym}：{err}（仍按新腿实际张数尝试平）")
        flip.expect_close(sym, new_side, "flip_rollback")
        if new_now > 0:
            #撤新腿条件单同样按方向精确撤（不误伤此刻还活着的旧腿）
            ex.cancel_price_orders_side(sym, new_side)
            r = ex.market_close_partial(sym, new_side, new_now, text="t-v2-fliprb")
            LOG(f"  ↩️ 回滚平新腿 {sym} {new_side} {new_now}张"
                f"（原因：{why[:80]}）：{r.get('reason') or '已提交'}")
        # 重查判定成败（**不解析文案**）
        ok2, legs2, err2 = ex.positions_strict()
        left = int(float(((legs2.get(sym) or {}) if ok2 else {}).get(new_side) or 0))
        if ok2 and left <= 0:
            # D1：回滚成功 → 删新腿记录（保留旧腿）
            eng = self._engine_ref
            if eng is not None:
                eng._mutate_meta(self._clear_one_meta(f"{sym}|{new_side}"),
                                 deleted={f"{sym}|{new_side}"})
            else:
                st = self._st()
                (st.get("open_meta") or {}).pop(f"{sym}|{new_side}", None)
                self._save(st, deleted={f"{sym}|{new_side}"})
            flip.set_phase(sym, flip.PH_ROLLBACK_DONE)
            txn = flip.get(sym)
            LOG(f"  ✅ ROLLBACK OK {sym} {new_side}：本轮什么都没发生，"
                f"回到 {old_side} {old_size}张（成本 2 笔 taker 等价）")
            try:
                journal.log("flip", phase="rollback_done", sym=sym,
                            new_side=new_side, old_side=old_side, why=why[:200],
                            cost="2x taker",
                            elapsed_s=round(txn.elapsed_s(), 1) if txn else None)
            except Exception:
                pass
            flip.clear_close_cause(sym, new_side)
            flip.end(sym)
            # 🔴 P0-1：到此为止。本轮不再执行任何后续动作（含 also_open）。
            return
        # D2：回滚失败 → S5，只报警不自动重试
        flip.note_error(sym, f"回滚平新腿失败，剩 {left} 张")
        try:
            journal.log("anomaly", kind="flip_rollback_failed", sym=sym,
                        new_side=new_side, old_side=old_side,
                        new_left=left, why=why[:200],
                        note="停止一切自动动作，等人工介入")
        except Exception:
            pass
        LOG(f"  🚨🚨🚨 回滚失败 {sym} {new_side} 仍剩 {left} 张 —— "
            f"**停止一切自动动作**（不重试、不自动平一边、不自动减仓），"
            f"该品种冻结 1 小时，等人工介入。原因为：{why[:120]}")
        flip.keep_failed(sym, float((self.cfg.get("flip") or {}).get(
            "rollback_freeze_s", 3600)))
        # ⚠ P0-1：这里也**不 return 前continue 到本轮后续逻辑** —— _flip 的
        #   调用方（_try_open）在 _flip 返回后立即 return，不会再动这个品种。

    def _clear_one_meta(self, key: str):
        """`_mutate_meta` 的 fn 协议实现：只删一个 key（返回 True = 真删了）。"""
        def _fn(st):
            om = st.setdefault("open_meta", {})
            if key not in om:
                return False
            om.pop(key, None)
            return True
        return _fn

    def _last_scan_item(self, sym: str) -> dict:
        """取本轮 scan 快照里该品种的标注（翻仓开仓打标签用，与首仓同源）。"""
        items = self._last_scan or(
            state.load(self.cfg["runtime_dir"] + "/scan.json", {}).get("items") or [])
        return next((it for it in items if it.get("contract") == sym), None) or {}

    def _try_pyramid_add(self, sym: str, side: str, band: dict,
                         key: str) -> bool:
        """高频金字塔加仓（10-02 19:37 用户拍板上线，业界规则交叉验证）。

        业界硬规则（NexusFi / DXP / TradingCompendium / quantstrategy）：
          ① 间距 0.5~1.0×ATR —— 固定点数/时间是任意的，只是噪音非确认
          ② 只给盈利单加仓（No adverse pyramiding）—— 严禁亏损加码
          ③ 最多 N 档追加，超出后相关性风险与复杂度失控
          ④ 各档统一止损（Unified stop）
          ⑤ 加仓前实算总风险 =Σ|entry_i − stop|×size_i ≤ 单笔风险 ×risk_mult
          ⑥ 兜底冷却（1 根 15m K 线），防同 K 线内重复
        返回 True=已加仓，False=拒开（已记日志）。
        """
        # 🔴 B3 的第二个活跃入口（架构 T03 必查点）：加仓路径会撤旧 SL 再重挂，
        #   而旧的按-text 匹配实现（`ex._cancel_price_orders(sym, "t-v2-SL")`，
        #   **已于 10-05 删除**）只认 text、不看方向。
        #   翻仓刚结束、冻结刚解除、meta 还没写完的那个边界窗口里走到这里，
        #   会把新腿的 SL 一起撤掉 → 新腿裸奔。故入口直接用同一个冻结判定。
        #   冻结粒度 = contract，别的品种不受影响。
        if flip.is_frozen(sym):
            LOG(f"  ⏸ 本轮不加仓 {sym} {side}：该品种处于翻仓/回滚窗口期"
                f"（冻结剩 {flip.freeze_left(sym):.0f}s），不撤单不重挂")
            return False
        ac = (self.cfg.get("hf_add") or {})
        if not ac.get("enabled", False):
            LOG(f"  🚫 拒开 {sym} {side}：同向已有仓，加仓未启用")
            return False
        st = self._st()
        meta = (st.get("open_meta") or {}).get(key) or {}
        if not meta:
            LOG(f"  🚫 拒开 {sym} {side}：同向仓缺元数据，不盲加")
            return False
        px = float(feeder.live_price(sym) or 0)
        entry0 = float(meta.get("entry") or 0)
        size0 = int(meta.get("size") or 0)
        risk_u = float(meta.get("risk_u") or 0)
        adds = int(meta.get("adds") or 0)
        atr = float(band.get("atr") or 0)
        if px <= 0 or entry0 <= 0 or size0 <= 0 or atr <= 0:
            LOG(f"  🚫 拒开 {sym} {side}：加仓所需数据不全（px/entry/size/atr）")
            return False
        # 规则③：档数上限
        if adds >= int(ac.get("max_adds", 1)):
            LOG(f"  🚫 拒开 {sym} {side}：加仓已 {adds} 达上限 {ac.get('max_adds')}")
            return False
        # 规则②：只给盈利单加仓。浮盈方向 = 顺行情势
        gain = (px - entry0) if side == "long" else (entry0 - px)
        need_gain = float(ac.get("gain_atr_min", 1.0)) * atr
        if gain < need_gain:
            LOG(f"  🚫 拒开 {sym} {side}：加仓需已有浮盈 ≥ "
                f"{float(ac.get('gain_atr_min', 1.0))}×ATR（{need_gain/px:.2%}），"
                f"当前 {gain/px:.2%}（严禁亏损加仓）")
            return False
        # 规则①：间距 ≥ N×ATR（离均价，不是离现价）
        gap = abs(px - entry0)
        need_gap = float(ac.get("spacing_atr", 1.0)) * atr
        if gap < need_gap:
            LOG(f"  🚫 拒开 {sym} {side}：距均价 {gap/entry0:.2%} < 间距 "
                f"{float(ac.get('spacing_atr', 1.0))}×ATR（{need_gap/entry0:.2%}）")
            return False
        # 规则⑥：兜底冷却（防同 K 线内重复）
        cool = float(ac.get("cooldown_s", 900))
        last = float(meta.get("last_add_t") or meta.get("t_at_open") or 0)
        if last and (time.time() - last) < cool:
            LOG(f"  🚫 拒开 {sym} {side}：加仓冷却中（剩 "
                f"{(cool - (time.time() - last))/60:.0f} 分钟）")
            return False
        # 规则③b：张数递减 = 首仓 × ratio
        add_sz = max(1, int(size0 * float(ac.get("size_ratio", 0.5))))
        qm = float(ex.spec(sym).get("quanto_multiplier") or 1)
        # 规则⑤：加仓后总风险 =|新均价 − SL| × 总张数 ≤ 单笔计划风险 × risk_mult
        new_sz = size0 + add_sz
        new_avg = (entry0 * size0 + px * add_sz) / new_sz
        sl_new = self._add_sl(sym, side, new_avg, band, meta) \
            if ac.get("recalc_avg_sl", True) else float(meta.get("sl") or 0)
        if sl_new <= 0:
            LOG(f"  🚫 拒开 {sym} {side}：无法重算均价止损，不加仓")
            return False
        new_dist = abs(new_avg - sl_new)
        total_risk = new_dist * qm * new_sz
        cap = risk_u * float(ac.get("risk_mult", 2.0))
        if total_risk > cap:
            LOG(f"  🚫 拒开 {sym} {side}：加仓后总风险 {total_risk:.4f}U > 上限 "
                f"{cap:.4f}U（单笔风险×{ac.get('risk_mult')}）")
            return False
        # 全部通过 → 执行加仓（规则④：统一重挂 SL/TP，均价重算）
        ex.set_lev(sym, float(meta.get("lev") or self.risk.lev_for(sym)))
        r = ex.market_open(sym, side, add_sz, text="t-v2-add")
        if not r.get("ok"):
            LOG(f"  ❌ 加仓失败 {sym} {side}：{r.get('reason')}（如实上报不记成功）")
            return False
        fill = float(r.get("fill_price") or px)
        add_done = int(r.get("filled") or add_sz)
        tot_sz = size0 + add_done
        tot_avg = (entry0 * size0 + fill * add_done) / tot_sz
        # 重算均价后的统一 SL/TP（撤旧 SL 单时**按方向精确撤**，不误伤另一方向；
        # 旧的按-text 匹配实现 `ex._cancel_price_orders(sym, "t-v2-SL")`
        # 已于 10-05 删除——它只看 text，双仓模式下同 contract 两腿的 text
        # 完全相同，会连另一方向的 SL 一起撤掉）
        # ⚠ tags 只传 ("t-v2-SL",)：本行之后会重挂 SL + **复用原 TP**，
        #   TP 必须留着（与改动前语义逐字一致）。
        try:
            ex.cancel_price_orders_side(sym, side, tags=("t-v2-SL",))
        except Exception:
            pass
        sl_final = self._add_sl(sym, side, tot_avg, band, meta)
        tp_final = float(meta.get("tp") or 0)
        sl_id, tp_id = ex.place_tpsl(sym, sl_px=sl_final, tp_px=tp_final,
                                    is_long=(side == "long"))
        meta.update({"entry": tot_avg, "size": tot_sz, "sl": sl_final,
                     "sl_id": sl_id, "tp_id": tp_id, "adds": adds + 1,
                     "last_add_t": time.time(),
                     "risk_u": min(total_risk, cap)})
        self._save(st)
        LOG(f"  📐 加仓 {sym} {side} 第{adds + 1}档 +{add_done}张 @{fill:g}"
            f"（金字塔递减，总 {tot_sz}张 均价 {tot_avg:g} SL 重挂 {sl_final:g}）")
        self._trade_log({"t": time.time(), "sym": sym, "side": side,
                         "action": "add", "entry": fill, "add_size": add_done,
                         "total_size": tot_sz, "avg": tot_avg, "sl": sl_final,
                         "sig": "pyramid"})
        return True

    def _add_sl(self, sym: str, side: str, avg: float, band: dict,
                meta: dict) -> float:
        """加仓后按新均价重算统一 SL（固定 1.5%，与 exit.sl_min_pct 同源）。

        10-03 21:30 同步改：原为 max(0.2%, 1×ATR)，随用户拍板「止损就 1.5%」统一口径，
        避免加仓后 SL 反而比首仓更近（ATR 在窄带品种上只有 0.1%）。
        """
        ex_exit = self.cfg.get("exit") or {}
        floor = avg * float(ex_exit.get("sl_min_pct", 0.015))
        sl = avg + floor if side == "short" else avg - floor
        try:
            from exchange import executor as _ex
            return float(_ex._round_px(sym, sl))
        except Exception:
            return round(sl, 8)

    def _recheck_open_fill(self, sym: str, side: str, tries: int = 3) -> int:
        """P0-1：开仓回报失败后，用三态快照复查「交易所到底成交没有」。

        返回三值（**这是全项目唯一实现该语义的地方**）：
            >= 0  → 交易所侧确认该方向有这么多张 → 认定 late_fill，按成交处理
            None  → 快照可信且确认**无**该方向的腿 → 真未成交
              -1  → 连续 `tries` 轮快照都不可信 → **无法判定**（不猜）

        为什么必须用 `positions_strict()` 而不是 `positions()`：
        `positions()` 走 `g.request_authed(...) or []`，而 `gate_client.request()`
        是 `json.loads(raw) if raw else {}`（`gate_client.py:109`）——**HTTP 200 +
        空 body** 时 `{}` 被 `or []` 吞成 `[]`，即「静默无仓」。用它复查等于把
        「查不到」当成「没成交」，正是 P0-1 的病根（`market_open` 内部的幽灵成交
        终检也是宽松版，遇空 body 同样形同虚设）。

        ## 不可信时为什么「不猜」而是交给裸仓巡检
        3 轮仍不可信时，本函数既不按成交、也不按未成交处理（守两条铁律）。
        残余风险由 `_guard` 的**裸仓巡检**兜底，核实结论如下（实测配置）：
          · 触发条件：有仓但 `open_meta` 无 key → `meta.sl == 0` → 走裸仓分支；
          · 节奏：`guard_heavy_every=30` × `guard_interval_s=2` → **每 60s 一次**
            （MEMORY 另记：实测一轮 heavy guard 要 4.3s，占空比 215%，
              最坏发现延迟约 129s —— 即最坏有 1~2 分钟的裸奔窗口）；
          · 动作：`place_tpsl` 补**保护性 SL**（距离 1×ATR15，兜底 entry×0.5%）。
          · ⚠ 它**不补 TP、不回填 open_meta** → 该仓不会进入动态止盈/峰值跟踪，
            只能靠交易所侧 SL 保命。
        所以本函数在不可信时打🚨 + 写 journal（`kind="open_fill_unknown"`），
        让「查不到」这件事**事后可查**，而不是静默变成一个裸仓。

        ## 为什么重试 3 轮、间隔怎么定
        3 轮各 sleep 0.6s（合计约 1.8s）：交易所侧成交回报通常在秒级落地，
        1.8s 足够覆盖「回报延迟」；而无限重试会把 hf 线程（60s 一轮）卡住，
        反而拖慢整个决策循环。`ok=True` 的那一轮立即返回，不做无谓等待。
        """
        import time as _t
        for i in range(max(1, int(tries))):
            ok, legs, err = ex.positions_strict()
            if ok:
                n = int(float((legs.get(sym) or {}).get(side) or 0))
                return n if n > 0 else None
            LOG(f"  ⚠ {sym} {side} 开仓复查第 {i + 1}/{tries} 轮：持仓快照不可信"
                f"（{err}）——不把「查不到」当「没成交」")
            if i + 1 < max(1, int(tries)):
                _t.sleep(0.6)
        return -1

    def _execute(self, order: dict, d: dict, sig: str = "model",
                 scan_item: dict = None):
        """真实下市价/挂单开仓。`scan_item` = 本轮该品种的标注。

        10-05：把 `scan_item` 透传进来，只为在 open 流水与 open_meta 里补记
        **开仓那一刻的快照与 trade_id**（见文件末 `_trade_log` 调用）。
        这两件是反馈闭环 / 标签统计的前提——平仓记录由 engine.py 写（带 reason/
        hold_min/peak/t_at_open），open 记录原来只有 entry/sl/tp/risk_u，
        **两边配不上对**，模型无法回答"我上次那么想 → 赚了还是亏了"。
        实测 259 笔 open 里 m1h_mom 命中 0 笔。
        纯落账，**不参与任何开仓/拒开判断**（用户铁规「判断权 100% 在模型」）。
        """
        sym, side = order["contract"], order["side"]
        ex.set_lev(sym, order["lev"])
        gcfg = self.cfg["guards"]
        # 10-02 12:12 用户拍板：开仓先挂 maker 单（买一/卖一，省 0.0586% 单边费），
        # 挂 entry_po_timeout_s 秒未成交转市价兜底；entry_post_only=false 一键回退
        #
        # 10-06 再入场锚点（用户拍板：同品种同方向连续开单，**全由代码负责**）：
        #   上一单平掉后价格若已走远，再按现价开同方向就是追价（今日实测该组
        #   笔均 −0.33U / 胜率 1/4）。这里查出锚点交给 ex.post_only_open：
        #   现价不劣于锚点 → 照常挂盘口；已劣于 → 改挂限价等回踩，不成交就放弃。
        #   reentry_guard_enabled=false 一键恢复旧行为（不查锚点、照常市价兜底）。
        anchor_px, anchor_desc = None, None
        if gcfg.get("reentry_guard_enabled", False):
            _a = _reentry_anchor(self.cfg.get("runtime_dir") or ".", sym, side,
                                 float(gcfg.get("reentry_window_min", 0) or 0))
            if _a:
                anchor_px, anchor_desc = _a
                LOG(f"  🎯 {sym} {side} 再入场检测：{anchor_desc}")
        if gcfg.get("entry_post_only", True):
            r = ex.post_only_open(
                sym, side, order["size"],
                timeout_s=float(gcfg.get("entry_po_timeout_s", 5)),
                anchor_px=anchor_px,
                anchor_tol_pct=float(gcfg.get("reentry_chase_tol_pct", 0.001)),
                cancel_on_timeout=bool(anchor_px))
        else:
            r = ex.market_open(sym, side, order["size"])
        # 再入场锚点挂单未成交 → 本轮放弃（**不追价**）。与"开仓失败"区分开：
        #   这是**预期的业务结果**，不是故障，所以单独出日志 + 记 journal。
        if (not r.get("ok")) and r.get("no_chase"):
            LOG(f"  🚫 {sym} {side} 再入场挂单未成交 → 本轮放弃（不追价）"
                f"｜{anchor_desc}")
            try:
                journal.log("anomaly", kind="reentry_no_chase", sym=sym, side=side,
                            anchor=anchor_px, why=str(anchor_desc))
            except Exception:
                pass
            return
        if not r.get("ok"):
            # 🔴 P0-1：开仓回报失败 ≠ 没成交。下单已发出，交易所侧可能已经成交，
            #   只是 IOC 回报没跟上（`market_open` 内部虽有幽灵成交终检，但那次查询
            #   走的是**宽松版 positions()**，遇「HTTP 200 + 空 body」会静默返回
            #   `[]` → 终检形同虚设 → 这里就带着 ok=False 回到这里）。
            #   若直接 return：不写 open_meta、不挂 SL/TP → **裸仓**。
            #   所以这里用 `positions_strict()`（三态）复查交易所事实。
            #
            #   两条铁律（本函数是它们唯一的实现处）：
            #     ① 绝不把「不可信」当「没成交」——那是本 bug 的病根；
            #     ② 绝不把「不可信」当「有成交」——那会记一笔不存在的仓，
            #        让引擎拿着一张不存在的 SL/TP 去管一个不存在的仓，更糟。
            #   不可信时**只重查**，查不出来就如实上报 + 高危告警 + 记 journal，
            #   由 `_guard` 的裸仓巡检兜底（见 `_recheck_open_fill` docstring）。
            _leg = self._recheck_open_fill(sym, side)
            if _leg is None:
                LOG(f"  ❌ 开仓失败 {sym} {side}: {r.get('reason')}（如实上报不记成功）")
                return
            if _leg < 0:
                # 3 轮仍不可信：不猜、不记仓、不挂单（守铁律 ②），但必须留痕。
                LOG(f"  🚨🚨 {sym} {side} 开仓回报失败且**持仓快照连续 3 轮不可信**，"
                    f"无法判定是否成交 → 本轮不记账、不挂 SL/TP。"
                    f"若实际已成交，将由 guard 裸仓巡检补挂保护性SL（最坏 ~1~2 分钟），"
                    f"**请人工核对 {sym} 持仓**")
                try:
                    journal.log("anomaly", kind="open_fill_unknown",
                                sym=sym, side=side, want=int(order["size"]),
                                reason=str(r.get("reason"))[:200],
                                why="开仓回报失败且 3 轮持仓快照均不可信；"
                                    "既不按成交也不按未成交处理，等人工核对")
                except Exception:
                    pass
                return
            # 交易所侧确认有该方向的腿 → 认定 late_fill，按成交继续
            # （下面的 place_tpsl + open_meta 写入会把保护单补上，裸仓即被堵住）。
            LOG(f"  ⚠ {sym} {side} 开仓回报失败但**交易所侧确认有 {_leg} 张**"
                f"（late_fill）→ 按成交记账并补挂 SL/TP")
            try:
                journal.log("anomaly", kind="late_fill_confirmed", sym=sym,
                            side=side, filled=int(_leg),
                            want=int(order["size"]),
                            reason=str(r.get("reason"))[:200],
                            why="开仓回报 ok=False，positions_strict 复查确认已成交")
            except Exception:
                pass
            r = dict(r)
            r["ok"] = True
            r["filled"] = int(_leg)
            r["late_fill"] = True
        fill = float(r.get("fill_price") or order["price"])
        size = int(r.get("filled") or order["size"])   # 部分成交按实际张数记账
        # 该单实际往返手续费率（10-02 13:2x 用户拍板：满足手续费就介入止盈盯盘，
        # 不写死——按品种费率 + 该单成交路径实算，E 段动态止盈以此为锁定下限。
        # 10-02 16:0x 实测修正：账户结算费率 taker 0.05% / maker 0.02%（正费不返佣），
        # 挂牌接口的 0.075% / -0.01% 偏高 → 优先按账户最近成交实算，失败回退挂牌）
        rf = ex.g.real_fee_rates() or {}
        sp = ex.spec(sym)
        taker_fee = float(rf.get("taker")
                          or abs(float(sp.get("taker_fee_rate") or 0.00075)))
        maker_fee = float(rf.get("maker")
                          or float(sp.get("maker_fee_rate") or -0.0001))  # 负值=返佣
        if r.get("maker_filled") is not None:                    # maker 超时转市价混合
            mk = int(r.get("maker_filled") or 0)
        else:
            mk = size if r.get("maker") else 0                   # 全 maker 或全 taker
        tk = max(0, size - mk)
        fee_open = (mk * maker_fee + tk * taker_fee) / max(1, mk + tk)
        fee_rt = round(fee_open + taker_fee, 6)                  # 平仓按 taker 市价落袋
        sl_id, tp_id = ex.place_tpsl(sym, sl_px=order["sl"], tp_px=order["tp"],
                                     is_long=(side == "long"))
        # 记录开仓元数据（恐慌拦截/教训反思都要用）
        st = self._st()
        # 10-03 20:15 修（幽灵峰值 bug 根因③）：开仓必须把**峰值锚定到入场价**。
        #   原写法不带 peak/tp_peak → guard C/E 段读不到就 fallback 到 meta 里的
        #   旧值，而该值可能是**上一代同 sym|side 仓位的峰值**（记录被覆盖时）→
        #   新仓一出生就被判定"已经赚过又回吐"，立刻按保本兜底秒平。
        #   实证：ZEC 20:00:40 开 → 20:00:52 按"峰值1318.27"平（该峰值属上一笔）；
        #   SKHYNIX 15:48:35 开 → 15:48:47 按"峰值1370.49"平，开仓日志 48 秒才打。
        #   注意：本处是**整条覆盖写**（同一 key 属于本次新开的仓），不是合并。
        # 10-05 任务 E：本单唯一 id，**必须由 t_at_open 派生**（不能另起一次
        # time.time()）—— engine.py 平仓时会把 meta 的 `t_at_open` 原样抄进
        # close 记录，而统计侧 `_tag_stats` 是用
        #   f"{sym}|{side}|{int(t_at_open*1000)}"
        # 反查 trade_id 的。两处时间戳若不是同一个 float，int(×1000) 会差
        # 几毫秒 → **永远配不上**（mock 实测踩过：trade_id 用 20:06:57.079
        # 而 t_at_open 是 20:06:57.073，差 6ms）。所以这里只取一次时间。
        t_at_open = time.time()
        trade_id = f"{sym}|{side}|{int(t_at_open * 1000)}"
        # ---- 10-07 17:15 上移：本轮 scan 的标注与候选快照 ----
        #   为什么必须提到 open_meta 之前：候选 TTL 只有几小时、regime/band_pos
        #   每轮都在变，**平仓时现查会拿到另一个时刻的答案** ⇒ 记账必须锚在
        #   开仓那一刻。open_meta 是开仓→平仓全程存活的唯一载体（engine 平仓时
        #   从 meta 透传进 journal close），所以提取必须在这里。
        #   零额外 HTTP：直接取本轮 scan_item（与喂给模型的值同源）；缺给 None。
        _si = scan_item or {}
        _cand = _si.get("candidate") or {}
        st.setdefault("open_meta", {})[f"{sym}|{side}"] = {
            "entry": fill, "sl": order["sl"], "tp": order["tp"],
            "size": size, "risk_u": order["risk_u"],
            "rr": order["rr"], "t_at_open": t_at_open,
            "sl_id": sl_id, "tp_id": tp_id,
            "wr": order["win_rate"], "sig": sig, "fee_rt": fee_rt,
            "peak": fill, "tp_peak": fill,
            # 10-06：最大不利偏移（MAE）的锚点，与 peak **完全对称**——guard
            #   每轮只朝不利方向推进、只进不退。开仓即锚定入场价，同样是为防
            #   新仓继承上一代同 sym|side 仓位的旧 trough（幽灵峰值同型 bug）。
            "trough": fill,
            "trade_id": trade_id,
            # 冻结初始 SL 距离（guard 的 C/E 段 armed 门槛用，防 C 段推进 SL 后
            # sl_dist 塌缩 → armed 退回手续费线。此处开仓即落，guard 不必再猜）
            "sl_dist0": round(abs(float(order["sl"]) - float(fill)), 8),
            "reason": d.get("reason") or "",
            # ---- 10-07 17:15：开仓那一刻的候选 + regime 快照 ----
            #   背景：journal.py 的 docstring 早就约定 open 应带 cand 字段，
            #   但实现一直只落了 entry_src='touch' 这种**单一优先级**标签
            #   （deep>seq>wm>touch，会掩盖"同时命中多个形态"）⇒ 事后复盘无法
            #   回答"这单当时有没有候选／候选什么方向／regime 判成什么"，
            #   于是今天 3 笔亏得分不清是候选机制的问题还是 regime 判定的问题。
            #   has_cand 显式布尔：让"无候选"与"字段缺失"在统计时可区分。
            #   纯落账，不参与任何判定（用户铁规：判断权 100% 在模型）。
            "cand_side": (_cand.get("side") if _cand else None),
            "has_cand": bool(_cand),
            "cand_types": ([k for k in ("deep", "seq", "wm", "first")
                            if _cand.get(k)] or None),
            "cand_confirm_ready": (_cand.get("confirm_ready") if _cand else None),
            "adx": _si.get("adx"), "regime": _si.get("regime"),
            "band_pos": _si.get("band_pos"),
            "walk_dn_bars": _si.get("walk_dn_bars"),
            "fake_dn_n": _si.get("fake_dn_n")}
        self._save(st)
        self.risk.bump_trades()
        msg = (f"✅ 开 {sym} {side} {size}张 @{fill:g} SL {order['sl']:g} "
               f"TP {order['tp']:g} RR {order['rr']} | {d.get('reason') or ''}")
        LOG(f"  {msg}")
        # 完整流水（10-03 01:20 用户要求「完整每一单的日志」）：开仓全字段进 journal，
        # 后期按 ev=open 过滤即可逐单复盘（对账/统计仍读精简的 trades.json）。
        # 10-07 17:15：此块已**继续上移**到 open_meta 写入之前（见上方 "10-07
        #   17:15 上移" 处）—— 因为候选快照要存进 open_meta 供平仓透传，
        #   而 open_meta 的写入位置比这里更早。
        # 形态来源：deep 深砸穿轨收回 / seq 连续假突破 / wm 双底双顶 /
        # touch 仅是碰轨（无上述形态）。**按优先级取一个**——一单可能同时
        # 命中多个形态，取最深的那个（deep > seq > wm > touch），保证标签
        # 是单一可分类值，统计时不会重复计数。
        _src = ("deep" if _cand.get("deep") else
                "seq" if _cand.get("seq") else
                "wm" if _cand.get("wm") else
                "touch" if (_cand.get("first") or _si.get("last_touch")) else None)
        try:
            _mult = float((sp or {}).get("quanto_multiplier") or 1)
        except Exception:
            _mult = 1.0
        _fee = float((rf or {}).get("taker") or 0)
        _notional = abs(int(size)) * _mult * float(fill)
        _e = float(fill) or 1.0
        journal.journal_open(
            sym=sym, side=side, entry=round(float(fill), 8),
            size=abs(int(size)), notional=round(_notional, 4), mult=_mult,
            sl=order["sl"], tp=order["tp"], rr=order["rr"],
            lev=int(order.get("lev") or 0), risk_u=order["risk_u"],
            fee_rt=rf.get("fee_rt"), fee_round=round(_notional * _fee, 4),
            win_rate=d.get("win_rate"), regime=d.get("regime"), bias=d.get("bias"),
            sl_dist_pct=round(abs(float(order["sl"]) - _e) / _e * 100, 3),
            tp_dist_pct=round(abs(float(order["tp"]) - _e) / _e * 100, 3),
            maker=bool((r or {}).get("maker")),
            taker_filled=(r or {}).get("taker_filled"),
            maker_filled=(r or {}).get("maker_filled"),
            late_fill=bool((r or {}).get("late_fill")),
            open_ts=time.time(),
            dual_side="dual_long" if side == "long" else "dual_short",
            reason=d.get("reason") or "", sig=sig,
            # ---- 10-06 新增（遥测/复盘用，**纯落账、不参与任何决策**）----
            # ① 三张订单号：开仓单 id 来自开仓回报 `r`（market_open 与
            #    post_only_open 的返回体里都带 `id`，空响应时为 None）；
            #    SL/TP 条件单 id 来自上面 `place_tpsl` 的返回——与写进
            #    open_meta 的是同一对值，平仓时再用它配对。
            # ② trade_id 与 _trade_log / 平仓记录同源 → pandas 可按它三表 join。
            # ③ 开仓那一刻的市场快照：直接取本轮 scan 标注（零额外 HTTP）。
            order_id=(r or {}).get("id"), sl_id=sl_id, tp_id=tp_id,
            trade_id=trade_id,
            m1h_mom=_si.get("m1h_mom"), band_pos=_si.get("band_pos"),
            entry_src=_src,
            # ---- 10-07 17:15：候选快照（补 journal.py 早有契约却一直漏掉的 cand）----
            #   cand_types 与 entry_src 互补：后者是单一优先级标签，前者保留全部。
            has_cand=bool(_cand),
            cand_side=_cand.get("side") if _cand else None,
            cand_types=([k for k in ("deep", "seq", "wm", "first")
                         if _cand.get(k)] or None),
            cand_level=_cand.get("level") if _cand else None,
            cand_confirm_ready=_cand.get("confirm_ready") if _cand else None,
            adx=_si.get("adx"),
            walk_dn_bars=_si.get("walk_dn_bars"),
            fake_dn_n=_si.get("fake_dn_n"),
            atr15=_si.get("atr15"), bw_pct=_si.get("bw_pct"),
            bw_burst=_si.get("bw_burst"), mid=_si.get("mid"),
            up=_si.get("up"), dn=_si.get("dn"), px=_si.get("px"),
            equity=round(float(ex.equity() or 0), 2))
        # 10-05 补记「开仓那一刻的市场快照」+ 唯一 trade_id——反馈闭环与标签
        # 统计的另一半。有了它，每笔平仓都能与当初的判断精确配对：模型在
        #【最近交易回顾】与 {PERF} 标签段里能同时看到「当时怎么想的」和
        # 「结果如何」。（_si/_cand/_src 的提取已上移到开仓落账段之前，
        # 见 10-06 注释；缺数据时给 None，_perf_stats 只认 tp_dist_pct 非空
        # 的 close 记录，这些附加字段缺失不影响总战绩统计口径。）
        self._trade_log({"t": t_at_open, "sym": sym, "side": side,
                         "action": "open", "entry": fill,
                         "sl": order["sl"], "tp": order["tp"],
                         "size": size, "risk_u": order["risk_u"],
                         "sig": sig, "reason": d.get("reason") or "",
                         "trade_id": trade_id,
                         "m1h_mom": _si.get("m1h_mom"),
                         "band_pos": _si.get("band_pos"),
                         "regime": _si.get("regime"),
                         "win_rate": d.get("win_rate"),
                         "entry_src": _src})

    # -------------------------------------------------- 平仓（模型说平就平）
    def _try_close(self, d: dict):
        cfg = self.cfg["guards"]
        sym = d.get("contract")
        side = (d.get("side") or "").lower()
        # 🔴 10-05 修（空快照灾难链，用户批准）：原`ex.positions()` 在
        # 「HTTP 200 + 空 body」时静默返回 `[]` → 下面 `if not pos` 会打
        # 「无持仓可平」并return，看起来是正常业务分支，实际是**查询失败**
        # 被伪装成「没仓」。本路径的唯一后果是「漏平」（模型建议被吞），
        # 不涉及资金损失，但**必须可观测** —— 否则将来统计「模型平仓建议被
        # 采纳率」时会把查询失败算成「模型判断错」，得出错误结论。
        # ⚠ 用 `positions_strict_rows()`（保留原始行）：本函数要读
        #   `unrealised_pnl` 与 `entry_price`，legs 结构里没有。
        _ok, _poss, _perr = ex.positions_strict_rows()
        if not _ok:
            LOG(f"  🚫 {sym} {side} 平仓建议本轮跳过：持仓快照不可信"
                f"（{_perr}）——绝不把「查不到」当成「没仓」")
            try:
                journal.log("anomaly", kind="positions_unavailable",
                            where="try_close", sym=sym, side=side,
                            err=str(_perr)[:200],
                            why="平仓建议跳过本轮，查询失败不等于无仓")
            except Exception:
                pass
            return
        pos = next((p for p in _poss if p["contract"] == sym
                    and float(p.get("size", 0)) != 0
                    and (float(p["size"]) > 0) == (side == "long")), None)
        if not pos:
            LOG(f"  {sym} {side} 无持仓可平")
            return
        if f"{sym}|{side}" not in (self._st().get("open_meta") or {}):
            LOG(f"  {sym} {side} 是账外仓（非系统所开），隔离中，忽略 close")
            return
        st = self._st()
        meta = (st.get("open_meta") or {}).get(f"{sym}|{side}") or {}
        upnl = float(pos.get("unrealised_pnl") or 0)
        px = feeder.live_price(sym)
        entry = float(pos.get("entry_price") or 0)
        gain = (px - entry) / entry if entry and side == "long" else \
            (entry - px) / entry if entry else 0
        # 唯一保留的平仓限制：浮盈在 0 ~ 0.1% 之间不值得平（覆盖不了往返手续费）；
        # 浮亏侧不设任何限制，模型判断方向变了直接平
        if 0 < gain < cfg["profit_cover_pct"]:
            LOG(f"  {sym} {side} 浮盈 {gain:.2%} < 0.1% 成本线，不平")
            return
        msg = ex.market_close(sym, side)
        LOG(f"  {msg} | 模型理由：{d.get('reason') or ''}")
        (st.setdefault("open_meta", {})).pop(f"{sym}|{side}", None)
        self._save(st)
        self._trade_log({"t": time.time(), "sym": sym, "side": side, "action": "close",
                         "pnl": round(upnl, 4), "reason": d.get("reason") or "",
                         "close_cause": flip.CAUSE_MANUAL})
        self._maybe_reflect({"sym": sym, "side": side, "pnl": upnl,
                             "meta": meta, "gain": gain,
                             "close_cause": flip.CAUSE_MANUAL})

    # -------------------------------------------------- 反思进化
    def _maybe_reflect(self, tr: dict):
        """委托复盘迭代引擎（agents/review.py）：质检三条闸 + 同类归并 + 判词入库"""
        from agents.review import ReviewAgent
        ReviewAgent(self.cfg).on_close(tr)

    # -------------------------------------------------- 落盘
    def _record(self, d: dict | None, candidates: list):
        ts = datetime.now().strftime("%m-%d %H:%M:%S")
        cands = [c["contract"] + ":" + c["candidate"]["side"]
                 for c in candidates if c.get("candidate")]
        line = f"\n## {ts}\n- 扫描 {len(candidates)} 品种 | 机器候选: {', '.join(cands) or '无（滑行/等待触轨形态中，非开仓窗口）'}\n- 决策: " \
               f"{json.dumps(d, ensure_ascii=False) if d else '无输出'}"
        state.append_line(self.decisions_path, line)
        # 滚动保留（10-08：加了 scan 逐品种判定后每轮多 ~400B，此文件原先从不裁剪）
        _trim(self.decisions_path,
              int((self.cfg.get("llm") or {}).get("decisions_tail_lines", 600) or 600))
        if d:
            self._scan_diag(d, candidates)

    def _scan_diag(self, d: dict, candidates: list):
        """核对模型 scan（逐品种判定）的覆盖率与【开仓前置】硬约束——**只在异常时出声**。

        🔴 只诊断、不拦截：绝不据它拒开/改方向（用户铁规「判断权 100% 在模型」）。
        目的：把"它到底有没有把每个品种过一遍、有没有自打嘴巴"变成可查的日志事实
        （这正是 10-08 那次"全池贴下轨"泛泛叙事的盲区——当时无从核对）。
        """
        scan = d.get("scan")
        if not isinstance(scan, list):
            LOG("  🧭 逐品种判定：本轮模型未给 scan（覆盖率无从核对）")
            return
        got = {}
        for it in scan:
            if not isinstance(it, dict):
                continue
            c = str(it.get("c") or it.get("contract") or "").strip()
            v = str(it.get("v") or it.get("verdict") or "").strip().lower()
            if c:
                got.setdefault(c, v)
        want = [c["contract"] for c in candidates]
        mom_of = {c["contract"]: c.get("m1h_mom") for c in candidates}
        missing = [s for s in want if s not in got]
        bad = []
        for s, v in got.items():
            if v not in ("long", "short"):
                continue
            try:
                f = float(mom_of.get(s))
            except (TypeError, ValueError):
                continue
            if abs(f) < 0.05:
                continue                     # 侧向不受限（与【开仓前置】同口径）
            if (v == "long" and f < 0) or (v == "short" and f > 0):
                bad.append(f"{s.replace('_USDT', '')}={v}(mom{f:+.2f})")
        if missing or bad:
            msg = f"  🧭 逐品种判定 覆盖 {len(got)}/{len(want)}"
            if missing:
                msg += " 漏: " + ",".join(x.replace("_USDT", "") for x in missing)
            if bad:
                msg += " ⚠违【开仓前置】: " + "; ".join(bad)
            LOG(msg)

    def _trade_log(self, tr: dict):
        trades = state.load(self.trades_path, [])
        trades.append(tr)
        state.save(self.trades_path, trades[-500:])
