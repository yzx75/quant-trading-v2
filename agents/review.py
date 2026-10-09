# -*- coding: utf-8 -*-
"""
agents/review.py — 复盘线程（2026-09-30 立，2026-10-05 收缩）

⚠ **10-05 收缩：只剩教训生成一件事是真的。**
  用户核实后确认，下面四项的产物全部**零有效消费者**，已全部停算：
    · 甲行情分段 segments  —— 零外部消费者；且 decision.py 命中数=0，
      模型从不读它，scan.json 里已有更准的 regime 字段。
    · 乙 影子参数 shadow    —— 零外部消费者；每小时 15 品种 × 120 根 K线
      的重活，停了直接把 CPU 让给 guard 线程（2s 轮、守护全部持仓）。
    · 每单判词 verdicts+god —— 前端那个面板已换成标签战绩（读 trades.json
      的真实平仓记录），不再读它。停算但**保留代码**：MFE/MAE 是唯一能回答
      「止损打掉的那笔、如果没有 SL 会怎样」的信号，将来重建要重写拉 K线那套。
    · sig_stats             —— 前端从没读过，且比现在的 4 维标签统计更粗，
      留着是两套重复口径。
  四者的**函数体全部保留**（随时可恢复调用），只是 `cycle()` 不再调、
  `_load()`/`_save()` 不再输出这些键——不留 `segments: {}` 这种空壳，
  否则分不清「已停算」还是「还没算」。

  剩下唯一有价值的链路（**一行都没动**）：
    平仓 → `_lesson()` → llm.ask(thinking/reflect) → runtime/lessons.md
         → decision.py `_brief()` 拼进「最近教训」段 → **模型在读**
"""
from __future__ import annotations

import re
import time
from datetime import datetime

from agents import llm
from core import indicators as ind
from core import log as core_log
from core import state

LOG = core_log.log

# 教训质检词表
_ACT = ("开", "平", "等", "避", "确认", "追", "守", "拒", "推", "放", "别", "不")
_COND = ("才", "须", "若", "再", "后", "前", "否则", "时", "达", "破", "如果", "只要", "一旦")
_BAD = ("平本", "+0.0", "0%幅度", "作为", "总之", "一般来说")

# 10-05：已停算、不再产出也不再保留的键。_load()/_save() 双向过滤，
# 用途见文件头。注意 sig_stats 是 on_close 里的副产物，一并清掉。
_RETIRED_KEYS = ("segments", "t_seg", "shadow", "t_shadow",
                 "verdicts", "sig_stats")

# lessons.md 的文件头说明行（模块级常量：前端空态文案与这里保持同一口径）。
# 2026-10-05 修正：原文写"见 trade_review 看结果"，但代码里根本没有叫
# trade_review 的东西（是团队内部对该面板的叫法），模型读到会去找一个
# 不存在的字段。改成指向 brief 里真实存在的两段。
_LESSON_HEADER = ("（只记录亏损单的条件教训；盈利单不进教训，"
                  "其结果见 brief 的「最近交易回顾」与标签战绩；"
                  "翻仓/回滚平仓不生成教训但计入战绩统计）\n")

# 假桶名（10-05 23:1x bug 的产物）：标签配不上的老单曾被硬写成
# `[聚合] 未知 | 暂无战绩 | ...`。它不是一类真实行情，而是"没数据"，
# 留着会污染 `_tag_stats` 分组、误导模型、显示成纯噪音 → 一律剔除。
# ⚠ 桶键口径是 **4 维**（mom×pos×regime×entry_src，见 `_lesson` 里的说明），
#   所以这里的假桶名单也按 4 维写；3 维的旧写法一并留着（历史遗留行）。
_FAKE_BUCKETS = ("未知", "未知×未知×未知", "未知×未知×其他",
                 "无数据×无数据×其他", "无数据×无数据×其他×无")


class ReviewAgent:
    """复盘迭代引擎（无常驻状态，全部落 runtime/review.json）"""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.path = cfg["runtime_dir"] + "/review.json"
        self.lessons_path = cfg["runtime_dir"] + "/lessons.md"
        # 配不上标签的教训正文单独存（只留知识、不进模型视野），
        # 见 _append_untagged 的说明。
        self.untagged_path = cfg["runtime_dir"] + "/lessons_untagged.md"
        rcfg = cfg.setdefault("review", {})
        rcfg.setdefault("god_view_bars", 20)
        rcfg.setdefault("shadow_params", [{"n": 15, "k": 2.0}, {"n": 20, "k": 2.0},
                                          {"n": 25, "k": 2.2}])
        rcfg.setdefault("lesson_min_ratio", 0.55)

    # -------------------------------------------------- 状态
    # 10-05：默认值里**不再有** segments/shadow/verdicts/sig_stats。
    # 这几个键已停算（见文件头），留 {} 空壳会让人分不清「已停算」还是
    # 「还没算出第一轮」——前端/日志里看到空对象会误判成数据缺失。
    def _load(self) -> dict:
        d = state.load(self.path, {})
        if isinstance(d, dict):
            # 存量文件里可能还留着停算前的旧键，读出来时顺手剔掉：
            # 只在真有残留时才写回（正常情况下是零 IO 的纯内存过滤）。
            stale = [k for k in _RETIRED_KEYS if k in d]
            if stale:
                for k in stale:
                    d.pop(k, None)
                self._save(d)
                LOG(f"  🧹 review.json 清理已停算的键：{', '.join(stale)}")
        return d if isinstance(d, dict) else {}

    def _save(self, d: dict):
        # 出口再挡一道：任何调用方（包括保留的 update_segments/_god_view，
        # 万一被谁手工调一次）写出去的数据都不含已停算的键。
        for k in _RETIRED_KEYS:
            d.pop(k, None)
        state.save(self.path, d)

    # -------------------------------------------------- 周期（线程 5 分钟一轮）
    def cycle(self):
        """10-05 收缩后**故意留空**（原：每小时的 segments/shadow + 每轮 god_view）。

        保留这个空方法是因为 `engine.py` 起 review 线程时要调 `self.review.cycle`
        （engine.py 不能改，方法被删会 AttributeError 起不来线程）。
        四项复盘统计全部停算，review 线程现在**不产生任何 IO**：
        教训走 `on_close()` 事件驱动（平仓当场生成），不依赖这个 5 分钟轮。

        省掉的重IO（对 guard 线程 2s 轮有实际意义）：
          · update_segments —— 原每 3600s 读 scan.json 并分类 15 品种
          · update_shadow  —— 原每 3600s 拉**15 品种 × 120 根 15m K线**
            并对每组参数逐根算布林轨道（最重的一项，串行 CPU+ 网络）
          · _god_view      —— 原每 300s 为最多 8 笔待判单各拉 60 根 K线
        """
        return

    # -------------------------------------------------- 每单判词（平仓事件驱动）
    def on_close(self, tr: dict):
        """平仓后只做一件事：**生成教训**（质检+归并 → lessons.md）。

        10-05 收缩：原先这里还会把本单 append 进 `verdicts`（上帝视角待判
        队列）并累加 `sig_stats`（信号分组胜率），两个键**均已停算**——
        前端那个面板已换成读 trades.json 的标签战绩，不再读它们；sig_stats
        的口径也已被 4 维标签统计取代，留着是两套重复数字。
        所以这里现在**一行落盘都没有**，只有 _lesson() 这条真链路。

        ⚠ 教训是 review 线程唯一有价值的产物（→ lessons.md → brief 的
          「最近教训」段 → 模型在读），**不要为了"省IO"把它一起砍掉**。
        """
        self._lesson(tr)

    # -------------------------------------------------- 教训生成（标签聚合版）
    # ---- 教训的读写上限：控制 lessons.md 不无限膨胀（L4）----
    # 聚合后条目数很少（按标签组合归并），但**老格式行**（10-05 之前写的
    # 91 行散乱自由文本）会在首次写入时被清掉，见 _write_lessons。
    _LESSON_MAX_LINES = 30

    def _lesson(self, tr: dict):
        """平仓后生成一条**带标签、带战绩**的结构化教训（L2 + L3）。

        10-05 改造（用户：「和订单号联动起来，并且要真的能帮助模型做决策」）：

        改前的三个问题：
          ① 教训是自由文本，没有 trade_id → 无法统计「持这条教训的那类交易
             后来赚没赚」，与标签闭环完全脱节。
          ② **每笔平仓都写**（实测 91 条里 49 条来自盈利单，占 56%）——
             "教训"本该是"别再犯"的筛选，盈利单也写就把规则搞混了，
             模型看到几十条混杂规则无从取舍。
          ③ 归并只打一个 `(同类×8)` 计数，看不出「哪类教训重复 8 次、赢率多少」。

        改后：
          · **只对亏损单写**（L2，盈利单不进教训）。这是"条件教训"的本意。
          · 带上 `trade_id` + 标签组合 + 该组合的**真实战绩**（L1/L3），
            模型看到的不再是散乱规则，而是「这类条件下我 historically
            亏多少」——这才能直接指导决策。
          · 同一标签组合的教训**合并成一条**（去重键 = 标签组合），
            计数与胜率/累计盈亏直接从 `_tag_stats` 口径现算。

        ⚠ 标签/战绩需要 trade_id 配对，老数据（无 trade_id）取不到 →
          **跳过不写**，绝不编造（宁可少一条，也不能给模型假依据）。
        """
        rm = self.cfg["llm"].get("reflect_model_env")
        if not rm:
            return
        # ---- L2：只对亏损单写教训（用户 21:2x 明确口径）----
        # 用户的原话：「给模型不是有胜率的吗，可以在自我迭代那里生成，
        # 但是模型教训不生成」——分工是：
        #   · 胜率 / 标签统计（自我迭代面板）→ 盈亏单**都算**
        #   · 模型教训（lessons.md）→ **只对亏损单**
        # 盈亏单都进标签统计，所以"盈利单的结果"在别处已经如实呈现，
        # 不需要再写一条"教训"混进规则里（那会让规则不纯：教训本该是
        # "别再犯"的筛选，混进盈利单就失去筛选意义）。
        if float(tr.get("pnl") or 0) > 0:
            return
        # ---- 10-05 翻仓/回滚平仓：**不生成教训**，但**绝不挡账** ----
        # 为什么必须挡：翻仓是「模型主动换方向」，回滚是「系统执行层问题」，
        # 两者都**不是模型判断失误**。若不挡，会写出「这类条件下你亏了」这类
        # 教训 → 模型未来**回避自己本来正确的判断**，这是比双开更隐蔽、
        # 更长期的教训库污染（PRD §8.1）。
        # 为什么只挡教训：那些平仓**照实**写进 trades.json、照实计入 4 维标签桶
        # 与看板回顾（`_tag_stats` / `build_recent_reviews` 都不改）——
        # 「只挡教训，绝不挡账」。系统自吃的亏必须可见。
        # ⚠ 位置铁律：必须在上面 `pnl > 0` 判断**之后**（保持「只对亏损单写
        #   这条 L2 原则」为主闸，本条只是追加一个更细的排除理由）。
        # ⚠ 读取一律用 `.get()`（老记录没有 close_cause 键），
        #   `None` 不在元组里→ 老记录行为**完全不变**。
        if tr.get("close_cause") in ("flip_out", "flip_rollback"):
            LOG(f"  📝 教训跳过（{tr.get('sym')} {tr.get('side')}）："
                f"close_cause={tr.get('close_cause')}（翻仓/回滚平仓不是模型"
                f"判断失误，不写教训；但已如实计入战绩统计）")
            return
        meta = tr.get("meta") or {}
        sym, side = tr.get("sym"), tr.get("side")
        # L1：trade_id。open_meta 里 10-05 起才有；拿不到就跳过本条。
        # ★ 反查键必须与 decision._tag_stats() **完全一致**，否则联不上：
        #   decision 侧 f"{sym}|{side}|{int(t_open*1000)}"，其中 t_open 取自
        #   close 记录的 t_at_open（engine.py 从 open_meta 抄来），与这里的
        #   meta["t_at_open"] 是同一个 float → 两侧 int(×1000) 必然相同。
        trade_id = str(meta.get("trade_id") or "")
        if not trade_id:
            t_open = meta.get("t_at_open")
            if not t_open:
                LOG(f"  📝 教训跳过（{sym} {side}）：无 trade_id/t_at_open，无法与"
                    f"标签体系联动")
                return
            trade_id = f"{sym}|{side}|{int(float(t_open) * 1000)}"
        # 标签组合 + 该组合真实战绩（import 复用 decision 的分类与统计口径，
        # **不另写一套**——两处口径漂移会让模型看到互相矛盾的依据）
        combo_mom = combo_pos = combo_reg = combo_src = ""
        # 标签模块统一 import 复用（decision 的分类函数是唯一真源，
        # **不另写一套**——两处口径漂移会让模型看到互相矛盾的依据）
        try:
            from agents.decision import _read_trades_tail, _tag_stats, \
                _mom_dir_of, _pos_zone_of, _regime_of, _entry_src_of
        except Exception as e:  # noqa: BLE001
            LOG(f"  📝 教训跳过（{sym}）：标签模块不可用 {str(e)[:60]}")
            return
        try:
            trades = _read_trades_tail(
                (self.cfg["runtime_dir"] or "") + "/trades.json", 65536)
            op = next((t for t in trades if t.get("action") == "open"
                       and str(t.get("trade_id") or "") == trade_id), None)
            if op is None:
                LOG(f"  📝 教训跳过（{sym}）：trades.json 里配不上 trade_id={trade_id}"
                    f"（老单没有 trade_id，无法与标签体系联动）")
                return
            combo_mom = _mom_dir_of(op)
            combo_pos = _pos_zone_of(op)
            combo_reg = _regime_of(op)
            combo_src = _entry_src_of(op)
            # ⚠⚠ **必须 4 维**（10-05 01:0x 口径漂移 bug 的根因）：
            #   `_tag_stats()` 的桶键是 **4 维** "mom × pos × regime × entry_src"
            #   （实测桶键形如「顺动能 × 追高 × 其他 × touch」）。这里原来只
            #   拼 3 维，匹配时又只比桶键前 3 段（`[:3] == want`）→ **第 4 维
            #   被完全忽略**，后果有两层：
            #     ① 匹配错桶：同 3 维下若另有 ×deep / ×seq 的桶，`next()`
            #        只会取到**第一个**，战绩张冠李戴；
            #     ② 教训行丢掉了 entry_src，模型看到的信息不完整。
            #   实测事故：桶里是 n=5/wins=4/pnl=+0.244，教训行却写成
            #   「出现 3 次 | 胜率 100%（3/3）| 累计 +0.530U」——三个数全错，
            #   而这句话会直接进 brief 误导模型（「这类情况胜率 100%」）。
            combo = f"{combo_mom}×{combo_pos}×{combo_reg}×{combo_src}"
            # 该标签组合的累计战绩（n / 胜率 / 累计U）—— 直接取_tag_stats，
            # 与看板和提示词里那份同源同口径。
            # ⚠ 对齐坑：`_tag_stats` 的桶键用 **" × "**（带空格）分隔，本地的
            #   combo 用 "×"（无空格）→ 直接 `startswith` 匹配不上（实测会误报
            #   "本类首笔"）。所以按维度值**逐段**比对，且**必须比全 4 段**。
            st = _tag_stats(self.cfg["runtime_dir"])
            want = [x.strip() for x in combo.split("×")]
            hits = [b for k, b in st.get("buckets", {}).items()
                    if [x.strip() for x in k.split("×")] == want]
            agg = hits[0] if len(hits) == 1 else None
            if len(hits) > 1:
                # 4 维仍重名 = 桶键口径又变了，宁可不写数字也不能猜一个
                LOG(f"  ⚠ 教训战绩匹配到 {len(hits)} 个同 4 维桶，降级为无战绩："
                    f"{combo}")
            stat = "本类首笔"
            if agg:
                n_, w_, p_ = int(agg["n"]), int(agg["wins"]), float(agg["pnl"])
                # 自检：绝不允许输出不可能的数字（n<1 或 wins>n 立即降级）。
                # 这道闸是给"口径再漂一次"兜底的——宁可显示"暂无战绩"，
                # 也不能让模型读到「胜率 100%」这种把亏损单算成全胜的假象。
                if 0 < w_ <= n_ and n_ > 0:
                    stat = (f"出现 {n_} 次 | 胜率 {round(w_ * 100.0 / n_)}%"
                            f"（{w_}/{n_}）| 累计 {p_:+.3f}U")
                else:
                    LOG(f"  ⚠ 教训战绩自检不通过（n={n_} wins={w_}），降级为无战绩")
                    stat = "暂无战绩"
        except Exception as e:  # noqa: BLE001
            # 统计失败 ≠ 标签缺失。标签本身可能完全正常（只是 _tag_stats 读
            # 文件失败），此时**仍按正常标签写入**、只把战绩标成"暂无"，
            # 绝不因此写出「未知」假桶（那会把一条好数据糟蹋成噪音）。
            LOG(f"  ⚠ 教训战绩统计失败（{sym}），按正常标签写入无战绩："
                f"{str(e)[:80]}")
            if not combo_mom or not combo_pos:
                return
            combo = f"{combo_mom}×{combo_pos}×{combo_reg}×{combo_src}"
            stat = "暂无战绩"

        # ---- 标签必须配得上，否则不写聚合（10-05 23:1x bug 修复）----
        # 实测事故：XRP_USDT short -0.931U 那笔开在 20:0x 之前，其 open 记录
        # 里 trade_id/m1h_mom/band_pos/regime 全是 None → 配不上标签 → 落进
        # 一个叫「未知」的假桶，写出 `[聚合] 未知 | 暂无战绩 | ...`。
        # 「未知」是**假桶**，不是"数据缺失"的正常表现，危害有三：
        #   ① 污染统计：多个「未知」在 `_tag_stats` 分组里被当成一个真实类别；
        #   ② 误导模型：模型会把「未知」理解成一类真实行情，而不是"没数据"；
        #   ③ 显示成「暂无战绩」：既无标签也无胜率，纯噪音。
        # 正确行为：**配不上标签就不写聚合条目**。判据用 decision 的分类函数
        # 自己的"无数据"返回值（不另写判据，避免两处口径漂移）：
        #   _mom_dir_of / _pos_zone_of → 无数据字段时返回「无数据」
        #   _regime_of                  → 非 range/squeeze/wide 一律「其他」
        # ⚠ 关键：m1h_mom 与 band_pos 缺失 → 前两者返回「无数据」，此时**一定**
        #   是 20:0x 之前的老单（那两项是10-05 才加的），不是"行情真的没数据"。
        #   所以「无数据」出现即判定不可用，**不写聚合**（但教训正文仍要保留，
        #   见下面 lesson 生成之后的那段落盘——这里只把"是否入桶"记下来）。
        untaggable = (combo_mom == "无数据" or combo_pos == "无数据")

        reason = meta.get("reason") or ""
        base_q = (f"这笔交易刚平仓：{sym} {side}，盈亏 {tr['pnl']:+.3f}U"
                  f"（价格幅度 {tr.get('gain', 0):+.2%}）。"
                  + ("" if untaggable
                     else f"开仓时状态：{combo}。")
                  + f"开仓理由是「{reason}」。"
                  f"用一句话总结一条可执行教训（≤40字）：必须包含触发条件和动作，"
                  f"形如「<条件>时才<动作>」或「<条件>则<动作>」；"
                  f"禁止复述这笔的盈亏结局，不要空话。")
        # L4：模型调用也包起来。反思模型超时/限流是常态，不该冒泡到
        # engine._post_close（那里虽会兜住，但会刷无意义的警告日志，
        # 掩盖真正的告警）。这里吞掉即可——教训本来就是"锦上添花"，
        # 丢一条不该影响交易线程。
        try:
            lesson = self._ask_lesson(base_q)
            if lesson and not self._lesson_ok(lesson):
                lesson = self._ask_lesson(
                    base_q + "（上次不合格被打回：缺条件或缺动作，重写）")
        except Exception as e:  # noqa: BLE001
            LOG(f"  ⚠ 教训生成跳过（{sym}）：反思模型不可用 {str(e)[:60]}")
            return
        if not self._lesson_ok(lesson):
            LOG(f"  📝 教训质检不过（{sym}），丢弃：{str(lesson)[:40]}")
            return
        # ---- 标签配不上：只留正文、不入桶（10-05 23:1x bug 修复）----
        # 放在 lesson 生成**之后**：正文要先拿到手才有地方存。顺序反了会
        # 报 `cannot access local variable 'lesson'`（实测踩过）。
        if untaggable:
            LOG(f"  📝 教训不入桶（{sym} {side}）：开仓标签缺失（老单，"
                f"20:05 之前无 m1h_mom/band_pos）→ 不写「未知」假桶；"
                f"正文已存 lessons_untagged.md")
            self._append_untagged(tr, lesson)
            return
        # L3：同标签组合合并成一条（去重键 = 标签组合，不另写相似度逻辑）
        self._upsert_agg(combo, lesson, stat, tr)
        LOG(f"  📝 教训[{combo}]：{lesson[:36]}")

    def _ask_lesson(self, q: str) -> str:
        txt = llm.ask([{"role": "user", "content": q}], temp=0.3, max_tokens=100,
                      thinking=True, reflect=True)
        return (txt or "").strip().strip('"')

    def _append_untagged(self, tr: dict, lesson: str):
        """把「配不上标签」的教训正文单独落到lessons_untagged.md。

        为什么单独存、而不是并进 lessons.md（10-05 23:1x 用户拍板）：
          · **桶要丢**：假桶（"未知"）会污染 `_tag_stats` 分组、误导模型、
            显示成"暂无战绩"纯噪音 → 必须从 lessons.md 清掉。
          · **正文要留**：这教训记录了一次真实的判断失误（比如 XRP 那笔
            "须等价格实体跌破中轨才进场做空"），是有价值的知识，丢了可惜。
        两件事不矛盾：桶（结构化统计）与正文（知识）可以分开存。
        独立文件的好处：`_lessons()` 只读 lessons.md，所以**模型看不到这个
        文件**——正文留作档案/复盘用，不会以"没有战绩支撑的孤立规则"去
        干扰模型决策。这正是用户要的效果（教训不生成给模型看 → 但知识不丢）。
        行数封顶同 lessons（30 行），只追加不重写，异常全吞。
        """
        try:
            ts = datetime.now().strftime("%m-%d %H:%M")
            state.append_line(
                self.untagged_path,
                f"[未聚合] {ts} {tr.get('sym')} {tr.get('side')} "
                f"{float(tr.get('pnl') or 0):+.3f}U → {lesson}")
        except Exception as e:  # noqa: BLE001
            LOG(f"  ⚠ 未标签教训落盘失败：{str(e)[:60]}")

    def _lesson_ok(self, t: str | None) -> bool:
        if not t or len(t) <= 8:
            return False
        if any(w in t for w in _BAD):
            return False
        return any(w in t for w in _ACT) and any(w in t for w in _COND)

    # ---- L3/L4：聚合条目的读写 ----
    # 聚合行格式（单行，便于 decision._brief() 逐行取用与前端展示）：
    #   [聚合] <标签组合> | <战绩> | <时间> <sym> <side> <pnl>U → <教训>
    _AGG_RE = re.compile(r"^\[聚合\]\s*([^|]+?)\s*\|")

    def _read_lessons(self) -> list:
        """读 lessons.md 的**正文行**（自动跳过文件头说明行）。

        跳过头说明是必需的：`_write_lessons` 每次都会写那一行，若把它当成
        正文数据返回，`reset_lessons` 第二次调用就会把**自己的文件头**当成
        "老格式行"清掉（实测踩过：二次调用返回 1，且日志谎报"清理了 1 行"）。
        """
        try:
            with open(self.lessons_path, encoding="utf-8") as f:
                return [x.rstrip("\n") for x in f.readlines()
                        if x.strip() and not x.startswith("（只记录亏损单")]
        except Exception:
            return []

    def _upsert_agg(self, combo: str, lesson: str, stat: str, tr: dict):
        """把教训并入聚合条目：同 combo 替换掉旧行（并保留计数口径），否则新增。

        整个文件**全量重写**：lessons.md 只有 30 行量级（`_LESSON_MAX_LINES`
        封顶），重写成本远低于维护增量补丁的复杂度，且天然解决
        "老格式散乱行怎么淘汰"这个问题——见 `_write_lessons`。
        异常一律吞掉：教训写失败绝不能打断 review 线程。
        """
        try:
            lines = self._read_lessons()
            ts = datetime.now().strftime("%m-%d %H:%M")
            new = (f"[聚合] {combo} | {stat} | {ts} {tr.get('sym')} "
                   f"{tr.get('side')} {float(tr.get('pnl') or 0):+.3f}U → {lesson}")
            out, hit = [], False
            for ln in lines:
                m = self._AGG_RE.match(ln)
                if m and m.group(1).strip() == combo.strip():
                    if not hit:      # 同组合：替换（新的教训 + 新的战绩）
                        out.append(new)
                        hit = True
                    continue
                out.append(ln)
            if not hit:
                out.append(new)
            self._write_lessons(out)
            return True
        except Exception as e:  # noqa: BLE001
            LOG(f"  ⚠ 教训写入失败（不打断 review 线程）：{str(e)[:80]}")
            return False

    def _write_lessons(self, lines: list):
        """整体重写 lessons.md，并在文件头写一行口径说明。

        · **只保留聚合行**（老格式的散乱自由文本行在本次改造后不再有新增，
          留着只会让模型看到一堆无法与战绩联动的规则 → 一次性清掉）。
        · **剔除假桶**：标签组合为「未知」的行是 23:1x 那次 bug 的产物
          （标签配不上却硬写成桶），会污染统计、误导模型 → 这里一并过滤，
          即使还有残留也会被下一次写入清掉。
        · 只留**最后 `_LESSON_MAX_LINES` 行**（新→旧追加，旧的从头部淘汰），
          防止无限膨胀（91 行散乱行 → 约 30 行以内）。
        · 文件头说明：告诉模型这些是亏损单的条件教训、盈利单的结果去哪看。
        """
        keep = [x for x in lines
                if self._AGG_RE.match(x)
                and self._AGG_RE.match(x).group(1).strip() not in _FAKE_BUCKETS
                ][-self._LESSON_MAX_LINES:]
        with open(self.lessons_path, "w", encoding="utf-8") as f:
            f.write(_LESSON_HEADER)
            f.write("\n".join(keep))
            if keep:
                f.write("\n")

    def reset_lessons(self) -> int:
        """**一次性**清掉老格式教训行，只留文件头说明（幂等，可反复调）。

        为什么需要这个方法（10-05 用户拍板「清掉旧教训」）：`_write_lessons`
        只在**有新教训写入时**才会被调用。而聚合逻辑上线后如果一直没有
        亏损单平仓（例：当天平仓全是盈利单），老格式那 91 行就会一直留在
        文件里被拼进 brief —— 里面是自由文本、无 trade_id、无法与战绩联动，
        恰恰是这次改造要消灭的东西。所以需要一个**主动**触发清理的入口。

        · **不编造任何聚合条目**：本方法只做"清"，不写"写"。有没有教训
          由后续真实亏损单的 `_lesson()` 决定（L2：只对亏损单写）。
        · 幂等：已经只剩头说明时再调一次，结果不变（返回 0）。
        · 返回被清掉的老行数，便于日志/自检确认。
        """
        try:
            old = self._read_lessons()
            stale = [x for x in old if not self._AGG_RE.match(x)]
            keep = [x for x in old if self._AGG_RE.match(x)]
            if not stale:
                # 没有老行也要**重写一次文件头**：口径文案会随规则演进，
                # 光靠"有老行才写"会让旧口径的文件头一直留在盘上。
                try:
                    with open(self.lessons_path, encoding="utf-8") as f:
                        cur = f.read()
                    if not cur.startswith(_LESSON_HEADER):
                        self._write_lessons(keep)
                        LOG("  🧹 lessons.md 文件头口径已更新为最新规则")
                except Exception:
                    pass
                return 0
            self._write_lessons(keep)
            LOG(f"  🧹 已清理老格式教训 {len(stale)} 行（现仅保留聚合条目，"
                f"无聚合条目时只剩文件头说明）")
            return len(stale)
        except Exception as e:  # noqa: BLE001
            LOG(f"  ⚠ 教训清理失败（不影响交易）：{str(e)[:80]}")
            return 0

    # -------------------------------------------------- 甲：行情分段（读 scan，零额外行情请求）
    # ⚠ 10-05 已停算：`cycle()` 不再调用（`segments` 键也不再写出，见 _save）。
    #   保留函数体，随时可恢复调用。停算理由：零外部消费者，且 decision.py
    #   从不读它——scan.json 里已有更准的 `regime` 字段（每轮实时算）。
    def update_segments(self):
        scan = state.load(self.cfg["runtime_dir"] + "/scan.json", {})
        segs = {}
        for it in scan.get("items") or []:
            sym = it.get("contract") or ""
            if (it.get("close_out_up_bars") or 0) >= 2:
                segs[sym] = "trend_up"
            elif (it.get("close_out_dn_bars") or 0) >= 2:
                segs[sym] = "trend_dn"
            elif (it.get("bw_pct") is not None and it["bw_pct"] <= 0.15) or it.get("bb_in_kc"):
                segs[sym] = "squeeze"
            elif (it.get("bw_burst") or 0) >= 1.35 or (it.get("bw_pct") or 0) >= 0.85:
                segs[sym] = "expansion"
            else:
                segs[sym] = "range"
        d = self._load()
        d["segments"], d["t_seg"] = segs, time.time()
        self._save(d)

    # -------------------------------------------------- 乙：影子参数（只统计不切换）
    # ⚠ 10-05 已停算：`cycle()` 不再调用（`shadow`/`t_shadow` 键也不再写出）。
    #   保留函数体，随时可恢复调用。停算理由：零外部消费者，且这是 review
    #   线程唯一的重 IO —— 每小时 15 品种 × 120 根 15m K线，还要对每组影子
    #   参数逐根算布林轨道。停了把 CPU 直接让给 guard 线程（2s 一轮）。
    def update_shadow(self):
        from market import feeder
        out = {}
        for sym in self.cfg["universe"]:
            try:
                bars = feeder.closed_bars(sym, self.cfg["boll"]["hf_tf_min"],
                                          days=3, n=120)
            except Exception:
                continue
            closes = [b["c"] for b in bars]
            if len(closes) < 120:
                continue
            row = {}
            for p in self.cfg["review"]["shadow_params"]:
                n, k = int(p["n"]), float(p["k"])
                bw_sum, bw_cnt, touches, outc = 0.0, 0, 0, 0
                for i in range(n, len(closes) + 1):
                    m, u, dn = ind.boll_band(closes[:i], n, k)
                    if m <= 0:
                        continue
                    bw_sum += (u - dn) / m
                    bw_cnt += 1
                    c = closes[i - 1]
                    if c >= u or c <= dn:
                        outc += 1
                    # 触轨率：每根 bar 对自己当根的轨道（13:32 版误用末根轨道，已修）
                    bar = bars[i - 1]
                    if bar["h"] >= u or bar["l"] <= dn:
                        touches += 1
                row[f"{n}/{k:g}"] = {
                    "bw_avg": round(bw_sum / bw_cnt * 100, 3) if bw_cnt else 0,
                    "touch_pct": round(touches / bw_cnt * 100, 1) if bw_cnt else 0,
                    "close_out_pct": round(outc / bw_cnt * 100, 1) if bw_cnt else 0}
            if row:
                out[sym] = row
        # 残缺防护（14:46 事故）：成功率过半才允许覆盖，残缺保留旧数据下轮重试
        old = self._load().get("shadow") or {}
        if len(out) < max(1, len(self.cfg["universe"]) // 2):
            if old:
                LOG(f"  ⚠ 影子参数本轮仅 {len(out)} 品种成功（网络抖动），保留旧数据待重试")
                return
        d = self._load()
        d["shadow"], d["t_shadow"] = out, time.time()
        self._save(d)
        LOG(f"  🔁 复盘线程：影子参数统计 {len(out)} 品种 / 分段 {len(d['segments'])} 品种")

    # -------------------------------------------------- 上帝视角回看
    # ⚠ 10-05 已停算：`cycle()` 不再调用（`verdicts` 键也不再写出，
    #   所以本函数读不到待判队列，会在 `pend` 为空处直接返回——**要恢复
    #   必须同时把 on_close 里的 verdicts 入库那段一起恢复**，只调它无效）。
    #   保留函数体的理由：MFE/MAE 是唯一能回答「止损打掉的那笔、如果没有
    #   SL 会怎样」的信号，重建要重新写拉 K线 那套逻辑，成本高。
    def _god_view(self):
        from market import feeder
        d = self._load()
        # 用 .get 而非 d["verdicts"]：该键已停算、_save 不再写出，
        # 直接下标会 KeyError 让 review 线程抛异常。恢复调用前请先恢复
        # on_close 里的 verdicts 入库。
        pend = [v for v in (d.get("verdicts") or []) if v.get("god") is None]
        if not pend:
            return
        n_bars = int(self.cfg["review"]["god_view_bars"])
        for v in pend[:8]:
            try:
                bars = feeder.closed_bars(v["sym"], self.cfg["boll"]["hf_tf_min"],
                                          days=7, n=60)
            except Exception:
                continue
            fwd = [b for b in bars if int(b["t"]) >= (v["t"] - 900) * 1000]
            if len(fwd) < 5:                     # 平仓后行情走够再判，防提前下结论
                continue
            fwd = fwd[:n_bars]
            side, exit_px = v["side"], float(v.get("exit") or 0)
            if exit_px <= 0:
                v["god"] = {"err": "no_exit_px"}
                continue
            tp_hit, mfe, mae = None, 0.0, 0.0
            for j, b in enumerate(fwd):
                fav = ((b["h"] - exit_px) if side == "long" else (exit_px - b["l"])) / exit_px
                adv = ((exit_px - b["l"]) if side == "long" else (b["h"] - exit_px)) / exit_px
                mfe, mae = max(mfe, fav), max(mae, adv)
                if tp_hit is None and float(v.get("tp") or 0) and \
                        ((b["h"] >= v["tp"]) if side == "long" else (b["l"] <= v["tp"])):
                    tp_hit = j + 1
            v["god"] = {"bars": len(fwd), "tp_reached": tp_hit is not None,
                        "tp_in_bars": tp_hit, "mfe_pct": round(mfe * 100, 2),
                        "mae_pct": round(mae * 100, 2)}
            pnl = float(v.get("pnl") or 0)
            tag = ("早平" if tp_hit else "平得对") if pnl >= 0 else \
                  ("扛到止盈可回血" if tp_hit else "止损正确")
            LOG(f"  🔭 上帝视角 {v['sym']} {side} {pnl:+.3f}U → "
                f"TP{'可达(第%s根)' % tp_hit if tp_hit else '未达'} "
                f"MFE {mfe:.2%} / MAE {mae:.2%}（{tag}）")
        self._save(d)
