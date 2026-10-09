# -*- coding: utf-8 -*-
"""
strategy/boll.py — 布林带「末次触轨」状态机（唯一战法，高频周期由 hf_tf_min 定）

状态流：沿轨滑行（观察）→ 候选 cand →（喂给决策者确认方向）→ 开仓
候选产生五条路径（A/B/C 继承旧系统实盘拍板；D/E 10-01 补齐 12 形态缺口）：
  A. 末次触轨：本次触轨极值比上次抬高（多）/降低（空）+ 收盘收回轨内
  B. 首触直开：中轨走平（区间市）+ 首次触轨 + 收盘收回 + 非三连阴/三连阳
  C. 深砸衰竭：实体深砸穿轨 ≥2×ATR 后收回轨内（垂直脉冲衰竭）
  D. 浅假突破序列：6 根内 ≥2 次冲出轨但收盘收回（未深穿）= 连续打针衰竭
  E. W底/M头：15 根内两探轨附近 + 颈线突破（第二探与第一探差 ≤0.5×ATR）
作废规则：破候选极值 / 同根 bar 双触轨（带宽绞肉）/ 过期 / 序列重置。
标注（只喂决策者，代码不据此开仓）：带宽分位/张口、BB⊂KC 超级缩口、
滑行计数、轨外强趋势计数、三推力衰竭。
"""
from __future__ import annotations

from core import indicators as ind


class BollStateMachine:
    """每个 (lane, contract) 一个实例；逐根收盘 bar 喂入。
    lane: "hf"(15m) / "trend"(4H)，仅用于状态隔离命名。"""

    def __init__(self, cfg: dict):
        self.n = int(cfg.get("n", 20))
        self.k = float(cfg.get("k", 2.0))
        self.cand_ttl_bars = int(cfg.get("cand_ttl_bars", 4))
        self.seq_gap_bars = int(cfg.get("seq_gap_bars", 6))
        self.deep_atr = float(cfg.get("deep_atr", 2.0))
        self.flat_range = float(cfg.get("flat_range", 0.25))
        # 单根 bar 秒数（10-07 02:2x 修）：原代码 6 处写死 `15 * 60` / `900000`，
        #   周期改 1H 后全部语义错乱（TTL/断档/序列/时效窗口会按 15 分钟折算）。
        #   改为从 hf_tf_min 派生，缺省 15 保持逐字节回退。
        self.bar_s = max(1, int(cfg.get("hf_tf_min", 15))) * 60
        # 状态
        self.last_bar = {}       # sym -> bar_t(ms)
        self.prev = {}           # sym -> {"long": 极值, "long_t": bar_t, "short":..., "short_t":...}
        self.deep = {}           # sym -> {"long"/"short": {"level","bar_t"}}
        self.walk = {}           # sym -> 滑行/轨外/三推力计数
        self.bw = {}             # sym -> 带宽标注
        self.band = {}           # sym -> 最新收盘根轨道快照 {"mid","up","dn","atr","bt"}
        self.cand = None         # 当前处理品种的候选视图（真值见 self.cands）
        self.cands: dict = {}    # {sym: cand} 按品种独立存（10-03 修：原全局单槽会互清）

    # -------------------------------------------------- 基础
    def _atr(self, bars, tail=14):
        return ind.atr(bars, tail)

    def is_flat_mid(self, closes, atr15):
        """中轨走平：最近 4 根中轨极差 ≤ flat_range×ATR（区间市）"""
        if len(closes) < self.n + 3 or atr15 <= 0:
            return False
        ms = [ind.boll_band(closes[:len(closes) - kk], self.n, self.k)[0]
              for kk in (3, 2, 1, 0)]
        return (max(ms) - min(ms)) <= self.flat_range * atr15

    # -------------------------------------------------- 喂 bar
    def feed(self, sym: str, bars: list):
        """喂入含最新收盘根的 bars 序列。返回 (cand or None, "same" or "new")"""
        closes = [b["c"] for b in bars]
        if len(closes) < self.n + 2:
            return None, "same"
        bt = int(bars[-1]["t"])
        # ⚠ 10-03 01:15 修复（第三路审查①，确认为 bug）：`self.cand` 原本是**全局单槽**，
        #   喂 A 品种的 bar 会把 A 的候选顶掉/清空，喂 B 品种时 B 品种又可能把A 的
        #   候选清成None（同方向时 `(self.cand or {}).get("side")==side` 判定成立），
        #   且 `if not self.cand: return`（199 行）会让**别的品种的候选挡住本品种的
        #   D/E 路径**。→ 15 品种高频候选随机丢失/被抑制，与 trend.py 00:25 修掉的
        #   "全池单槽 cand" 是**完全同型**的病灶。
        #   修法（最小侵入）：真值改存 `self.cands[sym]`，`self.cand` 只作为「当前
        #   处理品种」的读写视图——下面全部 18 处候选逻辑一字未改。
        self.cand = self.cands.get(sym)      # 载入本品种候选视图
        if bt <= int(self.last_bar.get(sym) or 0):
            return self.cand, "same"
        self.last_bar[sym] = bt
        mid, up, dn = ind.boll_band(closes, self.n, self.k)
        bar = bars[-1]
        atrv = self._atr(bars)

        # 轨道快照（SL/TP 取开仓时最新收盘根轨道）
        self.band[sym] = {"mid": mid, "up": up, "dn": dn, "atr": atrv, "bt": bt}

        # 带宽标注：分位（≤15% 缩口蓄势）+ 张口（较上根 ≥1.35× 变盘启动）+ BB⊂KC
        bws = []
        for kk in range(max(self.n, len(closes) - 120), len(closes) + 1):
            m2, u2, d2 = ind.boll_band(closes[:kk], self.n, self.k)
            bws.append(max(u2 - d2, 1e-12) / m2)
        w = self.bw.setdefault(sym, {})
        w["bw_prev"] = float(w.get("bw") or 0)
        w["bw"] = max(up - dn, 1e-12) / mid
        w["pct"] = (sum(1 for x in bws if x <= w["bw"]) / len(bws)) if bws else 1.0
        w["burst"] = (w["bw"] / w["bw_prev"]) if w["bw_prev"] > 0 else 0.0
        kc_m = ind.ema(closes, 20)
        w["kc_in"] = bool(kc_m and atrv > 0 and (up - dn) <= 1.2 * atrv
                          and up < kc_m + 1.5 * atrv and dn > kc_m - 1.5 * atrv)

        # C 路径：深砸衰竭
        dp = self.deep.setdefault(sym, {})
        if bar["c"] < dn - self.deep_atr * atrv:
            dp["long"] = {"level": bar["l"], "bar_t": bt}
        elif dp.get("long"):
            if bar["c"] > dn:
                self.cand = {"sym": sym, "side": "long", "level": float(dp["long"]["level"]),
                             "bar_t": bt, "deep": True}
                dp.pop("long", None)
            elif bt - int(dp["long"].get("bar_t") or bt) > self.cand_ttl_bars * self.bar_s:
                dp.pop("long", None)
        if bar["c"] > up + self.deep_atr * atrv:
            dp["short"] = {"level": bar["h"], "bar_t": bt}
        elif dp.get("short"):
            if bar["c"] < up:
                self.cand = {"sym": sym, "side": "short", "level": float(dp["short"]["level"]),
                             "bar_t": bt, "deep": True}
                dp.pop("short", None)
            elif bt - int(dp["short"].get("bar_t") or bt) > self.cand_ttl_bars * self.bar_s:
                dp.pop("short", None)

        pv = self.prev.setdefault(sym, {})
        ttl = self.seq_gap_bars * self.bar_s   # 断档重置窗口（按 bar 折算秒）
        # A/B 路径：下轨序列（做多向）
        if bar["l"] <= dn:
            pl = pv.get("long")
            if pl is not None and bar["l"] > pl:
                if bar["c"] > dn:             # 结构抬高 + 收回 = 候选
                    self.cand = {"sym": sym, "side": "long", "level": bar["l"], "bar_t": bt}
                elif (self.cand or {}).get("side") == "long":
                    self.cand = None          # 抬高但实体砸轨 = 仍在滑行，作废
                pv["long"], pv["long_t"] = bar["l"], bt
            else:
                first = pl is None or bt - float(pv.get("long_t") or 0) > ttl
                pv["long"], pv["long_t"] = bar["l"], bt
                if (self.cand or {}).get("side") == "long":
                    self.cand = None          # 破候选低点 = 作废
                mono = len(closes) >= 3 and closes[-1] < closes[-2] < closes[-3]
                if first and bar["c"] > dn and self.is_flat_mid(closes, atrv) and not mono:
                    self.cand = {"sym": sym, "side": "long", "level": bar["l"],
                                 "bar_t": bt, "first": True}
        # A/B 路径：上轨序列（做空向）
        if bar["h"] >= up:
            ph = pv.get("short")
            if ph is not None and bar["h"] < ph:
                if bar["c"] < up:
                    self.cand = {"sym": sym, "side": "short", "level": bar["h"], "bar_t": bt}
                elif (self.cand or {}).get("side") == "short":
                    self.cand = None
                pv["short"], pv["short_t"] = bar["h"], bt
            else:
                first = ph is None or bt - float(pv.get("short_t") or 0) > ttl
                pv["short"], pv["short_t"] = bar["h"], bt
                if (self.cand or {}).get("side") == "short":
                    self.cand = None
                mono = len(closes) >= 3 and closes[-1] > closes[-2] > closes[-3]
                if first and bar["c"] < up and self.is_flat_mid(closes, atrv) and not mono:
                    self.cand = {"sym": sym, "side": "short", "level": bar["h"],
                                 "bar_t": bt, "first": True}

        # 滑行 / 轨外强趋势 / 三推力（只作标注）
        wk = self.walk.setdefault(sym, {})
        if bar["c"] < mid:
            wk["dn"] = int(wk.get("dn") or 0) + 1
            wk["dn_touch"] = bool(wk.get("dn_touch") or bar["l"] <= dn)
        else:
            wk["dn"], wk["dn_touch"] = 0, False
        if bar["c"] > mid:
            wk["up"] = int(wk.get("up") or 0) + 1
            wk["up_touch"] = bool(wk.get("up_touch") or bar["h"] >= up)
        else:
            wk["up"], wk["up_touch"] = 0, False
        wk["up_out"] = int(wk.get("up_out") or 0) + 1 if bar["c"] > up else 0
        wk["dn_out"] = int(wk.get("dn_out") or 0) + 1 if bar["c"] < dn else 0
        if bar["h"] >= up:
            if not wk.get("up_wick"):
                wk["up_push"] = int(wk.get("up_push") or 0) + 1
            wk["up_wick"] = True
        else:
            wk["up_wick"] = False
        if bar["c"] < mid:
            wk["up_push"] = 0
        if bar["l"] <= dn:
            if not wk.get("dn_wick"):
                wk["dn_push"] = int(wk.get("dn_push") or 0) + 1
            wk["dn_wick"] = True
        else:
            wk["dn_wick"] = False
        if bar["c"] > mid:
            wk["dn_push"] = 0

        # 触轨→回中轨序列（10-02 用户令补齐判据缺口）：记录最近一次触轨的方向与时刻，
        # 之后每根检查是否收盘收回中轨另一侧。touch_mid=false = 还挂在轨外没回来
        # （趋势/张口未走完，别猜头）；true = 已回来过（回归结构成立）。
        if bar["h"] >= up:
            wk["t_dir"], wk["t_bt"], wk["t_mid"] = "up", bt, False
        elif bar["l"] <= dn:
            wk["t_dir"], wk["t_bt"], wk["t_mid"] = "dn", bt, False
        elif wk.get("t_dir") and not wk.get("t_mid"):
            if (wk["t_dir"] == "up" and bar["c"] < mid) or \
                    (wk["t_dir"] == "dn" and bar["c"] > mid):
                wk["t_mid"] = True

        # D/E 路径（10-01 用户拍板补齐 12 形态缺口，无状态窗口扫描）：
        # D. 浅假突破序列：6 根内 ≥2 次冲出轨但收盘收回（未深穿）= 连续打针衰竭
        # E. W底/M头：15 根内两探轨附近（第二探不低/高于第一探 0.5×ATR）+ 颈线突破
        win = bars[-8:-1]
        fs_up_n = sum(1 for b in win if b["h"] >= up and b["c"] < up)
        fs_dn_n = sum(1 for b in win if b["l"] <= dn and b["c"] > dn)
        if bar["h"] >= up:
            fs_up_n += 1
        if bar["l"] <= dn:
            fs_dn_n += 1
        wk["fs_up_n"], wk["fs_dn_n"] = fs_up_n, fs_dn_n
        fresh_seq = not (wk.get("seq_t") and bt - int(wk["seq_t"]) <= 4 * self.bar_s)
        if not self.cand:
            if bar["h"] >= up and bar["c"] < up and fs_up_n >= 2 and fresh_seq:
                self.cand = {"sym": sym, "side": "short", "level": bar["h"],
                             "bar_t": bt, "seq": True}
                wk["seq_t"] = bt
            elif bar["l"] <= dn and bar["c"] > dn and fs_dn_n >= 2 and fresh_seq:
                self.cand = {"sym": sym, "side": "long", "level": bar["l"],
                             "bar_t": bt, "seq": True}
                wk["seq_t"] = bt
            else:
                cand_wm = None
                winb = bars[-15:]
                lo = min(range(len(winb)), key=lambda i: winb[i]["l"])
                hi = max(range(len(winb)), key=lambda i: winb[i]["h"])
                if winb[lo]["l"] <= dn:
                    l2s = [j for j in range(lo + 1, len(winb))
                           if abs(winb[j]["l"] - winb[lo]["l"]) <= 0.5 * atrv]
                    if l2s:
                        j = l2s[0]
                        if j - lo >= 2 and bar["c"] > mid:
                            neck = max(b["h"] for b in winb[lo:j + 1])
                            if bar["c"] > neck:
                                cand_wm = {"sym": sym, "side": "long", "level": winb[j]["l"],
                                           "bar_t": bt, "wm": "W"}
                if cand_wm is None and winb[hi]["h"] >= up:
                    h2s = [j for j in range(hi + 1, len(winb))
                           if abs(winb[j]["h"] - winb[hi]["h"]) <= 0.5 * atrv]
                    if h2s:
                        j = h2s[0]
                        if j - hi >= 2 and bar["c"] < mid:
                            neck = min(b["l"] for b in winb[hi:j + 1])
                            if bar["c"] < neck:
                                cand_wm = {"sym": sym, "side": "short", "level": winb[j]["h"],
                                           "bar_t": bt, "wm": "M"}
                fresh_wm = not (wk.get("wm_t") and bt - int(wk["wm_t"]) <= 4 * self.bar_s)
                if cand_wm and fresh_wm:
                    self.cand = cand_wm
                    wk["wm_t"] = bt

        # 候选兜底作废：同根双触轨 / 过期
        c = self.cand
        if c and c.get("sym") == sym:
            if (bar["l"] <= dn and bar["h"] >= up) or \
                    bt - int(c.get("bar_t") or bt) > self.cand_ttl_bars * self.bar_s:
                self.cand = None
        c = self.cand
        # 写回本品种的候选（10-03：真值存 per-sym 槽，全局 self.cand 仅作视图）
        if c and c.get("sym") == sym:
            self.cands[sym] = c
        else:
            self.cands.pop(sym, None)
        return (c if (c and c.get("sym") == sym) else None), "new"

    # -------------------------------------------------- 确认与标注
    def confirm_ready(self, sym: str, px: float) -> bool:
        """中轨确认：多单要求 px>中轨且中轨不降；空单镜像"""
        b = self.band.get(sym)
        if not b or px <= 0:
            return False
        return (px > b["mid"] and b["mid"] >= b.get("mid_pv", b["mid"])) \
            if (self.cand or {}).get("side") == "long" else \
            (px < b["mid"] and b["mid"] <= b.get("mid_pv", b["mid"]))

    def snapshot_band(self, sym: str, closes) -> dict:
        """带上一根中轨对比的轨道快照（确认判定与 SL/TP 共用）"""
        mid, up, dn = ind.boll_band(closes, self.n, self.k)
        mid_pv = ind.boll_band(closes[:-1], self.n, self.k)[0] if len(closes) > self.n else mid
        b = dict(self.band.get(sym) or {})
        b.update({"mid": mid, "up": up, "dn": dn, "mid_pv": mid_pv})
        self.band[sym] = b
        return b

    def annotations(self, sym: str, closes, px: float, bars=None) -> dict:
        """喂给决策者的结构化标注"""
        w = self.walk.get(sym) or {}
        bw = self.bw.get(sym) or {}
        # 10-03 13:14：band_pos=价格在带内的相对位置（0=下轨 0.5=中轨 1=上轨）。
        #   提示词「开仓位置铁律」的唯一判据。实证（237 笔）：顺势回调型胜率仅 46%、
        #   逆势高位型 68% —— 均值回归必须逆着价格已走的方向做。
        b = self.band.get(sym) or {}
        try:
            _up, _dn = float(b.get("up") or 0), float(b.get("dn") or 0)
            _bp = round((float(px) - _dn) / (_up - _dn), 2) if _up > _dn > 0 else 0.5
        except Exception:
            _bp = 0.5
        ann = {
            "band_pos": _bp,
            "bw_pct": round(float(bw.get("pct") or 1.0), 2),
            "bw_burst": round(float(bw.get("burst") or 0), 2),
            "bb_in_kc": bool(bw.get("kc_in")),
            "walk_up_bars": int(w.get("up") or 0),
            "walk_dn_bars": int(w.get("dn") or 0),
            "walk_up_touch": bool(w.get("up_touch")),
            "walk_dn_touch": bool(w.get("dn_touch")),
            "close_out_up_bars": int(w.get("up_out") or 0),
            "close_out_dn_bars": int(w.get("dn_out") or 0),
            "three_push_up": int(w.get("up_push") or 0),
            "three_push_dn": int(w.get("dn_push") or 0),
            "fake_up_n": int(w.get("fs_up_n") or 0),
            "fake_dn_n": int(w.get("fs_dn_n") or 0),
        }
        # ---- 触轨→回中轨序列（10-02 用户令补齐判据缺口）----
        t_dir = w.get("t_dir") or ""
        if t_dir:
            t_bt = int(w.get("t_bt") or 0)
            bars_since = max(0, (int(bars[-1]["t"]) - t_bt) // (self.bar_s * 1000)) \
                if (bars and t_bt) else -1
            ann["last_touch"] = t_dir
            ann["touch_mid"] = bool(w.get("t_mid"))
            ann["bars_since_touch"] = bars_since
        # ---- W底/M头两脚位置（判据=第二脚够不够得到轨道；复刻 feed 里 E 路径口径）----
        if bars and len(bars) >= 6:
            winb = bars[-15:]
            atrv = self._atr(bars) or 1e-9
            lo = min(range(len(winb)), key=lambda i: winb[i]["l"])
            hi = max(range(len(winb)), key=lambda i: winb[i]["h"])
            l1 = float(winb[lo]["l"])
            l2s = [j for j in range(lo + 1, len(winb))
                   if abs(winb[j]["l"] - l1) <= 0.5 * atrv]
            h1 = float(winb[hi]["h"])
            h2s = [j for j in range(hi + 1, len(winb))
                   if abs(winb[j]["h"] - h1) <= 0.5 * atrv]
            mid, up, dn = ind.boll_band(closes, self.n, self.k)
            legs = {}
            # 只在第一脚确实触轨外时才给（控制噪声），ok=第二脚是否缩回带内=衰竭确认。
            # 过滤①两脚至少隔 2 根；②两脚之间要有实质回抽（≥0.5×ATR 离开第一脚）——
            # 否则只是同一波段的相邻 K 线，不是双底/双顶（与 feed 里 E 路径口径对齐）。
            def _pullback(ia, ib, px1, is_low):
                for b_ in winb[ia:ib]:
                    if is_low and b_["h"] >= px1 + 0.5 * atrv:
                        return True
                    if (not is_low) and b_["l"] <= px1 - 0.5 * atrv:
                        return True
                return False
            if l2s and l1 <= dn:
                j2 = l2s[0]
                if j2 - lo >= 2 and _pullback(lo, j2, l1, True):
                    l2 = float(winb[j2]["l"])
                    legs["w"] = {"l1": round(l1, 8), "l2": round(l2, 8),
                                 "ok": bool(l2 > dn), "gap": int(j2 - lo)}
            if h2s and h1 >= up:
                j2 = h2s[0]
                if j2 - hi >= 2 and _pullback(hi, j2, h1, False):
                    h2 = float(winb[j2]["h"])
                    legs["m"] = {"h1": round(h1, 8), "h2": round(h2, 8),
                                 "ok": bool(h2 < up), "gap": int(j2 - hi)}
            if legs:
                ann["legs"] = legs
        return ann
