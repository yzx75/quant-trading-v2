# -*- coding: utf-8 -*-
"""strategy/regime.py — 行情定性层（ADX/+DI/−DI + 布林带宽 → regime 标签 + 档位）。

🔴 10-07 用户令「不要用布林带战法当唯一的了……状态机改为这些指标的计算，
   参考马丁的计算」→ 本模块由「纯布林量标签」重写为**收网 Seine 口径**，与
   `seine/strategy/regime.py` **逐根同源**。指标层见 `core/indicators.py`
   的等长序列版（boll_series / atr_series / adx_di，已与 seine 对拍逐值一致）。

判定（与 seine 一致）：
    bw = (up - dn) / mid
    regime = "range"
    if adx >= adx_trend:   regime = "up_trend" if pdi > ndi else "down_trend"
    elif bw > high_vol_bw: regime = "high_vol"
    bw < bw_min（轨不具参考性）→ regime = "low_vol"，meaningful=False

**只做定性、不做拦截**（沿用 10-02 用户令「不要加护栏」）：输出随 brief 喂模型，
代码不据它拒开、不改方向。

与老版的差异：
  · 旧版只有 breakout_up/dn、trend_up/dn、wide、squeeze、range 七种，全部由
    布林带宽/触轨推出，**不看趋势强度**；
  · 新版趋势/震荡的区分来自 ADX 与 +DI/−DI 的真实方向强度，布林只贡献带宽
    （high_vol 判定）与 setup_hint（触轨素材）。
"""
from __future__ import annotations

from dataclasses import dataclass

from core import indicators as ind

# regime → 档位（high_vol/low_vol 复用 range 档，与 seine 一致）
_TIER_OF = {
    "up_trend": "up_trend",
    "down_trend": "down_trend",
    "range": "range",
    "high_vol": "range",
    "low_vol": "range",
}


@dataclass
class RegimeResult:
    regime: str          # up_trend | down_trend | range | high_vol | low_vol
    tier: str            # 档位：up_trend | down_trend | range
    adx: float
    pdi: float
    ndi: float
    atr: float
    bandwidth: float
    setup_hint: str      # 布林触轨：band_fade_long / band_fade_short / trend_cont / range_inside
    meaningful: bool     # 带宽 ≥ bw_min（轨道具参考性）


def _rcfg(cfg: dict) -> dict:
    return (cfg or {}).get("regime") or {}


def tier_of(regime: str, cfg: dict) -> str:
    """regime 标签 → 档位（high_vol/low_vol 复用 range）。

    v2 目前没有「按档位锁参数」体系，故无 regime_tiers 时直接返回映射；
    有显式档位表时按其存在性收敛（与 seine 行为一致，便于将来接档位参数）。
    """
    t = _TIER_OF.get(regime, "range")
    tiers = (cfg or {}).get("regime_tiers")
    if tiers:
        return t if t in tiers else "range"
    return t


def _bw(up, dn, mid) -> float:
    return (up - dn) / mid if mid else 0.0


def regime_at(bars: list, i: int, cfg: dict, _boll=None, _adx=None) -> dict:
    """第 i 根的原始判定。返回 {regime, adx, pdi, ndi, bandwidth, ok}。

    `_boll=(mids,ups,dns)` / `_adx=(adxs,pdis,ndis)` 为可选预计算数组
    （避免同一轮里重复 O(n) 重算）。ok=False 表示该根不可判定。
    """
    reg = _rcfg(cfg)
    adx_trend = float(reg.get("adx_trend", 25))
    high_vol_bw = float(reg.get("high_vol_bw", 0.03))
    bw_min = float(reg.get("bw_min", 0.004))

    boll_cfg = (cfg or {}).get("boll") or {}
    n = int(boll_cfg.get("n", 20))
    k = float(boll_cfg.get("k", 2.0))
    if _boll is not None:
        mids, ups, dns = _boll
    else:
        mids, ups, dns = ind.boll_series([b["c"] for b in bars], n, k)
    if _adx is not None:
        adxs, pdis, ndis = _adx
    else:
        adxs, pdis, ndis = ind.adx_di(bars, 14)

    if i < 0 or i >= len(bars) or mids[i] is None or adxs[i] is None:
        return {"regime": "low_vol", "adx": 0.0, "pdi": 0.0, "ndi": 0.0,
                "bandwidth": 0.0, "ok": False}
    bw = _bw(ups[i], dns[i], mids[i])
    if bw < bw_min:
        return {"regime": "low_vol", "adx": float(adxs[i]), "pdi": float(pdis[i]),
                "ndi": float(ndis[i]), "bandwidth": bw, "ok": False}
    regime = "range"
    if adxs[i] >= adx_trend:
        regime = "up_trend" if pdis[i] > ndis[i] else "down_trend"
    elif bw > high_vol_bw:
        regime = "high_vol"
    return {"regime": regime, "adx": float(adxs[i]), "pdi": float(pdis[i]),
            "ndi": float(ndis[i]), "bandwidth": bw, "ok": True}


def _setup_hint(bars: list, i: int, cfg: dict, regime: str, _boll=None) -> str:
    """布林触轨提示（纯素材，不参与任何拦截）。"""
    boll_cfg = (cfg or {}).get("boll") or {}
    n = int(boll_cfg.get("n", 20))
    k = float(boll_cfg.get("k", 2.0))
    if _boll is not None:
        mids, ups, dns = _boll
    else:
        mids, ups, dns = ind.boll_series([b["c"] for b in bars], n, k)
    if i < 0 or i >= len(bars) or mids[i] is None:
        return ""
    b = bars[i]
    if b["l"] <= dns[i] and b["c"] > dns[i]:
        return "band_fade_long"
    if b["h"] >= ups[i] and b["c"] < ups[i]:
        return "band_fade_short"
    if regime in ("up_trend", "down_trend"):
        return "trend_cont"
    return "range_inside"


def classify(bars: list, cfg: dict) -> RegimeResult:
    """对最后一根已收盘 bar 定性，返回 RegimeResult。

    bars 不足（< 2n+1 根，ADX 出不来）时返回低置信结果（meaningful=False），不抛错。
    布林/ADX 只算一次，regime 与 hint 共用，避免同轮重复 O(n) 计算。
    """
    if not bars:
        return RegimeResult("low_vol", "range", 0.0, 0.0, 0.0, 0.0, 0.0, "", False)
    i = len(bars) - 1
    boll_cfg = (cfg or {}).get("boll") or {}
    n = int(boll_cfg.get("n", 20))
    k = float(boll_cfg.get("k", 2.0))
    _boll = ind.boll_series([b["c"] for b in bars], n, k)
    _adx = ind.adx_di(bars, 14)
    r = regime_at(bars, i, cfg, _boll=_boll, _adx=_adx)
    regime = r["regime"] if r["ok"] else "low_vol"
    hint = _setup_hint(bars, i, cfg, regime, _boll=_boll) if r["ok"] else ""
    return RegimeResult(
        regime=regime,
        tier=tier_of(regime, cfg),
        adx=r["adx"], pdi=r["pdi"], ndi=r["ndi"],
        atr=ind.atr_last(bars, 14), bandwidth=r["bandwidth"],
        setup_hint=hint, meaningful=bool(r["ok"]),
    )
