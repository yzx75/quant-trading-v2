# -*- coding: utf-8 -*-
"""纯函数指标库：布林带 / ATR / EMA / RSI / MACD。

两套口径，别混用：
  · **尾部标量**（sma / boll_band / atr / rsi / macd）—— 只取最近 n 根，返回一个数。
    老布林战法与风控在用，勿改。
  · **等长序列**（boll_series / atr_series / atr_last / adx_di）—— 返回与 bars 等长
    的列表（前若干根为 None），供行情定性层逐根判定用。算法与收网 Seine 引擎
    （seine/core/indicators.py）**逐值同源**（Wilder 平滑），便于两引擎对拍。

bars 格式统一：{"t": 毫秒, "o","h","l","c","v": float}"""


def sma(cl, n):
    n = min(n, len(cl))
    return sum(cl[-n:]) / n if n else 0.0


def ema(cl, n):
    if not cl:
        return 0.0
    k = 2 / (n + 1)
    e = cl[0]
    for c in cl[1:]:
        e = c * k + e * (1 - k)
    return e


def stdev_window(cl, n):
    n = min(n, len(cl))
    w = cl[-n:]
    m = sum(w) / len(w)
    return (sum((c - m) ** 2 for c in w) / len(w)) ** 0.5 if w else 0.0


def boll_band(closes, n=20, k=2.0):
    """BOLL(n,k) → (mid, up, dn)，取最近 n 根收盘"""
    if len(closes) < 2:
        return 0.0, 0.0, 0.0
    n = min(n, len(closes))
    w = closes[-n:]
    mid = sum(w) / len(w)
    sd = (sum((c - mid) ** 2 for c in w) / len(w)) ** 0.5
    return mid, mid + k * sd, mid - k * sd


def atr(bars, n=14):
    """平均真实波幅（尾部 n 根）"""
    if len(bars) < 2:
        return 0.0
    trs = []
    for i in range(max(1, len(bars) - n), len(bars)):
        pc = bars[i - 1]["c"]
        trs.append(max(bars[i]["h"] - bars[i]["l"],
                       abs(bars[i]["h"] - pc), abs(bars[i]["l"] - pc)))
    return sum(trs) / len(trs)


def rsi(cl, n=14):
    if len(cl) < n + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(-n, 0):
        d = cl[i] - cl[i - 1]
        gains += max(d, 0)
        losses += max(-d, 0)
    if losses == 0:
        return 100.0
    rs = (gains / n) / (losses / n)
    return 100 - 100 / (1 + rs)


def macd(cl, fast=12, slow=26, sig=9):
    """返回 (dif, dea, hist)"""
    if len(cl) < slow + sig:
        return 0.0, 0.0, 0.0

    def _ema_series(vals, n):
        k = 2 / (n + 1)
        out = [vals[0]]
        for v in vals[1:]:
            out.append(v * k + out[-1] * (1 - k))
        return out

    ef = _ema_series(cl, fast)
    es = _ema_series(cl, slow)
    dif = [a - b for a, b in zip(ef, es)]
    dea = _ema_series(dif, sig)
    return dif[-1], dea[-1], (dif[-1] - dea[-1]) * 2


# ================================================================ 等长序列口径
# 移植自 seine/core/indicators.py（10-07 用户令：行情定性改用收网 Seine 的指标口径）。
# 与上面的尾部标量版**并存不冲突**：老战法/风控读上面那批，新定性层读下面这批。

def true_ranges(bars):
    """逐根真实波幅 TR（第 0 根用 h-l，其余含前收缺口）。"""
    trs = []
    for i, b in enumerate(bars):
        if i == 0:
            trs.append(b["h"] - b["l"])
            continue
        pc = bars[i - 1]["c"]
        trs.append(max(b["h"] - b["l"], abs(b["h"] - pc), abs(b["l"] - pc)))
    return trs


def boll_series(closes, n=20, k=2.0):
    """BOLL(n,k) → (mids, ups, dns) 三个等长列表，前 n-1 根为 None。

    与 seine/core/indicators.py::boll **逐值同源**（总体标准差，除以 n 而非 n-1）。
    """
    mids, ups, dns = [], [], []
    for i in range(len(closes)):
        if i + 1 < n:
            mids.append(None); ups.append(None); dns.append(None); continue
        win = closes[i + 1 - n:i + 1]
        m = sum(win) / n
        sd = (sum((x - m) ** 2 for x in win) / n) ** 0.5
        mids.append(m); ups.append(m + k * sd); dns.append(m - k * sd)
    return mids, ups, dns


def atr_series(bars, n=14):
    """**Wilder ATR** 等长列表，前 n 根 None（与尾部标量版 atr 的算法不同，勿混）。

    注意：与上面 `atr()`（简单均值）是两套算法；本函数对齐 seine 口径。
    """
    trs = true_ranges(bars)
    out = [None] * len(bars)
    if len(bars) < n + 1:
        return out
    s = sum(trs[1:n + 1]) / n
    out[n] = s
    for i in range(n + 1, len(bars)):
        s = (s * (n - 1) + trs[i]) / n
        out[i] = s
    return out


def atr_last(bars, n=14) -> float:
    """最新一根的 Wilder ATR（取不到返回 0.0）。"""
    for v in reversed(atr_series(bars, n)):
        if v is not None:
            return float(v)
    return 0.0


def adx_di(bars, n=14):
    """Wilder ADX / +DI / −DI，返回三个等长列表，前 (2n-1) 根为 None。

    与 seine/core/indicators.py::adx_di **逐值同源**，两引擎可对拍。
    需要 bars 至少 2n+1 根（n=14 时 29 根）才有首个 ADX。
    """
    L = len(bars)
    adx = [None] * L
    pdi = [None] * L
    ndi = [None] * L
    if L < 2 * n + 1:
        return adx, pdi, ndi
    trs = true_ranges(bars)
    pdm, ndm = [], []
    for i in range(L):
        if i == 0:
            pdm.append(0.0); ndm.append(0.0); continue
        up = bars[i]["h"] - bars[i - 1]["h"]
        dn = bars[i - 1]["l"] - bars[i]["l"]
        pdm.append(up if (up > dn and up > 0) else 0.0)
        ndm.append(dn if (dn > up and dn > 0) else 0.0)

    def wilder(vals, start):
        out = [None] * L
        s = sum(vals[start:start + n])
        out[start + n - 1] = s
        for i in range(start + n, L):
            s = s - s / n + vals[i]
            out[i] = s
        return out

    atr_s = wilder(trs, 1)
    p_s = wilder(pdm, 1)
    n_s = wilder(ndm, 1)
    dx = [None] * L
    for i in range(L):
        if atr_s[i] and atr_s[i] > 0 and p_s[i] is not None and n_s[i] is not None:
            pdi[i] = 100.0 * p_s[i] / atr_s[i]
            ndi[i] = 100.0 * n_s[i] / atr_s[i]
            denom = pdi[i] + ndi[i]
            dx[i] = 100.0 * abs(pdi[i] - ndi[i]) / denom if denom > 0 else 0.0
    first = next((i for i in range(L) if dx[i] is not None), None)
    if first is None:
        return adx, pdi, ndi
    idx = first + n - 1
    if idx >= L:
        return adx, pdi, ndi
    s = sum(dx[first:first + n]) / n
    adx[idx] = s
    for i in range(idx + 1, L):
        s = (s * (n - 1) + dx[i]) / n
        adx[i] = s
    return adx, pdi, ndi
