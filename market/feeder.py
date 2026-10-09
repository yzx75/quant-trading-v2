# -*- coding: utf-8 -*-
"""
market/feeder.py — 行情数据层：K 线（缓存）、现价、资金费率
K线缓存：同周期同一根未收盘 bar 的历史部分不变，45 秒内直接命中（旧系统实测可行）。
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request

from core.config import load_env

load_env()
BASE = "https://api.gateio.ws/api/v4/futures/usdt"

_TF_MAP = {1: "1m", 5: "5m", 15: "15m", 30: "30m", 60: "1h", 120: "2h",
           240: "4h", 480: "8h", 1440: "1d"}

_opener = urllib.request.build_opener()          # 跟随系统代理（行情域名直连常被墙）
_opener_direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
_cache: dict = {}          # (contract, tf) -> (expires, bars)
CACHE_TTL = 45             # 秒


def _get(path: str, params: dict = None, timeout: int = 15, retries: int = 2):
    url = BASE.rstrip("/") + "/" + path.lstrip("/")
    if params:
        url += "?" + urllib.parse.urlencode(params)
    last = None
    for i in range(retries):
        for op in (_opener, _opener_direct):     # 先走代理，失败回退直连
            try:
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with op.open(req, timeout=timeout) as r:
                    return r.read().decode("utf-8")
            except Exception as e:  # noqa: BLE001
                last = e
        time.sleep(1.0 * (i + 1))
    raise RuntimeError(f"行情请求失败 {url}: {last}")


def closed_bars(contract: str, tf_min: int, days: int = 7, n: int = 45) -> list:
    """已收盘 K 线，升序。返回 [{"t": 毫秒, "o","h","l","c","v"}]"""
    key = (contract, tf_min)
    now = time.time()
    hit = _cache.get(key)
    if hit and hit[0] > now and len(hit[1]) >= min(n, 20):
        return hit[1][-n:]
    interval = _TF_MAP[tf_min]
    raw = json.loads(_get("candlesticks",
                          {"contract": contract, "interval": interval, "limit": n + 1}))
    bars = []
    for r in raw[:-1]:                    # 最后一根未收盘，丢弃
        bars.append({"t": int(r["t"]) * 1000, "v": float(r["v"]),
                     "c": float(r["c"]), "h": float(r["h"]),
                     "l": float(r["l"]), "o": float(r["o"])})
    if bars:
        _cache[key] = (now + CACHE_TTL, bars)
    return bars[-n:]


def live_price(contract: str) -> float:
    try:
        raw = json.loads(_get("tickers", {"contract": contract}))
        return float(raw[0]["last"]) if raw else 0.0
    except Exception:
        return 0.0


def funding_rate(contract: str) -> float:
    try:
        d = _get(f"contracts/{contract}")
        return float(d.get("funding_rate") or 0.0)
    except Exception:
        return 0.0
