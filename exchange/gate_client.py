# -*- coding: utf-8 -*-
"""
gate_client.py — Gate.io API v4 私有接口客户端（签名访问）

用途：在 Gate 网页不可达的网络环境下，用 API Key 完成账户查询与下单。
安全约定：
  - 凭据只从本地 .env 或环境变量读取，绝不写入日志、绝不外发。
  - .env 权限需用户自行保管；本脚本不开启任何提现相关接口。

签名（Gate v4，与官方 Python SDK 一致）：
    body_hash = sha512(body).hexdigest()
    sign_str  = f"{METHOD}\n{url_path}\n{query_string}\n{body_hash}\n{timestamp}"
    SIGN      = hmac_sha512(SECRET, sign_str).hexdigest()
  服务端若只认简版（hmac(SECRET, body_hash)），会自动回退重试。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import urllib.error

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

BASE = "https://api.gateio.ws"
API_PREFIX = "/api/v4"
ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")  # v2 根目录


# ---------------------------------------------------------------- 凭据
def _load_env_file(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def credentials():
    _load_env_file(ENV_FILE)
    key = os.environ.get("GATE_KEY", "").strip()
    secret = os.environ.get("GATE_SECRET", "").strip()
    if not key or not secret:
        raise SystemExit(
            "缺少凭据。请在 %s 中写入：\n"
            "GATE_KEY=你的KEY\nGATE_SECRET=你的SECRET\n" % ENV_FILE
        )
    return key, secret


_direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
# 09-30 17:33 事故：系统代理(127.0.0.1:60747)隧道随机 502，GET/POST 全灭 → 引擎停机 78 分钟。
# 默认强制直连（直连已实测稳定）；确需走代理的环境设 GATE_FORCE_PROXY=1 恢复旧行为。
if os.environ.get("GATE_FORCE_PROXY") != "1":
    os.environ["no_proxy"] = "*"
    os.environ["NO_PROXY"] = "*"
_via_proxy = urllib.request.build_opener()      # no_proxy=* 时实际也是直连（默认）
# 本机网络实况（2026-09-28 00:57 faulthandler 定位）：代理健康时两条路都通，
# 但代理会【随机挂起】（HTTPS _tunnel 卡死不返回）——单路必挂。双路互备：
# GET 先直连 12s，不通换系统代理 20s；POST 不自动换路重试（防重复下单）。


def _open_fallback(req, timeout, allow_fallback):
    try:
        return _direct.open(req, timeout=min(timeout, 12) if allow_fallback else timeout)
    except Exception:
        if not allow_fallback:
            raise
    return _via_proxy.open(req, timeout=timeout)


# ---------------------------------------------------------------- 请求
def request(method: str, path: str, query: dict = None, body: dict = None,
            timeout: int = 20, authed: bool = True):
    """path 形如 /futures/usdt/accounts（无需带 /api/v4）"""
    key, secret = credentials() if authed else ("", "")
    url_path = API_PREFIX + path
    qs = urllib.parse.urlencode(query) if query else ""
    body_str = json.dumps(body, separators=(",", ":")) if body is not None else ""

    url = BASE + url_path + (("?" + qs) if qs else "")

    headers = {"Accept": "application/json", "Content-Type": "application/json",
               "User-Agent": "workbot-gate-client/1.0"}
    if authed:
        ts = str(int(time.time()))
        headers["KEY"] = key
        headers["Timestamp"] = ts
        headers["SIGN"] = _sign(secret, method.upper(), url_path, qs, body_str, ts)

    data = body_str.encode("utf-8") if body_str else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        # GET（无副作用）允许直连失败后换代理重试；POST/DELETE 不重试防重复下单
        with _open_fallback(req, timeout, allow_fallback=(method.upper() == "GET" and body is None)) as r:
            raw = r.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "ignore")
        raise RuntimeError(f"HTTP {e.code} {method} {url_path}: {detail[:400]}")
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"请求失败 {method} {url_path}: {e}")


def _sign(secret: str, method: str, url_path: str, qs: str, body_str: str, ts: str) -> str:
    body_hash = hashlib.sha512(body_str.encode("utf-8")).hexdigest()
    payload = f"{method}\n{url_path}\n{qs}\n{body_hash}\n{ts}"
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()


def _sign_simple(secret: str, body_str: str) -> str:
    body_hash = hashlib.sha512(body_str.encode("utf-8")).hexdigest()
    return hmac.new(secret.encode("utf-8"), body_hash.encode("utf-8"), hashlib.sha512).hexdigest()


def request_authed(method: str, path: str, query: dict = None, body: dict = None):
    """先试完整签名，401 时回退简版签名"""
    try:
        return request(method, path, query=query, body=body, authed=True)
    except RuntimeError as e:
        if "HTTP 401" not in str(e):
            raise
        key, secret = credentials()
        qs = urllib.parse.urlencode(query) if query else ""
        body_str = json.dumps(body, separators=(",", ":")) if body is not None else ""
        url = BASE + API_PREFIX + path + (("?" + qs) if qs else "")
        headers = {"Accept": "application/json", "Content-Type": "application/json",
                   "KEY": key, "Timestamp": str(int(time.time())),
                   "SIGN": _sign_simple(secret, body_str)}
        req = urllib.request.Request(
            url, data=body_str.encode("utf-8") if body_str else None,
            headers=headers, method=method.upper())
        with _via_proxy.open(req, timeout=20) as r:
            raw = r.read().decode("utf-8")
            return json.loads(raw) if raw else {}


# ---------------------------------------------------------------- 常用接口
def futures_accounts():
    return request_authed("GET", "/futures/usdt/accounts")


def futures_positions():
    return request_authed("GET", "/futures/usdt/positions")


# ---------------------------------------------------------------- 账户实际费率
# 10-02 16:0x 实测修正：contracts 公开接口的挂牌费率（taker 0.075% / maker -0.01%）
# ≠ 账户实际结算费率。用最近 200 笔成交按 role 实算：taker 0.05% / maker 0.02%
# （正费、不返佣）。fee_rt 虚高会让 E 段动态止盈门槛虚高（晚熟 0.05 个百分点级别）。
_fee_cache = {"t": 0.0, "v": None}


def real_fee_rates(max_age_s: int = 3600):
    """账户实际费率 {'taker': rate, 'maker': rate}（按最近成交实算，缓存 1h）。
    无样本/接口失败返回 None，调用方回退挂牌费率。"""
    if _fee_cache["v"] is not None and time.time() - _fee_cache["t"] < max_age_s:
        return _fee_cache["v"]
    try:
        trades = request_authed("GET", "/futures/usdt/my_trades",
                                query={"limit": 200}) or []
        agg = {"taker": [0.0, 0.0], "maker": [0.0, 0.0]}   # [fee, notional]
        qm_cache: dict = {}
        for t in trades:
            role = t.get("role")
            if role not in agg:
                continue
            c = t.get("contract") or ""
            if c not in qm_cache:
                try:
                    j = request("GET", f"/futures/usdt/contracts/{c}", authed=False)
                    qm_cache[c] = float((j or {}).get("quanto_multiplier") or 0)
                except Exception:
                    qm_cache[c] = 0.0
            notional = abs(int(t.get("size") or 0)) * qm_cache[c] * float(t.get("price") or 0)
            if notional <= 0:
                continue
            agg[role][0] += abs(float(t.get("fee") or 0))
            agg[role][1] += notional
        out = {role: f / n for role, (f, n) in agg.items() if n > 0}
        if out.get("taker"):            # 至少有 taker 样本才算成功
            _fee_cache["t"] = time.time()
            _fee_cache["v"] = out
            return out
    except Exception:
        pass
    return None


def spot_accounts():
    return request_authed("GET", "/spot/accounts")


def ticker(contract: str):
    return request("GET", f"/futures/usdt/tickers", query={"contract": contract}, authed=False)


def ping():
    return request("GET", "/spot/time", authed=False)


# ---------------------------------------------------------------- CLI
def main():
    args = sys.argv[1:]
    cmd = args[0] if args else "test"

    if cmd == "test":
        t = ping()
        print(f"[公共接口] 服务器时间: {t}")
        try:
            acct = futures_accounts()
            print(f"[私有接口] 合约账户: {json.dumps(acct, ensure_ascii=False)[:600]}")
        except Exception as e:  # noqa: BLE001
            print(f"[私有接口] 失败: {e}")
        try:
            sp = spot_accounts()
            print(f"[私有接口] 现货账户: {json.dumps(sp, ensure_ascii=False)[:600]}")
        except Exception as e:  # noqa: BLE001
            print(f"[私有接口] 失败: {e}")
    elif cmd == "positions":
        print(json.dumps(futures_positions(), ensure_ascii=False, indent=2)[:3000])
    elif cmd == "balance":
        print(json.dumps({"futures": futures_accounts(), "spot": spot_accounts()},
                         ensure_ascii=False, indent=2)[:3000])
    else:
        print("用法: python gate_client.py [test|positions|balance]")


if __name__ == "__main__":
    main()
