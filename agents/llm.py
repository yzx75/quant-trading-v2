# -*- coding: utf-8 -*-
"""
agents/llm.py — 决策者模型调用（OpenAI 兼容 /chat/completions）
读 .env：CLOUD_BASE_URL / CLOUD_API_KEY / CLOUD_MODEL（可配 CLOUD2_* 双端点轮询）。
连续失败指数退避冷却（90s × 次数，封顶 300s）。决策关思考模式（省时省钱），
反思/复盘可传 thinking=True 且走 REFLECT_MODEL 专用通道。
"""
from __future__ import annotations

import json
import time
import urllib.request

from core import log as core_log
from core.config import load_env

LOG = core_log.log
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

_env = load_env()


def _providers(reflect: bool = False) -> list:
    out = []
    for tag, prefix in (("云端①", "CLOUD"), ("云端②", "CLOUD2")):
        url = (_env.get(f"{prefix}_BASE_URL") or "").rstrip("/")
        key = _env.get(f"{prefix}_API_KEY") or ""
        model = _env.get(f"{prefix}_MODEL") or ""
        if url and key and model:
            out.append((url, key, model, tag))
    if reflect:
        rm = _env.get("REFLECT_MODEL") or ""
        if rm and out:
            u, k, _, _ = out[0]
            return [(u, k, rm, "反思通道")]
    return out


_cool: dict = {}
_fail: dict = {}


def ask(messages: list, temp: float = 0.2, max_tokens: int = 512,
        timeout: int = 90, thinking: bool = False, reflect: bool = False,
        provider=None) -> str | None:
    """返回 content 字符串；全部端点失败返回 None"""
    provs = [provider] if provider else _providers(reflect)
    if not provs:
        LOG("  ⚠ 无可用模型端点（检查 .env 的 CLOUD_*）")
        return None
    msgs = []
    for m in messages:
        c = m.get("content")
        if isinstance(c, list):
            c = " ".join(x.get("text", "") for x in c if isinstance(x, dict))
        msgs.append({"role": m.get("role", "user"), "content": c})
    msgs.append({"role": "system",
                 "content": "只输出一个 JSON 对象，不要解释、不要代码块、不要 markdown 围栏。"})
    now = time.time()
    for url, key, model, tag in provs:
        if now < _cool.get(url, 0):
            continue
        # glm-5.3 等强制思考模式的模型：反思通道预算提到 4096，防思维链吃光 token 正文截断
        body = {"model": model, "messages": msgs, "temperature": temp,
                "max_tokens": max(4096 if reflect else 512, max_tokens)}
        err = ""
        for attempt in range(2):        # 首选 json 模式+关思考，网关不支持则降级重试
            if attempt == 0:
                body["response_format"] = {"type": "json_object"}
                if not thinking:
                    body["enable_thinking"] = False
            else:
                body.pop("response_format", None)
                body.pop("enable_thinking", None)
            try:
                req = urllib.request.Request(
                    url + "/chat/completions", data=json.dumps(body).encode(),
                    headers={"Content-Type": "application/json",
                             "Authorization": "Bearer " + key})
                with _opener.open(req, timeout=timeout) as r:
                    raw = json.loads(r.read().decode())
                txt = ((raw.get("choices") or [{}])[0].get("message") or {}).get("content", "")
                if txt:
                    _fail[url] = 0
                    _cool.pop(url, None)
                    return txt.strip()
            except Exception as e:  # noqa: BLE001
                err = f"{type(e).__name__}: {str(e)[:110]}"
        _fail[url] = _fail.get(url, 0) + 1
        _cool[url] = time.time() + min(300, 90 * _fail[url])
        LOG(f"  ⚠ {tag}({model}) 不可用 {err} → 冷却 {int(_cool[url]-time.time())}s")
    return None


def parse_json(txt: str | None) -> dict | None:
    """宽容解析模型 JSON：剥围栏、截花括号"""
    if not txt:
        return None
    t = txt.strip()
    t = t.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    i, j = t.find("{"), t.rfind("}")
    if i < 0 or j <= i:
        return None
    try:
        return json.loads(t[i:j + 1])
    except Exception:
        return None
