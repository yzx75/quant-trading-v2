# -*- coding: utf-8 -*-
"""日志：stdout + 按天轮转文件。stdout 必须丢弃时由启动命令重定向。"""
import os
import threading
import time
from datetime import datetime

_lock = threading.Lock()
_log_dir = ""
_echo = True


def init(log_dir: str, echo: bool = True):
    global _log_dir, _echo
    _log_dir = log_dir
    _echo = echo
    os.makedirs(log_dir, exist_ok=True)


def _file() -> str:
    return os.path.join(_log_dir, time.strftime("engine-%Y%m%d.log"))


def log(msg: str):
    line = f"[{datetime.now().strftime('%m-%d %H:%M:%S')}] {msg}"
    with _lock:
        if _echo:
            try:
                print(line, flush=True)
            except Exception:
                pass
        if _log_dir:
            try:
                with open(_file(), "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception:
                pass


def err(msg: str):
    log("⚠ " + msg)


# 便捷导入别名
info = log
error = err


def tail(path: str, n: int = 100) -> list:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.readlines()[-n:]
