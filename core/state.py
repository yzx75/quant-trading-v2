# -*- coding: utf-8 -*-
"""状态持久化：原子写 JSON（断电/崩溃不留半个文件）"""
import json
import os
import threading
import time

_lock = threading.Lock()


def load(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {} if default is None else default


def save(path, data):
    with _lock:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        # Windows 上目标文件可能被杀软/扫描器瞬时锁住，os.replace 报 PermissionError——重试覆盖
        for i in range(5):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                time.sleep(0.2 * (i + 1))
        os.replace(tmp, path)   # 重试耗尽仍失败则抛出，由调用方 _loop 记异常（线程不死）


def append_line(path, line: str):
    """决策流水 / 教训文件：追加一行"""
    with _lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")
