# -*- coding: utf-8 -*-
"""
配置加载：configs/config.yaml（参数） + .env（密钥）。
密钥只进服务端内存，绝不写进看板 HTML / API 响应。
"""
import os

import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_env(path=None):
    """轻量 .env 读取（KEY=VALUE，# 注释），进 os.environ 并返回 dict"""
    path = path or os.path.join(BASE_DIR, ".env")
    cfg = {}
    if os.path.exists(path):
        for ln in open(path, encoding="utf-8"):
            ln = ln.strip()
            if ln and not ln.startswith("#") and "=" in ln:
                k, v = ln.split("=", 1)
                cfg[k.strip()] = v.strip()
                os.environ.setdefault(k.strip(), v.strip())
    return cfg


def load_config(path=None) -> dict:
    path = path or os.path.join(BASE_DIR, "configs", "config.yaml")
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # 运行时目录相对项目根
    for k in ("runtime_dir", "log_dir"):
        cfg[k] = os.path.join(BASE_DIR, cfg.get(k, "runtime" if k == "runtime_dir" else "logs"))
    os.makedirs(cfg["runtime_dir"], exist_ok=True)
    os.makedirs(cfg["log_dir"], exist_ok=True)
    return cfg


def p(*parts) -> str:
    """项目内路径拼接"""
    return os.path.join(BASE_DIR, *parts)
