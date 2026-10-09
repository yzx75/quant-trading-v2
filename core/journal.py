# -*- coding: utf-8 -*-
"""core/journal.py — 完整交易流水（10-03 01:20 用户要求「完整每一单的日志，
方便后期整理看看什么问题」）。

与 trades.json 的分工：
  · trades.json  —— 复盘/统计用的**精简**开平仓记录（保持原字段，向后兼容）
  · journal-YYYYMMDD.jsonl —— **全量**结构化流水，一行一事件，字段尽量齐全，
    供后期 pandas/grep 批量分析。**append-only，不覆盖**（事故现场不会被下一次
    写盘抹掉），文件按日切分。

事件类型（`ev` 字段）：
  open      开仓成交（含模型理由/候选来源/风控数字/费率）
  close     平仓（含毛利/手续费/净利/持仓时长/MFE/MAE/平仓来源）
  reject    护栏拒绝（拒开原因）
  guard     guard 动作（保证金守护/裸仓巡检/孤儿兜底）
            ⚠ 10-09 起 A/B/C/E 四段机械护盈、分批止盈、时间止损已整体删除，
              原来由它们产出的 `arm` / `trail` 两类事件随之消失；平仓来源
              只剩交易所条件单、D 段补单与模型主动平仓三种。
  anomaly   异常（误判成交/记录丢失/竞态/孤儿单等）—— 事后复盘最有用
  decision    模型每轮决策（含 regime/bias/候选数/是否开仓）
  exit_brief  每轮持仓管理输入特征、五条件复核值与模型决定
  ghost_pnl   不干预至固定 SL/TP 的只读影子结果（含 pending/final/ambiguous）
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime

_DIR = None
_FH: dict = {}          # 日期 -> 文件句柄（常驻，避免每条重开）


def bind(runtime_dir: str):
    """由 engine 启动时调用，绑定 runtime 目录。"""
    global _DIR
    _DIR = runtime_dir


def _path() -> str:
    d = datetime.now().strftime("%Y%m%d")
    return os.path.join(_DIR or ".", f"journal-{d}.jsonl")


def _fh():
    d = datetime.now().strftime("%Y%m%d")
    fh = _FH.get(d)
    if fh is None:
        os.makedirs(_DIR or ".", exist_ok=True)
        fh = open(_path(), "a", encoding="utf-8")
        _FH[d] = fh
    return fh


def log(ev: str, **fields):
    """写一条流水。**永不抛异常**（日志不能影响交易）。"""
    try:
        rec = {"t": round(time.time(), 3),
               "ts": datetime.now().strftime("%H:%M:%S"),
               "ev": ev}
        for k, v in fields.items():
            try:
                json.dumps(v)          # 可序列化才写
            except Exception:
                v = str(v)
            rec[k] = v
        _fh().write(json.dumps(rec, ensure_ascii=False) + "\n")
        _fh().flush()
    except Exception:
        pass


# ---------------------------------------------------------------- 开仓
def journal_open(**f):
    """开仓流水。约定字段：sym/side/entry/px/size/notional/lev/sl/tp/risk_u/
    rr/fee_rt/win_rate/regime/bias/cand/req_*/sl_*/tp_*/maker/taker/
    reason/open_ts/dual_side"""
    log("open", **f)


# ---------------------------------------------------------------- 平仓
def journal_close(**f):
    """平仓流水。约定字段：sym/side/entry/px/size/gross/fee/net/hold_bars/
    hold_min/mfe/mae/peak_pnl/closed_by/text/close_cause/pnl_pct/
    open_ts/close_ts"""
    log("close", **f)


# ---------------------------------------------------------------- 决策
def journal_decision(**f):
    log("decision", **f)
