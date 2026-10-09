# -*- coding: utf-8 -*-
"""v2 引擎看门狗 —— 常驻监护：引擎进程死亡 / 卡死 / 流水停更，自动拉起。

背景：2026-10-02 06:18 引擎启动后静默退出（后台 shell 嵌套 & 把子进程带走），
持仓 2 个却无人守护，靠人工 psutil 复核才发现。v2 一直没有 watchdog 托管，
进程掉了只能手动拉。

判活三层（任一不过即重启）：
  1. 进程存活：psutil 按 cmdline[-1]=="server.py" 精确匹配（PowerShell/tasklist 不可靠）
  2. HTTP 存活：GET /api/state 5 秒超时（防进程在但事件循环卡死）
  3. 流水新鲜：runtime/live.json mtime（watcher 约 8 秒一轮，超过 STALE 秒 = 卡死）

防误杀：
  GRACE      刚拉起的宽限期（引擎要 reconcile + 起 uvicorn，几十秒才就绪）
  RESTART_GAP 两次重启最小间隔，防抖
接管模式：watchdog 启动时若已有引擎在跑，只监视不重复拉起；该实例死后才 spawn。

用法（唯一正确姿势，不要在外层再套 &，否则本进程也会被 shell 带走）：
    NO_PROXY=* <venv python> watchdog.py > /dev/null 2>&1

⚠ 10-06 实测：**AI 沙箱内无法让进程真正常驻** —— DETACHED_PROCESS 与
  CREATE_BREAKAWAY_FROM_JOB 创建的进程，命令一结束仍被按 Job 清理
  （跨命令存活 0/4）。要长期常驻，请在**自己的终端**里手动启动本脚本。
  可选开关 `_WD_DETACH=1`：在自己终端启动时用它脱离该终端（详见下方注释）。
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

TARGET = os.path.join(BASE, "server.py")
WLOG = os.path.join(BASE, "logs", "watchdog.log")
LIVE = os.path.join(BASE, "runtime", "live.json")
PORT = 8801

CHECK = 20          # 体检间隔（秒）
GRACE = 60          # 刚拉起的宽限期（秒），期间只等不判
STALE = 120         # live.json 超过此秒数未更新 = 判定卡死
RESTART_GAP = 30    # 两次重启最小间隔（秒），防抖
HTTP_TIMEOUT = 5

# 必须用 default venv 的 python：裸3.13 缺 yaml，引擎起不来。
# 候选路径按序探测，第一个存在的即用；都没有则退回当前解释器
#（10-05开源前修：原为硬编码本机绝对路径，别人 clone 下来 watchdog 直接失效）
_VENV_CANDIDATES = (
    os.path.join(os.path.dirname(os.path.abspath(__file__)), ".venv",
                 "Scripts", "python.exe"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "venv",
                 "Scripts", "python.exe"),
    sys.prefix and os.path.join(sys.prefix, "Scripts", "python.exe"),
)
PY = next((p for p in _VENV_CANDIDATES if p and os.path.exists(p)),
          sys.executable)

# ---- 可选：自我脱离会话 shell（10-06，**默认关闭**）--------------------------
# 背景：`run_in_background` 起的后台 shell **自己也会被沙箱回收**，连带带走
#   watchdog 与引擎（10-06 11:04 实证：10:59 起的 server 在 11:04:5x 被连带杀掉）。
#   ⚠ 曾试图靠"启动时 DETACHED_PROCESS 自我重启"加固 —— **在 AI 沙箱里实测无效**：
#     用 DETACHED_PROCESS 与 CREATE_BREAKAWAY_FROM_JOB 各起哨兵，命令一结束全部被清理
#     （跨命令存活 **0/4**）。沙箱按 Job/进程树回收，**AI 会话内做不到真正常驻**。
#     ⇒ 要长期常驻，请在**自己的终端**里手动启动本脚本。
# 保留本开关（默认关）的理由：
#   ① 在用户自己的终端里 `_WD_DETACH=1` 启动，可脱离该终端（关窗口不影响）；
#   ② 若哪天沙箱放开 Job 限制，本机制可直接复用。
# 开启：`_WD_DETACH=1 NO_PROXY=* <venv python> watchdog.py`
#   ⚠ 必须置于 BASE / PY 定义之后（需要这两者）。
#   ⚠ 用 `_WD_DETACHED` 标记防无限自我重启。
#   ⚠ 环境变量在这里一并设好：NO_PROXY=* 是铁律（系统代理 127.0.0.1:60747 随机 502）。
# 代价：detach 后 stdout/stderr 丢弃；日志照常写 logs/watchdog.log，不受影响。
if (sys.platform == "win32"
        and os.environ.get("_WD_DETACH") == "1"
        and not os.environ.get("_WD_DETACHED")):
    _env = dict(os.environ)
    _env["_WD_DETACHED"] = "1"
    _env["NO_PROXY"] = "*"
    _env["no_proxy"] = "*"
    try:
        subprocess.Popen(
            [PY, os.path.abspath(__file__)], cwd=BASE, env=_env,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=0x00000008 | 0x00000200,   # DETACHED_PROCESS|NEW_GROUP
            close_fds=True)
        print("watchdog：已自我脱离会话 shell（DETACHED_PROCESS），本进程退出")
        sys.exit(0)
    except Exception as _e:
        print(f"watchdog：自我脱离失败（{_e}），退回前台常驻模式")


def wlog(s: str):
    line = f"[{datetime.now().strftime('%m-%d %H:%M:%S')}] {s}\n"
    try:
        os.makedirs(os.path.dirname(WLOG), exist_ok=True)
        with open(WLOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    try:
        print(line, end="", flush=True)
    except Exception:
        pass


def _engine_pids() -> list:
    """psutil 按 args[-1] 精确匹配 server.py（父子两个进程都算）。"""
    try:
        import psutil
    except Exception:
        return []
    out = []
    for p in psutil.process_iter(["pid", "cmdline"]):
        try:
            cl = p.info["cmdline"] or []
            if cl and cl[-1].endswith("server.py"):
                out.append(p.info["pid"])
        except Exception:
            pass
    return out


def http_ok() -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/state", timeout=HTTP_TIMEOUT)
        return True
    except Exception:
        return False


def live_age() -> float:
    try:
        return time.time() - os.path.getmtime(LIVE)
    except Exception:
        return 9999.0


def spawn():
    """拉起引擎。NO_PROXY=* 是铁律：系统代理 127.0.0.1:60747 会随机 502 隧道失败。"""
    env = os.environ.copy()
    env["NO_PROXY"] = "*"
    env["no_proxy"] = "*"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        env.pop(k, None)
    wlog(f"🚀 拉起引擎 {TARGET}（py={PY}）")
    return subprocess.Popen([PY, TARGET], cwd=BASE, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def kill_all():
    for pid in _engine_pids():
        try:
            import psutil
            psutil.Process(pid).kill()
        except Exception:
            pass


def main():
    os.makedirs(os.path.dirname(WLOG), exist_ok=True)
    proc = None
    last_restart = 0.0

    existing = _engine_pids()
    if existing:
        wlog(f"👀 检测到已在运行的引擎 PID {existing} → 接管模式（只监视，不重复拉起）")
    else:
        proc = spawn()
        last_restart = time.time()
        time.sleep(GRACE)

    wlog(f"✅ 看门狗就位：每 {CHECK}s 体检 | 宽限 {GRACE}s | 流水超时 {STALE}s")
    while True:
        try:
            time.sleep(CHECK)
            pids = _engine_pids()

            if not pids:
                wlog("💀 引擎进程不存在 → 准备重启")
            elif not http_ok():
                wlog(f"💀 引擎进程在（{pids}）但 HTTP :{PORT} 无响应 → 判定卡死，准备重启")
            elif live_age() > STALE:
                wlog(f"💀 引擎在但 live.json 已 {live_age():.0f}s 未更新（>{STALE}s）→ 判定卡死，准备重启")
            else:
                continue

            if time.time() - last_restart < RESTART_GAP:
                wlog(f"  ⏳ 距上次重启不足 {RESTART_GAP}s，跳过本轮（防抖）")
                continue

            kill_all()
            time.sleep(2)
            proc = spawn()
            last_restart = time.time()
            wlog("  ⏳ 新引擎宽限期，暂不判活")
            time.sleep(GRACE)
            if _engine_pids():
                wlog(f"  ✅ 重启成功，引擎 PID {_engine_pids()}")
            else:
                wlog("  ❌ 重启后仍未检出引擎进程，下一轮继续尝试")
        except KeyboardInterrupt:
            wlog("🛑 看门狗退出（收到中断）")
            return
        except Exception as e:
            wlog(f"⚠ 体检异常: {type(e).__name__}: {str(e)[:160]}")
            time.sleep(CHECK)


if __name__ == "__main__":
    main()
