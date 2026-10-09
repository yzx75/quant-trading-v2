#!/usr/bin/env python3
"""launch.py — 量化交易引擎 v2 一键上线脚本

用法（Windows 双击 START.bat 等价）:
    python launch.py                 # 安装依赖 + 启动引擎 + 健康检查
    python launch.py --dry-run       # 干跑模式：一切照常但不下真单（强烈建议先跑一次）
    python launch.py --port 9000     # 指定端口（默认 8801）
    python launch.py --stop          # 停止已启动的引擎
    python launch.py --no-install    # 跳过依赖安装（已装好时提速）

上线三步:
    1) 把 .env 放进本目录（GATE_KEY/GATE_SECRET/CLOUD_* 等，参考 .env.example）
    2) 双击 START.bat （或命令行 python launch.py --dry-run 先干跑验证）
    3) 浏览器打开 http://localhost:8801 看板
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PID_FILE = os.path.join(HERE, "runtime", "engine.pid")

REQUIRED_ENV_KEYS = ["GATE_KEY", "GATE_SECRET"]


def step(msg: str) -> None:
    print(f"\n==> {msg}")


def die(msg: str, hint: str = "") -> None:
    print(f"\n[FAIL] {msg}")
    if hint:
        print(f"       {hint}")
    sys.exit(1)


def check_python() -> None:
    step("检查 Python 版本（需 >= 3.10）")
    if sys.version_info < (3, 10):
        die(f"Python {sys.version_info.major}.{sys.version_info.minor} 过旧",
            "请安装 Python 3.10+ 后重试")
    print(f"    Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} OK")


def check_env() -> None:
    step("检查 .env 密钥文件")
    env_path = os.path.join(HERE, ".env")
    if not os.path.exists(env_path):
        die("找不到 .env",
            f"请在 {env_path} 放入密钥（参考 .env.example），至少需要: "
            + ", ".join(REQUIRED_ENV_KEYS))
    have = {}
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                have[k.strip()] = v.strip()
    missing = [k for k in REQUIRED_ENV_KEYS if not have.get(k)]
    if missing:
        die(f".env 缺少必填项: {', '.join(missing)}", "补上后重新运行")
    print(f"    .env OK（{len(have)} 项，必填项齐全）")


def install_deps(skip: bool) -> None:
    if skip:
        step("跳过依赖安装（--no-install）")
        return
    # 已装齐就跳过，免得每次上线都跑 pip
    try:
        import fastapi, uvicorn, yaml  # noqa: F401
        step("依赖已安装（fastapi/uvicorn/pyyaml），跳过 pip")
        return
    except ImportError:
        pass
    step("安装依赖 pip install -r requirements.txt")
    r = subprocess.run([sys.executable, "-m", "pip", "install", "-r",
                        os.path.join(HERE, "requirements.txt")],
                       cwd=HERE)
    if r.returncode != 0:
        die("依赖安装失败", "检查网络/代理后重试，或手动执行上面的 pip 命令")


def is_alive(pid: int) -> bool:
    if os.name == "nt":
        r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                           capture_output=True, text=True)
        return str(pid) in (r.stdout or "")
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def stop_engine() -> None:
    step("停止引擎")
    if not os.path.exists(PID_FILE):
        die(f"没有运行记录（{PID_FILE} 不存在）", "引擎可能未由本脚本启动")
    pid = int(open(PID_FILE).read().strip() or 0)
    if pid and is_alive(pid):
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/F", "/T"],
                           capture_output=True)
        else:
            os.kill(pid, 15)
        time.sleep(1.5)
        print(f"    已停止 PID {pid}")
    else:
        print(f"    PID {pid} 已不在运行，清理记录")
    os.remove(PID_FILE)


def health_ok(port: int, timeout: float = 3.0) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health",
                                    timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def start_engine(args) -> None:
    if os.path.exists(PID_FILE):
        old = int(open(PID_FILE).read().strip() or 0)
        if old and is_alive(old):
            die(f"引擎已在运行（PID {old}）",
                f"先执行 python launch.py --stop，或直接打开 http://localhost:{args.port}")
        os.remove(PID_FILE)

    step(f"启动引擎（{'DRY_RUN 干跑' if args.dry_run else '实盘'}，端口 {args.port}）")
    if not args.dry_run:
        print("    ⚠ 实盘模式：将使用真实资金下单。按 Ctrl+C 取消，5 秒后继续...")
        time.sleep(5)

    env = os.environ.copy()
    env["NO_PROXY"] = "*"
    if args.dry_run:
        env["DRY_RUN"] = "1"
    os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)   # zip 包不含 logs/，须自建
    os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)
    logf = open(os.path.join(HERE, "logs", "_launch_stdout.txt"), "ab")
    # Windows 下彻底脱离父进程/控制台/作业对象：窗口关闭、启动器退出都不连坐引擎
    flags = 0
    if os.name == "nt":
        flags = (subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
                 | subprocess.CREATE_BREAKAWAY_FROM_JOB)
    try:
        proc = subprocess.Popen(
            [sys.executable, "server.py", "--host", args.host, "--port", str(args.port)],
            cwd=HERE, env=env, stdout=logf, stderr=subprocess.STDOUT,
            creationflags=flags)
    except OSError:
        # 个别受限环境禁止 breakaway（如沙箱作业对象），退回普通分离启动
        proc = subprocess.Popen(
            [sys.executable, "server.py", "--host", args.host, "--port", str(args.port)],
            cwd=HERE, env=env, stdout=logf, stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS)
    open(PID_FILE, "w").write(str(proc.pid))
    print(f"    进程已拉起 PID {proc.pid}（日志: logs/_launch_stdout.txt + logs/engine-*.log）")

    step("健康检查（等待引擎就绪，最多 90 秒）")
    t0 = time.time()
    while time.time() - t0 < 90:
        if proc.poll() is not None:
            die(f"引擎进程退出（code={proc.returncode}）",
                "看 logs/_launch_stdout.txt 排错（常见: 端口被占/依赖缺失）")
        if health_ok(args.port):
            break
        time.sleep(2)
    else:
        die("90 秒内健康检查未通过", f"引擎可能仍在预热，手动访问 http://127.0.0.1:{args.port}/health 确认")

    print("    /health OK ✓")
    print("\n================ 上线完成 ================")
    print(f"  看板:   http://localhost:{args.port}")
    print(f"  健康:   http://localhost:{args.port}/health")
    print(f"  停止:   python launch.py --stop")
    if args.dry_run:
        print("  （当前为干跑模式，不下真单；实盘请去掉 --dry-run）")
    print("==========================================")
    print("引擎在后台运行，本窗口可以关闭。")


def main() -> None:
    ap = argparse.ArgumentParser(description="量化交易引擎 v2 一键上线")
    ap.add_argument("--port", type=int, default=8801)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--dry-run", action="store_true", help="干跑：不下真单")
    ap.add_argument("--no-install", action="store_true", help="跳过依赖安装")
    ap.add_argument("--stop", action="store_true", help="停止引擎")
    args = ap.parse_args()

    if args.stop:
        stop_engine()
        return
    os.chdir(HERE)
    check_python()
    check_env()
    install_deps(args.no_install)
    start_engine(args)


if __name__ == "__main__":
    main()
