@echo off
setlocal EnableExtensions

title v2 量化引擎 - 一键启动

REM ===================================================================
REM  v2 引擎一键启动器  （双击即可）
REM
REM  用法：
REM    双击                    -^> 前台运行 server.py（实时输出可见，推荐）
REM    _start_engine.cmd wd    -^> 守护模式（watchdog.py，引擎崩了自动重拉）
REM                               要常用就右键建快捷方式，在"目标"末尾加  wd
REM
REM  [警告] 为什么必须由你在本地终端启动（不能靠 AI 会话）：
REM    实测 4 种拉起方式（Start-Process / bash & / bat / New-Object Process）
REM    全部在 AI 会话结束时被回收（最长只活 6 分钟）。日志特征：
REM    engine-*.log 里【没有 Traceback】、scan.json 停止刷新。
REM
REM  [成功判据] 窗口里出现这两行：
REM    「引擎已启动（实盘）品种 13 个，高频 ≤9 仓，服务 :8801」
REM    「系统提示词已渲染：白名单 13 个品种…」
REM
REM  [停止] 按 Ctrl+C，或直接关掉本窗口。
REM    [注意] 别用 python launch.py --stop：server.py 不写 runtime\engine.pid，
REM           那个文件里是旧进程的死 PID，launch.py 会认错对象。
REM
REM  [编码] 本文件必须以 ANSI(GBK) 保存，且不要加 chcp 65001。
REM    否则 cmd 按 GBK 解析 UTF-8 字节会错位，把注释后半截当命令执行。
REM ===================================================================

REM ---------- 配置（换机器只改这两行） ------------------------------
set "BASE=D:\AI\.workbuddy\加密之旅\v2"
set "PY=C:\Users\54042\.workbuddy\binaries\python\envs\default\Scripts\python.exe"
set "PORT=8801"
REM -----------------------------------------------------------------

echo ============================================================
echo    v2 量化引擎 - 一键启动
echo    %DATE% %TIME%
echo ============================================================
echo.

REM ---------- 1/3 解释器自检 ----------
if not exist "%PY%" (
    echo [失败] 找不到 python 解释器：
    echo        %PY%
    echo        请修正本脚本顶部 "配置" 段的 PY 变量。
    echo.
    pause
    exit /b 1
)
echo [1/3] 解释器 OK

REM ---------- 2/3 端口占用自检 ----------
netstat -ano -p TCP | findstr ":%PORT% " | findstr /i "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo [警告] 端口 %PORT% 已在监听 -- 引擎可能已经在跑了。
    echo        看板: http://localhost:%PORT%
    echo        再起一个只会端口冲突，先确认：
    echo        - 想重启 = 关掉原来那个窗口后再跑本脚本
    echo        - 想强杀 = taskkill /F /IM python.exe  ^(会杀掉所有 python^)
    echo.
    choice /C YN /N /M "    仍然继续启动？(Y=继续 N=退出) "
    if errorlevel 2 (
        echo.
        echo 已取消。
        pause
        exit /b 0
    )
    echo.
) else (
    echo [2/3] 端口 %PORT% 空闲
)

REM ---------- 3/3 绕代理 + 启动 ----------
REM 铁律：系统代理（127.0.0.1:xxxxx，端口会变）会让 Gate 请求随机 502 隧道失败。
set "NO_PROXY=*"
set "no_proxy=*"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUNBUFFERED=1"

cd /d "%BASE%"
echo [3/3] NO_PROXY=* 已设置，工作目录 = %BASE%
echo.

if /i "%~1"=="wd" goto :WATCHDOG

REM ========== 默认：前台直跑引擎（输出实时可见） ==========
echo ------------------------------------------------------------
echo   模式: 前台直跑 server.py（Ctrl+C 或关窗口即停）
echo   看板: http://localhost:%PORT%
echo   日志: logs\engine-*.log
echo ------------------------------------------------------------
echo.
"%PY%" -u server.py --host 127.0.0.1 --port %PORT%
goto :ENDED

REM ========== 守护模式：watchdog.py ==========
:WATCHDOG
echo ------------------------------------------------------------
echo   模式: 守护（watchdog.py 每 20s 体检，引擎崩了自动重拉）
echo   看板:     http://localhost:%PORT%
echo   看门狗:   logs\watchdog.log
echo   引擎日志: logs\engine-*.log   ^(引擎自己的输出在这里^)
echo   [注意] 守护模式下引擎的实时输出被丢弃，排错请看上面这个文件
echo ------------------------------------------------------------
echo.
"%PY%" -u watchdog.py

:ENDED
echo.
echo ------------------------------------------------------------
echo   进程已退出（上方是它最后的输出）。
echo   - 按 Ctrl+C 停的 = 正常
echo   - 异常退出 = 去 logs\engine-*.log 里搜 "Traceback"
echo   场内若有持仓，交易所侧的 SL/TP 条件单仍然生效，不会裸奔。
echo ------------------------------------------------------------
pause
