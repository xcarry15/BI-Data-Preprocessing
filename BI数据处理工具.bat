@echo off
chcp 65001 >nul
title BI数据预处理工具

cd /d "%~dp0"

echo ====================================
echo    BI数据预处理工具
echo ====================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到Python，请先安装Python 3.x
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM 检查pandas是否安装
python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo [提示] 正在安装pandas库...
    pip install pandas openpyxl -q
    echo.
)

echo [启动] BI数据预处理工具
echo.
python "%~dp0\process_big_file.py"

pause
