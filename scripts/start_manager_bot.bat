@echo off
echo Starting Manager Bot...
set "PROJECT_DIR=E:\AI\AI_Projects_telegram\Telegram_bot"
set "PYTHON_EXE=%PROJECT_DIR%\venv\Scripts\python.exe"
set "BOT_SCRIPT=%PROJECT_DIR%\manager_bot.py"
set "BOT_LOG=%PROJECT_DIR%\manager_bot_log.txt"

cd /d "%PROJECT_DIR%"
if not exist "%PYTHON_EXE%" (
	echo ERROR: Python executable not found at %PYTHON_EXE%
	exit /b 1
)

start "ManagerBot" cmd /c ""%PYTHON_EXE%" -B "%BOT_SCRIPT%" >> "%BOT_LOG%" 2>&1"
echo Manager Bot started.
