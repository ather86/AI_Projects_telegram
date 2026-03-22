@echo off
echo Starting all services...
start "" "E:\AI\AI_Projects_telegram\Telegram_bot\scripts\start_comfyui.bat"
timeout /t 30 /nobreak > nul
start "" "E:\AI\AI_Projects_telegram\Telegram_bot\scripts\start_manager_bot.bat"
timeout /t 5 /nobreak > nul
start "" "E:\AI\AI_Projects_telegram\Telegram_bot\scripts\start_telegram_bot.bat"
echo All services started.
