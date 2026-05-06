@echo off
title LDB Server
echo ==================================================
echo   LDB Avg Balance Calculator - Starting...
echo ==================================================
echo.

cd /d D:\ldb_web

echo [1/2] Starting Flask server...
start "Flask" cmd /k "cd /d D:\ldb_web && python app.py"

echo [2/2] Starting ngrok (wait 3s)...
timeout /t 3 /nobreak >nul
start "ngrok" cmd /k "cd /d D:\ldb_web && ngrok http 5000 --url shakeable-willette-sippingly.ngrok-free.dev"

timeout /t 5 /nobreak >nul
echo.
echo ==================================================
echo   DONE!
echo.
echo   Local:  http://localhost:5000
echo   Online: https://shakeable-willette-sippingly.ngrok-free.dev
echo.
echo   Login:  admin / ldb2024
echo ==================================================
echo.
echo Close this window anytime. Keep the other 2 CMD open.
pause
