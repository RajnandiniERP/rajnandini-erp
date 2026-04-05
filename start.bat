@echo off
title Rajnandini ERP
color 0A
echo.
echo  ==========================================
echo    Rajnandini Fashion India Ltd - ERP
echo  ==========================================
echo.

:: Get WiFi IP
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /c:"IPv4 Address"') do (
    set RAW_IP=%%a
    goto :gotip
)
:gotip
set LOCAL_IP=%RAW_IP: =%

echo  [1] SAME WIFI ACCESS:
echo      http://localhost:8000
echo      http://%LOCAL_IP%:8000
echo.

:: Start ngrok in background
echo  [2] INTERNET ACCESS (ngrok):
where ngrok >nul 2>&1
if %errorlevel% NEQ 0 (
    echo      ngrok.exe not found in this folder!
    echo      Download from https://ngrok.com/download
    echo      and paste ngrok.exe here.
    echo.
    goto :startserver
)

start /min cmd /c "ngrok http 8000 --log stdout > ngrok_log.txt 2>&1"
echo      Starting tunnel, please wait...
timeout /t 4 /nobreak >nul

for /f "usebackq delims=" %%u in (`powershell -NoProfile -Command "try { (Invoke-RestMethod http://localhost:4040/api/tunnels).tunnels | Where-Object { $_.proto -eq 'https' } | Select-Object -ExpandProperty public_url } catch { '' }"`) do set NGROK_URL=%%u

if defined NGROK_URL (
    echo      %NGROK_URL%
    echo.
    echo      Share this link - works from ANYWHERE on any device!
) else (
    echo      Tunnel starting... check http://localhost:4040
)
echo.

:startserver
echo  ==========================================
echo   Press Ctrl+C to stop
echo  ==========================================
echo.
python main.py
pause
