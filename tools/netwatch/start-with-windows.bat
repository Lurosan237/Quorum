@echo off
rem Double-click once: netwatch starts now, in the background, and again every time you log in.
setlocal
set "HERE=%~dp0"
set "PYW="
for /f "delims=" %%i in ('where pythonw 2^>nul') do if not defined PYW set "PYW=%%i"
if not defined PYW for /f "delims=" %%i in ('where pyw 2^>nul') do if not defined PYW set "PYW=%%i"
if not defined PYW (
  echo Python was not found on this computer.
  echo Install it from https://www.python.org/downloads/ and tick "Add python.exe to PATH",
  echo then double-click this file again.
  pause
  exit /b 1
)
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
> "%STARTUP%\netwatch.cmd" echo @start "" "%PYW%" "%HERE%netwatch.py"
start "" "%PYW%" "%HERE%netwatch.py"
echo.
echo Netwatch is now recording in the background (no window), and will start again
echo every time you log in. Its data goes to:
echo   %HERE%netwatch-data
echo.
echo Keep the laptop plugged in and set it never to sleep while plugged in:
echo   Settings ^> System ^> Power ^> Screen and sleep ^> "When plugged in, put my device to sleep after": Never
echo.
echo To stop it for good: delete netwatch.cmd from %STARTUP%
echo and end "pythonw" in Task Manager.
pause
