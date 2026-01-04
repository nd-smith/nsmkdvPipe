@echo off
REM Token refresh script for Kafka pipeline
REM Refreshes Azure tokens every 45 minutes (tokens expire in ~60 min)
REM Usage: refresh_tokens.bat [interval_minutes]

setlocal enabledelayedexpansion

set INTERVAL_MINUTES=%1
if "%INTERVAL_MINUTES%"=="" set INTERVAL_MINUTES=45

set TOKEN_FILE=src\tokens.json
set INTERVAL_SECONDS=0
set /a INTERVAL_SECONDS=%INTERVAL_MINUTES%*60

echo ============================================================
echo Token Refresh Script
echo Token file: %TOKEN_FILE%
echo Refresh interval: %INTERVAL_MINUTES% minutes
echo ============================================================
echo.

:refresh_loop
echo [%date% %time%] Refreshing tokens...

REM Get storage token
for /f "delims=" %%a in ('az account get-access-token --resource https://storage.azure.com/ --query accessToken -o tsv 2^>nul') do set STORAGE_TOKEN=%%a

if "%STORAGE_TOKEN%"=="" (
    echo ERROR: Failed to get storage token. Run 'az login' first.
    goto :error
)

REM Get Kusto token
for /f "delims=" %%a in ('az account get-access-token --resource https://kusto.kusto.windows.net --query accessToken -o tsv 2^>nul') do set KUSTO_TOKEN=%%a

if "%KUSTO_TOKEN%"=="" (
    echo ERROR: Failed to get Kusto token. Run 'az login' first.
    goto :error
)

REM Write tokens.json
echo {> %TOKEN_FILE%
echo   "https://storage.azure.com/": "%STORAGE_TOKEN%",>> %TOKEN_FILE%
echo   "https://kusto.kusto.windows.net": "%KUSTO_TOKEN%">> %TOKEN_FILE%
echo }>> %TOKEN_FILE%

echo [%date% %time%] Tokens refreshed successfully
echo   - Storage token: %STORAGE_TOKEN:~0,20%...
echo   - Kusto token: %KUSTO_TOKEN:~0,20%...
echo.
echo Next refresh in %INTERVAL_MINUTES% minutes. Press Ctrl+C to stop.
echo.

REM Wait for interval
timeout /t %INTERVAL_SECONDS% /nobreak > nul
goto :refresh_loop

:error
echo.
echo Token refresh failed. Please run 'az login' and try again.
exit /b 1
