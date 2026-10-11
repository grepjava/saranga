@echo off
rem The broker instance's command, as bin\artemis.cmd is an Artemis
rem instance's: saranga, run for this instance (made by saranga create).

setlocal

if NOT "%ARTEMIS_INSTANCE%"=="" goto CHECK_ARTEMIS_INSTANCE
PUSHD .
CD %~dp0..
set ARTEMIS_INSTANCE=%CD%
POPD

:CHECK_ARTEMIS_INSTANCE
if "%ARTEMIS_INSTANCE_ETC%"=="" set ARTEMIS_INSTANCE_ETC=${artemis.instance.etc}

rem SARANGA names another saranga to run.
if "%SARANGA%"=="" set SARANGA=${saranga}

"%SARANGA%" %*

endlocal
exit /b %ERRORLEVEL%
