@echo off
rem The broker instance's command, as bin\artemis.cmd is an Artemis
rem instance's: saranga, run for this instance (made by saranga create).

setlocal

if NOT "%ARTEMIS_INSTANCE%"=="" goto CHECK_ARTEMIS_INSTANCE
rem PUSHD changes the drive too, where CD would not.
PUSHD "%~dp0.."
set ARTEMIS_INSTANCE=%CD%
POPD

:CHECK_ARTEMIS_INSTANCE
rem As Artemis's bin\artemis.cmd sets it, where upgrade looks for it.
set ARTEMIS_INSTANCE_ETC="${artemis.instance.etc}"
if defined ARTEMIS_INSTANCE_ETC set ARTEMIS_INSTANCE_ETC=%ARTEMIS_INSTANCE_ETC:"=%

rem SARANGA names another saranga to run.
if "%SARANGA%"=="" set SARANGA=${saranga}

"%SARANGA%" %*

endlocal
exit /b %ERRORLEVEL%
