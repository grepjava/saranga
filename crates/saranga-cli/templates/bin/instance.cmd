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

rem The Java arguments of the profile Artemis's bin\artemis.cmd loads
rem (artemis-utility.profile.cmd, but for run), and nothing else of it: their
rem -D options are system properties for saranga run.
if not "%ARTEMIS_PROFILE%"=="" goto LOAD_ARTEMIS_PROFILE
set ARTEMIS_PROFILE=artemis-utility.profile.cmd
if "%1"=="run" set ARTEMIS_PROFILE=artemis.profile.cmd
:LOAD_ARTEMIS_PROFILE
if not exist "%ARTEMIS_INSTANCE_ETC%\%ARTEMIS_PROFILE%" goto PROFILE_LOADED
setlocal
call "%ARTEMIS_INSTANCE_ETC%\%ARTEMIS_PROFILE%" %*
endlocal & set "JAVA_ARGS=%JAVA_ARGS%" & set "JAVA_ARGS_APPEND=%JAVA_ARGS_APPEND%"
:PROFILE_LOADED

rem SARANGA names another saranga to run.
if "%SARANGA%"=="" set SARANGA=${saranga}

"%SARANGA%" %*

endlocal
exit /b %ERRORLEVEL%
