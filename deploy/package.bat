@echo off
setlocal enabledelayedexpansion

REM =========================================================================
REM  uGO-Brain packaging script
REM  Assembles a deployable folder from build output + project data.
REM
REM  Usage:
REM      deploy\package.bat C:\uGO            (package to C:\uGO)
REM      deploy\package.bat                   (default: dist\uGO-deploy)
REM =========================================================================

set "PROJECT_ROOT=%~dp0.."
pushd "%PROJECT_ROOT%"

set "DIST_DIR=C:\uGO-build\dist"
set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=%DIST_DIR%\uGO-deploy"

echo.
echo ===== uGO-Brain Packaging =====
echo Source:  %DIST_DIR%
echo Target:  %TARGET%
echo.

REM -- Verify build outputs exist -------------------------------------------
if not exist "%DIST_DIR%\planner_service\planner_service.exe" (
    echo ERROR: planner_service.exe not found. Run deploy\build.bat first.
    goto :fail
)
if not exist "%DIST_DIR%\world_config_gui\world_config_gui.exe" (
    echo ERROR: world_config_gui.exe not found. Run deploy\build.bat first.
    goto :fail
)

REM -- Create deployment directory structure ---------------------------------
echo Creating directory structure ...
mkdir "%TARGET%" 2>nul

REM Executables (onedir folders)
echo Copying planner_service ...
xcopy /E /I /Y /Q "%DIST_DIR%\planner_service" "%TARGET%\planner_service"
echo Copying world_config_gui ...
xcopy /E /I /Y /Q "%DIST_DIR%\world_config_gui" "%TARGET%\world_config_gui"

REM Config / data directories
mkdir "%TARGET%\world\versions" 2>nul
mkdir "%TARGET%\world\world_config_gui\static" 2>nul
mkdir "%TARGET%\runtime" 2>nul
mkdir "%TARGET%\tracing" 2>nul
mkdir "%TARGET%\planning" 2>nul
mkdir "%TARGET%\Library" 2>nul
mkdir "%TARGET%\logs" 2>nul

REM -- Copy editable config files (do NOT overwrite existing) ----------------
echo Copying config files ...

if not exist "%TARGET%\world\world_config.json" (
    if exist "%PROJECT_ROOT%\world\world_config.json" (
        copy /Y "%PROJECT_ROOT%\world\world_config.json" "%TARGET%\world\world_config.json" >nul
    )
)

if not exist "%TARGET%\world\world_config_gui\mir_config.json" (
    if exist "%PROJECT_ROOT%\world\world_config_gui\mir_config.json" (
        copy /Y "%PROJECT_ROOT%\world\world_config_gui\mir_config.json" "%TARGET%\world\world_config_gui\mir_config.json" >nul
    )
)

if not exist "%TARGET%\Available_Tasks.json" (
    if exist "%PROJECT_ROOT%\Available_Tasks.json" (
        copy /Y "%PROJECT_ROOT%\Available_Tasks.json" "%TARGET%\Available_Tasks.json" >nul
    )
)

if not exist "%TARGET%\planning\process_policies.json" (
    if exist "%PROJECT_ROOT%\planning\process_policies.json" (
        copy /Y "%PROJECT_ROOT%\planning\process_policies.json" "%TARGET%\planning\process_policies.json" >nul
    )
)

REM -- Copy static web assets (always overwrite — these are code, not config) -
echo Copying static web assets ...
xcopy /E /I /Y /Q "%PROJECT_ROOT%\world\world_config_gui\static" "%TARGET%\world\world_config_gui\static"

REM -- Copy Library modules (needed if workflow runs via source fallback) -----
echo Copying Library modules ...
xcopy /E /I /Y /Q "%PROJECT_ROOT%\Library\*.py" "%TARGET%\Library"

echo.
echo ===== Packaging complete =====
echo.
echo Deployment folder: %TARGET%
echo.
echo   %TARGET%\
echo     planner_service\planner_service.exe    Planner bridge daemon
echo     world_config_gui\world_config_gui.exe  World config web GUI
echo     world\world_config.json                Editable config
echo     world\world_config_gui\mir_config.json Editable MiR config
echo     world\world_config_gui\static\         Web UI assets
echo     planning\process_policies.json          Editable process policies
echo     Available_Tasks.json                    Editable task catalog
echo     runtime\                                Runtime state files
echo     tracing\                                Trace output
echo     logs\                                   Application logs
echo.

popd
exit /b 0

:fail
popd
exit /b 1
