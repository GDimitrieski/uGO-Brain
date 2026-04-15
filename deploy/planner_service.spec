# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for planner_service.exe

Build:
    cd <project_root>
    pyinstaller deploy/planner_service.spec
"""

import os
import sys
import glob
from PyInstaller.utils.hooks import collect_submodules

PROJECT_ROOT = os.path.abspath(os.path.join(SPECPATH, os.pardir))
LIB_DIR = os.path.join(PROJECT_ROOT, "Library")

# CRITICAL: add project root to sys.path so collect_submodules can find
# project packages (engine, workflows, etc.) at build time.
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, LIB_DIR)

# ---------------------------------------------------------------------------
# Hidden imports: everything the workflow subprocess may need at runtime.
# ---------------------------------------------------------------------------

hidden = []

# Proper packages (have __init__.py) — collect all submodules automatically.
for pkg in ("engine", "planning", "routing", "Device", "world", "workflows"):
    found = collect_submodules(pkg)
    print(f"  collect_submodules('{pkg}'): {len(found)} modules")
    hidden += found

# Library/ has no __init__.py — list each module explicitly.
for py in glob.glob(os.path.join(LIB_DIR, "*.py")):
    mod_name = os.path.splitext(os.path.basename(py))[0]
    if mod_name.startswith("__"):
        continue
    hidden.append(mod_name)

# Ensure requests is included (used by Library modules, Device, routing, etc.)
hidden.append("requests")

print(f"  Total hidden imports: {len(hidden)}")

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

a = Analysis(
    [os.path.join(PROJECT_ROOT, "integration", "planner_web_interface", "service.py")],
    pathex=[PROJECT_ROOT, LIB_DIR],
    hiddenimports=sorted(set(hidden)),
    datas=[],          # all data files live on disk alongside the exe
    binaries=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["tkinter", "matplotlib", "PIL", "numpy", "scipy", "pandas"],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="planner_service",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="planner_service",
)
