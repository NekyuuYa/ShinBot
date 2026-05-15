from __future__ import annotations

import importlib
from pathlib import Path


def test_agent_package_lazy_exports_are_resolvable() -> None:
    for init_file in Path("shinbot/agent").rglob("__init__.py"):
        if "_archive" in init_file.parts:
            continue
        module_name = ".".join(init_file.parent.parts)
        module = importlib.import_module(module_name)
        for export_name in getattr(module, "__all__", []):
            assert getattr(module, export_name) is not None
