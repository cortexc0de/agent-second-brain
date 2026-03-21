from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

_MISSING = object()
_ORIGINAL_MODULES: dict[str, object] = {}


def _install_aiogram_runtime_stubs() -> None:
    for module_name in (
        "aiogram",
        "aiogram.client.default",
        "aiogram.enums",
        "aiogram.fsm.storage.memory",
        "aiogram.types",
        "d_brain.config",
        "d_brain.bot.handlers",
    ):
        _ORIGINAL_MODULES.setdefault(module_name, sys.modules.get(module_name, _MISSING))

    aiogram_module = types.ModuleType("aiogram")
    client_default_module = types.ModuleType("aiogram.client.default")
    enums_module = types.ModuleType("aiogram.enums")
    fsm_memory_module = types.ModuleType("aiogram.fsm.storage.memory")
    types_module = types.ModuleType("aiogram.types")

    class Bot:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    class Dispatcher:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs
            self.routers: list[object] = []

        def include_router(self, router) -> None:
            self.routers.append(router)

    class DefaultBotProperties:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    class ParseMode:
        HTML = "HTML"

    class MemoryStorage:
        pass

    class Update:
        pass

    aiogram_module.Bot = Bot
    aiogram_module.Dispatcher = Dispatcher
    client_default_module.DefaultBotProperties = DefaultBotProperties
    enums_module.ParseMode = ParseMode
    fsm_memory_module.MemoryStorage = MemoryStorage
    types_module.Update = Update

    sys.modules["aiogram"] = aiogram_module
    sys.modules["aiogram.client.default"] = client_default_module
    sys.modules["aiogram.enums"] = enums_module
    sys.modules["aiogram.fsm.storage.memory"] = fsm_memory_module
    sys.modules["aiogram.types"] = types_module


def _install_project_stubs() -> None:
    config_module = types.ModuleType("d_brain.config")
    config_module.Settings = type("Settings", (), {})
    sys.modules["d_brain.config"] = config_module

    handlers_module = types.ModuleType("d_brain.bot.handlers")
    for name in ("buttons", "commands", "decide", "do", "forward", "photo", "process", "review", "text", "voice", "weekly"):
        setattr(handlers_module, name, types.SimpleNamespace(router=f"{name}-router"))
    sys.modules["d_brain.bot.handlers"] = handlers_module


def _load_main_module():
    _install_aiogram_runtime_stubs()
    _install_project_stubs()
    path = Path(__file__).resolve().parents[1] / "src" / "d_brain" / "bot" / "main.py"
    spec = importlib.util.spec_from_file_location("test_bot_main_module", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["test_bot_main_module"] = module
    spec.loader.exec_module(module)
    return module


def _restore_modules() -> None:
    for module_name, original in _ORIGINAL_MODULES.items():
        if original is _MISSING:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = original


bot_main = _load_main_module()
_restore_modules()


class BotMainTests(unittest.TestCase):
    def test_create_dispatcher_includes_routers_in_expected_order(self) -> None:
        _install_aiogram_runtime_stubs()
        _install_project_stubs()
        try:
            dispatcher = bot_main.create_dispatcher()
        finally:
            _restore_modules()

        self.assertEqual(
            dispatcher.routers,
            [
                "commands-router",
                "process-router",
                "weekly-router",
                "decide-router",
                "review-router",
                "do-router",
                "buttons-router",
                "voice-router",
                "photo-router",
                "forward-router",
                "text-router",
            ],
        )


if __name__ == "__main__":
    unittest.main()
