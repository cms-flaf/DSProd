"""Registry of process customization modules.

Concrete `ProcessCustomization` subclasses register themselves with `@register_process`. The
models live in the `models` submodule (repo cms-flaf/DSProdModels): this module walks that tree
and loads every `plugin.py` it finds, so `get_process(name)` resolves any registered process. A
`prod_setup` YAML selects one by its `process:` key.

The models tree is *content*, not a Python package — it needs no `__init__.py` anywhere, and
nothing there has to be importable by name. Plugins are loaded straight from their file path,
which also keeps directory names free (e.g. `13p6TeV`, which is not a valid identifier).
"""

import importlib.util
import os
import pathlib
import re
import sys

# Note: loading happens lazily (not at module import time), because every plugin imports
# `register_process` back from this module.

_registry = {}
_models_loaded = False


def models_path():
    """Root of the models tree (the DSProdModels submodule)."""
    return os.path.join(os.environ["ANALYSIS_PATH"], "models")


def _load_plugins():
    """Import every `<models>/**/plugin.py` once, triggering `@register_process`."""
    global _models_loaded
    if _models_loaded:
        return
    root = pathlib.Path(models_path())
    if not root.is_dir():
        raise RuntimeError(
            f"models directory not found at {root} — is the DSProdModels submodule checked out? "
            "Run `git submodule update --init models`."
        )
    for plugin_path in sorted(root.rglob("plugin.py")):
        rel = plugin_path.relative_to(root).with_suffix("")
        # synthesize a unique, valid module name from the path (directory names may start with a
        # digit, e.g. 13p6TeV, so sanitize rather than use the path parts verbatim)
        suffix = "_".join(re.sub(r"\W", "_", part) for part in rel.parts)
        mod_name = f"dsprod.models.{suffix}"
        spec = importlib.util.spec_from_file_location(mod_name, plugin_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = module
        spec.loader.exec_module(module)
    _models_loaded = True


def register_process(cls):
    """Class decorator: instantiate and register a ProcessCustomization subclass."""
    inst = cls()
    if not getattr(inst, "name", None):
        raise RuntimeError(f"{cls.__name__} must define a non-empty `name`")
    if inst.name in _registry:
        raise RuntimeError(f"process {inst.name!r} is already registered")
    _registry[inst.name] = inst
    return cls


def get_process(name: str) -> "ProcessCustomization":
    _load_plugins()

    if name not in _registry:
        known = ", ".join(sorted(_registry)) or "<none>"
        raise KeyError(f"unknown process {name!r}; registered: {known}")
    return _registry[name]


def all_processes() -> "dict[str, ProcessCustomization]":
    _load_plugins()

    return dict(_registry)
