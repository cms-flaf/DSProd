"""Registry of process customization modules.

Concrete `ProcessCustomization` subclasses register themselves with `@register_process`.
The models live in the `dsprod_models` submodule (repo cms-flaf/DSProdModels); importing it
imports every model subpackage, so `get_process(name)` resolves any registered process. A
`prod_setup` YAML selects one by its `process:` key.
"""

# Note: do NOT import dsprod_models at module import time — its plugins import register_process
# back from this module (circular). Registration is triggered lazily by `_import_models()`.

_registry = {}


def _import_models():
    """Import the `dsprod_models` package to trigger process registration."""
    try:
        import dsprod_models  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "could not import the `dsprod_models` package — is the DSProdModels submodule "
            "checked out? Run `git submodule update --init dsprod_models`."
        ) from exc


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
    _import_models()

    if name not in _registry:
        known = ", ".join(sorted(_registry)) or "<none>"
        raise KeyError(f"unknown process {name!r}; registered: {known}")
    return _registry[name]


def all_processes() -> "dict[str, ProcessCustomization]":
    _import_models()

    return dict(_registry)
