"""Registry of process customization modules.

Concrete `ProcessCustomization` subclasses register themselves with `@register_process`.
`dsprod.processes` imports every process module on import, so `get_process(name)` resolves
any registered process. A `prod_setup` YAML selects one by its `process:` key.
"""

# Note: do NOT import from .processes here — dsprod.processes eagerly imports the concrete
# process modules, which import register_process back from this module (circular). Registration
# is triggered lazily in get_process/all_processes via `import dsprod.processes`.

_registry = {}


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
    import dsprod.processes  # noqa: F401  (ensure all process modules are imported)

    if name not in _registry:
        known = ", ".join(sorted(_registry)) or "<none>"
        raise KeyError(f"unknown process {name!r}; registered: {known}")
    return _registry[name]


def all_processes() -> "dict[str, ProcessCustomization]":
    import dsprod.processes  # noqa: F401

    return dict(_registry)
