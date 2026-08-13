# Processes

Everything process-specific in DSProd lives in a **process customization module**. The tasks stay
generic; a module teaches DSProd how to turn a compact process configuration into concrete
production points, how to obtain each point's gridpack, and how to render the CMSSW gen fragment.

The interface is the `ProcessCustomization` abstract base class in `dsprod/processes/base.py`; the
reference implementation is `dsprod/processes/x_hh_bbww.py` (X→HH→bbWW).

## The interface

A process module subclasses `ProcessCustomization`, sets a unique `name`, and implements three
required methods:

```python
from dsprod.registry import register_process
from dsprod.processes.base import ProcessCustomization, Point, GridpackSpec


@register_process
class MyProcess(ProcessCustomization):
    name = "My_Process"   # the prod_setup `process:` key

    def enumerate_points(self, process_cfg: dict) -> list[Point]:
        """Expand the setup's `points:` (e.g. a mass scan) into concrete Point objects."""

    def gridpack(self, point: Point, era: str) -> GridpackSpec:
        """Return how to obtain the gridpack: import an existing tarball or generate one."""

    def gen_fragment(self, point: Point, era: str) -> str:
        """Render the CMSSW gen fragment for this point/era and return its path."""
```

The `@register_process` decorator registers the class under its `name`. Because
`dsprod/processes/__init__.py` imports every process module, `get_process(name)` then resolves it
from any [production setup](prod-setups.md).

### `GridpackSpec`

`gridpack()` returns a `GridpackSpec` with one of two modes:

- `GridpackSpec(mode="existing", location=...)` — import a ready tarball (local path, `/eos`
  path, or `davs://` URL);
- `GridpackSpec(mode="generate", cards_template=..., generator="MadGraph5_aMCatNLO")` — generate
  it from cards, via the named `genproductions_scripts` generator.

For generate mode, override `render_gridpack_cards(point, out_dir)` to write the
`genproductions` input cards (`proc_card` / `run_card` / `customizecards` / `extramodels`) into
`out_dir`, and return the process `NAME`.

### Optional overrides

`point_name`, `gridpack_name`, `xsec`, `filter_efficiency`, and `validate` have sensible defaults
and can be overridden when a process needs them.

## Templates

Card and fragment templates live under `config/process_templates/<process>/`. For `X_HH_bbWW`:

```
config/process_templates/X_HH_bbWW/
├── cards/            # genproductions input cards (proc_card, run_card, customizecards, extramodels)
└── fragment.py       # CMSSW gen fragment template
```

The module fills in the per-point values (mass, decay channel, event count) when it renders these
for a given point/era.

## Adding a process

1. Add card/fragment templates under `config/process_templates/<process>/`.
2. Add a `ProcessCustomization` subclass in `dsprod/processes/<process>.py` and import it from
   `dsprod/processes/__init__.py`.
3. Write a [production setup](prod-setups.md) with `process: <name>` and its `points:`.

No task code changes — the graph is generic.
