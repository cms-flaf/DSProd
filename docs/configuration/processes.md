# Processes

Everything process-specific in DSProd lives in a **process customization module**. The tasks stay
generic; a module teaches DSProd how to turn a compact process configuration into concrete
production points, how to obtain each point's gridpack, and how to render the CMSSW gen fragment.

The interface — the `ProcessCustomization` abstract base class — lives in DSProd itself
(`dsprod/processes/base.py`). The **models** (plugins + cards + fragments) live in a separate
submodule, [DSProdModels](https://github.com/cms-flaf/DSProdModels), mounted at `models/`.
That tree is **content, not a Python package** — it has no `__init__.py` anywhere; DSProd loads each
`plugin.py` straight from its path. The reference model is `models/X_HH_bbWW/` (X→HH→bbWW).

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
        """Return how to *generate* the gridpack (used only when it is not already stored)."""

    def gen_fragment(self, point: Point, era: str) -> str:
        """Render the CMSSW gen fragment for this point/era and return its path."""
```

The `@register_process` decorator registers the class under its `name`. `get_process(name)`
walks the models tree, loads every `plugin.py`, and then resolves the name from any
[production setup](prod-setups.md).

### Gridpacks: locate-or-generate

A point never names its gridpack. `MakeGridpack` computes the gridpack's canonical location in the
[DSProdGridpacks](https://github.com/cms-flaf/DSProdGridpacks) store via
`gridpack_rel_path(point)` and imports it if present, otherwise generates it. So the plugin
provides two things:

- **where the gridpack lives** — override `gridpack_rel_path(point, era)` to return the path
  (relative to the `gridpacks` store) mirroring the model's own directory convention;
- **how to generate it** — `gridpack()` returns a
  `GridpackSpec(generator="MadGraph5_aMCatNLO", cards_template=...)`, and
  `render_gridpack_cards(point, out_dir)` writes the `genproductions` input cards (`proc_card` /
  `run_card` / `customizecards` / `extramodels`) into `out_dir` and returns the process `NAME`.

### Optional overrides

`point_name`, `gridpack_name`, `gridpack_rel_path`, `xsec`, `filter_efficiency`, and `validate`
have sensible defaults and can be overridden when a process needs them.

A plugin lives in the `models` submodule and imports DSProd's framework classes, so it is
usable only inside a DSProd checkout (not as a standalone library). It resolves its cards and
fragment relative to its own location (`os.path.dirname(__file__)`), so a model is self-contained.

## Model layout

Inside `models`, models are organized by **process → generator → center-of-mass energy**,
with the energy as the *last* level so the plugin and the process/generator tooling above it are
shared across energies. For `X_HH_bbWW`:

```
models/                                 (the DSProdModels submodule, mounted at models/)
└── X_HH_bbWW/                          # process (matches the plugin name)
    ├── README.md                       # process docs + links to the original sources
    ├── filters/                        # (optional) final-state filters, shared across generators/energies
    └── MadGraph5_aMCatNLO/             # generator (matches genproductions_scripts/bin + GridpackSpec.generator)
        ├── plugin.py                   # the ProcessCustomization, @register_process (shared across energies)
        ├── scripts/                    # (optional) prodcard-generation scripts (e.g. parametrized in mX)
        ├── models/                     # (optional) custom generator models, when not centrally available
        └── 13p6TeV/                    # center-of-mass energy (LAST level)
            ├── cards/                  # genproductions cards (proc_card, run_card, customizecards, extramodels)
            └── fragment.py             # CMSSW gen fragment
```

Only `plugin.py`, `cards/`, `fragment.py`, and the READMEs are required; `filters/`, `scripts/`,
and `models/` appear only when a model needs them. The plugin resolves its energy-specific inputs
via `com_energy(era)` and fills in the per-point values (mass, …) when it renders the cards.

DSProd **walks this tree and loads every `plugin.py`**, so a model becomes available simply by
adding its directory — there is no central registration list, and no `__init__.py`, to edit.

## Adding a process

In the [DSProdModels](https://github.com/cms-flaf/DSProdModels) repository:

1. Create `<process>/<generator>/` with a `plugin.py` — a `ProcessCustomization` subclass
   decorated with `@register_process` and a unique `name`.
2. Add a `<process>/<generator>/<comEnergy>/` folder with the `cards/` and `fragment.py` for each
   energy you produce, and document the process in `<process>/README.md`.

Then in DSProd, write a [production setup](prod-setups.md) with `process: <name>` and its
`points:`, and advance the `models` submodule pointer. No task code changes — the graph is
generic.
