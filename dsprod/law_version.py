"""The one law release DSProd runs with, refused at import when another one is loaded.

DSProd overrides law's CRAB job manager, its status parsing and its poll loop hooks, and those
change between releases without notice: law 0.1.21 renamed the job manager's `proxy` argument,
so 0.1.20-era code failed every submission and every status query with a TypeError that the
query retry path reported as an unreadable status. Mixing releases is just as quiet -- the job
script and the CRAB wrapper are rendered by the driver's law, the payload runs the copy vendored
into the worker tarball.

`tools.py` imports this module, and every module of the package that uses law imports `tools.py`,
so the check runs before any of them is usable. Keep `LAW_VERSION` equal to `law_version` in
`env.sh` and to the pin in `.github/workflows/unit-tests.yaml`.
"""

import os

import law

LAW_VERSION = "0.1.21"

if law.__version__ != LAW_VERSION:
    raise RuntimeError(
        f"DSProd needs law {LAW_VERSION}, but law {law.__version__} was imported from "
        f"{os.path.dirname(os.path.abspath(law.__file__))}. Source env.sh again in this "
        "checkout: it rebuilds soft/dsprod_env and re-vendors soft/vendor whenever they hold "
        "another law. On a CRAB worker the imported copy is the soft/vendor shipped with the "
        "job, so do that on the submitting machine and resubmit."
    )
