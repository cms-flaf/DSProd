"""Global DSProd configuration: `config/global.yaml` merged with `config/user_custom.yaml`.

Same idea as FLAF: `global.yaml` holds the defaults for all runs; `user_custom.yaml` holds the
user-specific overrides (their EOS storage area, CRAB `out_lfn_base`, ...) and is merged on top
(scalars and lists override; nested maps are deep-merged). This keeps deployment/site settings out
of the production setups, so a setup is identical whatever backend it is submitted with.
"""

import os

import yaml

_global_cfg = None


def _load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def _deep_merge(base, override):
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def get_global():
    """The merged global config (cached). global.yaml with user_custom.yaml layered on top."""
    global _global_cfg
    if _global_cfg is None:
        cfg_dir = os.path.join(os.environ["ANALYSIS_PATH"], "config")
        cfg = _load_yaml(os.path.join(cfg_dir, "global.yaml"))
        user_path = os.path.join(cfg_dir, "user_custom.yaml")
        if os.path.exists(user_path):
            cfg = _deep_merge(cfg, _load_yaml(user_path))
        _global_cfg = cfg
    return _global_cfg
