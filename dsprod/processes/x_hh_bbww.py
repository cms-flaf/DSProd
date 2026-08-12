"""X -> HH -> bb WW (resonant, radion/graviton) process customization.

Points are a resonance-mass scan. The gridpack is MadGraph (13.6 TeV), era-independent; it may
be supplied per point (`gridpack:` in the setup, existing mode) or generated (Phase 5). The gen
fragment (Pythia8 CP5 hadronizer) is common to all masses — the resonance mass lives in the
gridpack.
"""

import os

from ..registry import register_process
from .base import GridpackSpec, Point, ProcessCustomization


@register_process
class XHHbbWW(ProcessCustomization):
    name = "X_HH_bbWW"

    def enumerate_points(self, process_cfg):
        events_per_job = process_cfg.get("events_per_job", 0)
        points = []
        for p in process_cfg["points"]:
            points.append(
                Point(
                    process=self.name,
                    name=p["name"],
                    params={
                        k: v for k, v in p.items() if k not in ("name", "events_total")
                    },
                    events_total=p["events_total"],
                    events_per_job=p.get("events_per_job", events_per_job),
                )
            )
        return points

    def gridpack(self, point, era=None):
        loc = point.params.get("gridpack")
        if loc:
            return GridpackSpec(mode="existing", location=loc)
        return GridpackSpec(
            mode="generate",
            generator="MadGraph5_aMCatNLO",
            cards_template="config/process_templates/X_HH_bbWW/cards",
        )

    def gen_fragment(self, point, era=None):
        return os.path.join(
            os.environ["ANALYSIS_PATH"],
            "config",
            "process_templates",
            "X_HH_bbWW",
            "fragment.py",
        )
