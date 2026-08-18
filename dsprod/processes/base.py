"""Per-process customization interface.

Each physics process (e.g. X->HH->bbWW) ships a `ProcessCustomization` subclass that
knows how to turn a compact process configuration into concrete production points, where its
gridpack lives in the store (and how to generate it if absent), and how to render the CMSSW gen
fragment. Everything process-specific lives here; the law tasks stay generic.
"""

import math
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class GridpackSpec:
    """How to *generate* the gridpack when it is not already available in the store.

    `MakeGridpack` runs `genproductions_scripts/bin/<generator>/gridpack_generation.sh`
    against the cards rendered by the process (see `render_gridpack_cards`); `cards_template`
    records where those cards live.
    """

    generator: str = "MadGraph5_aMCatNLO"
    cards_template: str = ""


@dataclass
class Point:
    """A concrete production unit: one physics point of one process."""

    process: str  # registry key of the owning ProcessCustomization, e.g. "X_HH"
    name: str  # canonical storage name, e.g. "GluGlutoRadiontoHHto2B2Vto2B2L2Nu_M-800"
    params: dict = field(
        default_factory=dict
    )  # process parameters (mass, spin, channel, ...)
    #: events to produce **per era**, {era: n}. An era that is missing produces nothing, so one
    #: setup covers every era instead of one setup per era.
    events_total: dict = field(default_factory=dict)
    events_per_job: int = 0

    def n_events(self, era):
        """Events to produce for `era` (0 when this point is not produced there)."""
        return int(self.events_total.get(era, 0))

    def n_jobs(self, era):
        """Number of production jobs (seeds) for `era`."""
        n = self.n_events(era)
        if n <= 0 or self.events_per_job <= 0:
            return 0
        return math.ceil(n / self.events_per_job)


def events_per_era(value, eras):
    """Normalize a setup's `events_total` into `{era: n}`.

    Accepts a scalar (the same number in every era) or a mapping. An unknown era is an error
    rather than a silent zero — a typo there would produce nothing at all.
    """
    if isinstance(value, dict):
        unknown = sorted(set(value) - set(eras))
        if unknown:
            raise ValueError(
                f"events_total refers to era(s) {unknown} that the setup does not produce; "
                f"eras are {list(eras)}"
            )
        return {era: int(value.get(era, 0)) for era in eras}
    return {era: int(value) for era in eras}


class ProcessCustomization(ABC):
    #: registry key; every concrete subclass must set a unique, non-empty name
    name: Optional[str] = None

    @abstractmethod
    def enumerate_points(self, process_cfg: dict) -> "list[Point]":
        """Expand the process configuration (e.g. a mass scan) into concrete points."""

    @abstractmethod
    def gridpack(self, point: Point, era: str) -> GridpackSpec:
        """Return how to *generate* this point's gridpack (used only when it is not already
        available in the DSProdGridpacks store)."""

    @abstractmethod
    def gen_fragment(self, point: Point, era: str) -> str:
        """Render the CMSSW gen fragment for this point/era and return its path."""

    # ---- optional overrides -------------------------------------------------
    def point_name(self, point: Point) -> str:
        return point.name

    def gridpack_name(self, point: Point) -> str:
        """Name of the gridpack (channel-independent; the resonance mass etc. live in it)."""
        return self.point_name(point)

    def gridpack_rel_path(self, point: Point, era: str = None) -> str:
        """Canonical path of this point's gridpack inside the DSProdGridpacks store, relative
        to the `gridpacks` submodule root. `MakeGridpack` imports it from here if present
        and otherwise generates it. Override to mirror the model's own directory convention.
        """
        return os.path.join(self.name, self.gridpack_name(point), "gridpack.tar.xz")

    def render_gridpack_cards(self, point: Point, out_dir: str) -> str:
        """Render the genproductions input cards for `point` into out_dir (generate mode).

        Writes `<NAME>_proc_card.dat` / `_run_card.dat` / `_customizecards.dat` /
        `_extramodels.dat` and returns NAME (== the proc_card `output` name). Default: unsupported.
        """
        raise NotImplementedError(f"{self.name} does not support gridpack generation")

    def xsec(self, point: Point) -> Optional[float]:
        return None

    def filter_efficiency(self, point: Point) -> Optional[float]:
        return None

    def validate(self, point: Point) -> None:
        """Optional per-point sanity check; raise on invalid configuration."""
