"""Per-process customization interface.

Each physics process (e.g. X->HH->bbWW) ships a `ProcessCustomization` subclass that
knows how to turn a compact process configuration into concrete production points, how
to obtain the gridpack (existing or generated), and how to render the CMSSW gen fragment.
Everything process-specific lives here; the law tasks stay generic.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class GridpackSpec:
    """How to obtain the gridpack for a point in a given era.

    mode == "existing": `location` points at a ready gridpack tarball (local path,
        /eos path, or davs://... URL) to be imported/validated.
    mode == "generate": run `genproductions_scripts/bin/<generator>/gridpack_generation.sh`
        against the cards rendered from `cards_template`.
    """

    mode: str
    location: str = ""
    generator: str = "MadGraph5_aMCatNLO"
    cards_template: str = ""

    def __post_init__(self):
        if self.mode not in ("existing", "generate"):
            raise ValueError(
                f"GridpackSpec.mode must be existing|generate, got {self.mode!r}"
            )
        if self.mode == "existing" and not self.location:
            raise ValueError("GridpackSpec(existing) requires a location")
        if self.mode == "generate" and not self.cards_template:
            raise ValueError("GridpackSpec(generate) requires a cards_template")


@dataclass
class Point:
    """A concrete production unit: one physics point of one process."""

    process: str  # registry key of the owning ProcessCustomization, e.g. "X_HH_bbWW"
    name: str  # canonical storage name, e.g. "GluGlutoRadiontoHHto2B2Vto2B2L2Nu_M-800"
    params: dict = field(default_factory=dict)  # process parameters (mass, spin, ...)
    events_total: int = 0
    events_per_job: int = 0


class ProcessCustomization(ABC):
    #: registry key; every concrete subclass must set a unique, non-empty name
    name: Optional[str] = None

    @abstractmethod
    def enumerate_points(self, process_cfg: dict) -> "list[Point]":
        """Expand the process configuration (e.g. a mass scan) into concrete points."""

    @abstractmethod
    def gridpack(self, point: Point, era: str) -> GridpackSpec:
        """Return how to obtain the gridpack for this point/era (existing or generate)."""

    @abstractmethod
    def gen_fragment(self, point: Point, era: str) -> str:
        """Render the CMSSW gen fragment for this point/era and return its path."""

    # ---- optional overrides -------------------------------------------------
    def point_name(self, point: Point) -> str:
        return point.name

    def xsec(self, point: Point) -> Optional[float]:
        return None

    def filter_efficiency(self, point: Point) -> Optional[float]:
        return None

    def validate(self, point: Point) -> None:
        """Optional per-point sanity check; raise on invalid configuration."""
