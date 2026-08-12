# X->HH->bbWW Pythia8 hadronizer fragment (CP5, 13.6 TeV).
#
# TEMPLATE — PLACEHOLDER. The decay chain (H->bb, H->WW->2L2Nu) is produced in the matrix
# element (MadGraph + MadSpin), so Pythia only showers/hadronizes. This mirrors the standard
# madgraph-pythia8 CP5 structure but MUST be replaced by the exact central fragment before real
# production: fetch it from McM `requests/get_fragment/<GS-prepid>` (the GS request is not
# reachable via the public API because the GEN-SIM is unpublished in the chained flow — obtain
# the prepid via McM SSO). cmsDriver wires the ExternalLHEProducer(gridpack) at the LHEGS step
# (see dsprod/run_step.py); it is intentionally NOT defined here.

import FWCore.ParameterSet.Config as cms

from Configuration.Generator.Pythia8CommonSettings_cfi import *
from Configuration.Generator.MCTunesRun3ECM13p6TeV.PythiaCP5Settings_cfi import *
from Configuration.Generator.PSweightsPythia.PythiaPSweightsSettings_cfi import *

generator = cms.EDFilter(
    "Pythia8HadronizerFilter",
    maxEventsToPrint=cms.untracked.int32(1),
    pythiaPylistVerbosity=cms.untracked.int32(1),
    filterEfficiency=cms.untracked.double(1.0),
    pythiaHepMCVerbosity=cms.untracked.bool(False),
    comEnergy=cms.double(13600.0),
    PythiaParameters=cms.PSet(
        pythia8CommonSettingsBlock,
        pythia8CP5SettingsBlock,
        pythia8PSweightsSettingsBlock,
        processParameters=cms.vstring(
            "TimeShower:mMaxGamma = 4.0",
        ),
        parameterSets=cms.vstring(
            "pythia8CommonSettings",
            "pythia8CP5Settings",
            "pythia8PSweightsSettings",
            "processParameters",
        ),
    ),
)

ProductionFilterSequence = cms.Sequence(generator)
