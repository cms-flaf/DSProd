# X->HH->bbWW gen fragment (narrow radion), from McM
# requests/get_fragment/B2G-Run3Summer22EEwmLHEGS-00612.
#
# The gridpack produces H H (Higgs undecayed); Pythia decays H->bb / H->WW and the
# ResonanceDecayFilter selects the channel. daughters = 5,5,1,1,11,12 -> bb + (W->qq) + (W->lnu)
# = the SINGLE-lepton channel (2B2JLNu). Other channels differ only in the filter daughters
# (e.g. double-lepton 2L2Nu -> 5,5,11,12,11,12), sharing the same gridpack.
#
# DSProd's run_step overrides externalLHEProducer.args (the actual/staged gridpack), .nEvents,
# and the random seed at the LHEGS step, so this fragment is reused across mass points.

import FWCore.ParameterSet.Config as cms

# link to cards:
# https://github.com/cms-sw/genproductions/tree/master/bin/MadGraph5_aMCatNLO/cards/production/13p6TeV/HHresonant/Spin-0

externalLHEProducer = cms.EDProducer(
    "ExternalLHEProducer",
    args=cms.vstring(
        "/cvmfs/cms.cern.ch/phys_generator/gridpacks/RunIII/13p6TeV/slc7_amd64_gcc10/MadGraph5_aMCatNLO/GF_HH_Spin0/Radion_hh_narrow_M800_slc7_amd64_gcc10_CMSSW_12_4_8_tarball.tar.xz"
    ),
    nEvents=cms.untracked.uint32(5000),
    numberOfParameters=cms.uint32(1),
    outputFile=cms.string("cmsgrid_final.lhe"),
    generateConcurrently=cms.untracked.bool(False),
    scriptName=cms.FileInPath(
        "GeneratorInterface/LHEInterface/data/run_generic_tarball_cvmfs.sh"
    ),
)


from Configuration.Generator.Pythia8CommonSettings_cfi import *
from Configuration.Generator.MCTunesRun3ECM13p6TeV.PythiaCP5Settings_cfi import *
from Configuration.Generator.PSweightsPythia.PythiaPSweightsSettings_cfi import *

generator = cms.EDFilter(
    "Pythia8ConcurrentHadronizerFilter",
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
            "24:mMin = 0.05",
            "24:onMode = on",
            "25:m0 = 125.0",
            "25:onMode = off",
            "25:onIfMatch = 5 -5",
            "25:onIfMatch = 24 -24",
            "ResonanceDecayFilter:filter = on",
            "ResonanceDecayFilter:exclusive = on",  # on: require exactly the specified daughters
            "ResonanceDecayFilter:eMuTauAsEquivalent = on",  # e, mu, tau equivalent
            "ResonanceDecayFilter:allNuAsEquivalent = on",  # all neutrino flavours equivalent
            "ResonanceDecayFilter:udscAsEquivalent = on",  # udsc quarks equivalent
            "ResonanceDecayFilter:mothers = 24,25",
            "ResonanceDecayFilter:daughters = 5,5,1,1,11,12",
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
