# X->HH->bbWW gridpack cards (narrow radion)

Recipe extracted from the central gridpack
`Radion_hh_narrow_M800_slc7_amd64_gcc10_CMSSW_12_4_8_tarball.tar.xz`
(`/cvmfs/cms.cern.ch/phys_generator/gridpacks/RunIII/13p6TeV/slc7_amd64_gcc10/MadGraph5_aMCatNLO/GF_HH_Spin0/`).

- **model**: `heft_radion` (HEFT + radion); shipped here as `heft_radion.tar.gz` (36 KB) because it
  is not on the central `cms-project-generators` area.
- **process**: `generate p p > h2, ( h2 > H H )` — gg → radion(h2, PDG 35) → HH.
- **mass scan**: only `mass 35` changes; the width is fixed narrow (1 MeV). See `customizecards.dat`.
- **run_card**: 13.6 TeV (ebeam 6800), NNPDF31 (lhaid 325500), no matching (ickkw 0).

DSProd renders these per point (`__NAME__` → the gridpack name, `__MASS__` → the resonance mass)
into `<NAME>_proc_card.dat` / `_run_card.dat` / `_customizecards.dat` / `_extramodels.dat`, then runs
`genproductions_scripts/bin/MadGraph5_aMCatNLO/gridpack_generation.sh <NAME> <cards_dir>`.

**Deployment note:** `gridpack_generation.sh` fetches extramodels via `wget` from
`cms-project-generators.web.cern.ch/cms-project-generators/<model>` — `heft_radion.tar.gz` must be
uploaded there for a generate-mode run to succeed (it is provided here for that purpose and for
reproducibility). Existing-gridpack mode needs none of this.
