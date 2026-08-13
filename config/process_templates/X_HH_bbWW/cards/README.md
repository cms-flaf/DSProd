# X->HH->bbWW gridpack cards (narrow radion)

Authoritative genproductions recipe
(`cms-sw/genproductions/bin/MadGraph5_aMCatNLO/cards/production/13p6TeV/HHresonant/Spin-0`,
template `Radion_hh_narrow_M900`).

- **model**: `heft_radion`, packaged centrally as `dibosonResonanceModel.tar.gz` (`extramodels.dat`);
  `gridpack_generation.sh` fetches it from `cms-project-generators` (verified present). Nothing to ship.
- **process**: `generate p p > h2, ( h2 > H H )` — gg → radion(h2, PDG 35) → HH.
- **mass scan**: only `mass 35` changes; width fixed narrow (1 MeV). See `customizecards.dat`.
- **run_card**: 13.6 TeV (ebeam 6800), `$DEFAULT_PDF_SETS` (resolved by gridpack_generation.sh),
  no matching (ickkw 0).

DSProd renders these per point (`__NAME__` → gridpack name, `__MASS__` → resonance mass) into
`<NAME>_proc_card.dat` / `_run_card.dat` / `_customizecards.dat` / `_extramodels.dat`, then runs
`genproductions_scripts/bin/MadGraph5_aMCatNLO/gridpack_generation.sh <NAME> <cards_dir>`.

Existing-gridpack mode (central masses on cvmfs) uses none of this.
