#!/usr/bin/env python3
"""Compare `config/conditions_Run3.yaml` with the central CMS recipes it reproduces.

DSProd exists to reproduce central MC production for samples the central campaigns do not cover, so
every cmsDriver argument it builds should match the corresponding central campaign. Nothing else
catches a mismatch: the jobs run for hours and then fail deep in the chain, or worse, succeed with
the wrong configuration. The 2023 eras once carried the Run3Summer22 recipe (`era: Run3` and
`siPixelQualityRawToDigi` in RECO) and every job died at RECO after ~5 h of GEN-SIM.

Recipes come from McM's public REST API, which needs no certificate:

    https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get/<prepid>

Exit codes: 0 = match (or McM unreachable, reported), 1 = mismatch.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
CONDITIONS = REPO / "config" / "conditions_Run3.yaml"
MCM = "https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get/{}"

#: a request id is <PWG>-<campaign>-00001; campaigns are not covered by every PWG, so try a few
PWGS = ("HIG", "TOP", "SMP", "B2G", "EXO", "SUS", "BTV", "JME", "MUO", "EGM", "BPH")

#: DSProd era -> central campaign per step. A DRPremix request holds two sequences, DIGI then RECO.
CAMPAIGNS = {
    "Run3_2022": {
        "LHEGS": ("Run3Summer22wmLHEGS", 0),
        "DIGIPremixHLT": ("Run3Summer22DRPremix", 0),
        "RECO": ("Run3Summer22DRPremix", 1),
        "MINIAOD": ("Run3Summer22MiniAODv4", 0),
        "NANO:v12": ("Run3Summer22NanoAODv12", 0),
    },
    "Run3_2022EE": {
        "LHEGS": ("Run3Summer22EEwmLHEGS", 0),
        "DIGIPremixHLT": ("Run3Summer22EEDRPremix", 0),
        "RECO": ("Run3Summer22EEDRPremix", 1),
        "MINIAOD": ("Run3Summer22EEMiniAODv4", 0),
        "NANO:v12": ("Run3Summer22EENanoAODv12", 0),
    },
    "Run3_2023": {
        "LHEGS": ("Run3Summer23wmLHEGS", 0),
        "DIGIPremixHLT": ("Run3Summer23DRPremix", 0),
        "RECO": ("Run3Summer23DRPremix", 1),
        "MINIAOD": ("Run3Summer23MiniAODv4", 0),
        "NANO:v12": ("Run3Summer23NanoAODv12", 0),
    },
    "Run3_2023BPix": {
        "LHEGS": ("Run3Summer23BPixwmLHEGS", 0),
        "DIGIPremixHLT": ("Run3Summer23BPixDRPremix", 0),
        "RECO": ("Run3Summer23BPixDRPremix", 1),
        "MINIAOD": ("Run3Summer23BPixMiniAODv4", 0),
        "NANO:v12": ("Run3Summer23BPixNanoAODv12", 0),
    },
    # 2024 campaigns are named RunIII2024Summer24*, not Run3Summer24*
    "Run3_2024": {
        "LHEGS": ("RunIII2024Summer24wmLHEGS", 0),
        "DIGIPremixHLT": ("RunIII2024Summer24DRPremix", 0),
        "RECO": ("RunIII2024Summer24DRPremix", 1),
        "MINIAOD": ("RunIII2024Summer24MiniAODv6", 0),
        "NANO:v15": ("RunIII2024Summer24NanoAODv15", 0),
    },
}

#: campaign-wide settings -- a mismatch is a bug. The GlobalTag is deliberately not here: central
#: requests within one campaign legitimately differ (v14 vs v15), so it is reported as advisory.
STRICT = ("step", "era", "procModifiers", "beamspot")
ADVISORY = ("conditions", "eventcontent", "datatier")

#: DSProd key -> McM key
KEYS = {
    "step": "step",
    "era": "era",
    "procModifiers": "procModifiers",
    "beamspot": "beamspot",
    "GlobalTag": "conditions",
    "eventcontent": "eventcontent",
    "datatier": "datatier",
}


def fetch(prepid, timeout=60):
    try:
        with urllib.request.urlopen(MCM.format(prepid), timeout=timeout) as r:
            payload = json.load(r)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        raise ConnectionError(str(exc))
    results = payload.get("results")
    return results if isinstance(results, dict) and "_id" in results else None


def resolve(campaign, cache):
    """First existing <PWG>-<campaign>-00001 request, or None if the campaign has none."""
    if campaign in cache:
        return cache[campaign]
    found = None
    for pwg in PWGS:
        found = fetch(f"{pwg}-{campaign}-00001")
        if found:
            break
    cache[campaign] = found
    return found


def norm(value):
    """McM stores step/eventcontent/datatier as lists; DSProd writes the cmsDriver string."""
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return "" if value is None else str(value)


def writes_flat_nano(eventcontent):
    """Does cmsDriver write **flat** NanoAOD for this `eventcontent`?

    cmsDriver picks the output module by substring -- `if "NANOAOD" in streamType:
    CppType='NanoAODOutputModule'` in `Configuration/Applications/python/ConfigBuilder.py`, where
    `streamType` is the eventcontent string itself. "NANOAOD" is **not** a substring of
    "NANOEDMAODSIM", so that value leaves a `PoolOutputModule` and writes the nano content as EDM
    `nanoaodFlatTable` products: no flat `Muon_pt`-style branch, and nothing FLAF or HLepRare can
    read. This is checked here rather than against McM because it is DSProd's own deliverable
    contract, and because it must still be checked when McM is unreachable.
    """
    return any("NANOAOD" in part for part in norm(eventcontent).split(","))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--era", action="append", help="check only this era (repeatable)")
    ap.add_argument("--quiet", action="store_true", help="print only mismatches")
    args = ap.parse_args()

    sys.path.insert(0, str(REPO))
    from dsprod.run_step import resolve_step_params

    conditions = yaml.safe_load(CONDITIONS.read_text())
    eras = args.era or list(CAMPAIGNS)
    cache, problems, unchecked = {}, [], []

    # the deliverable must be flat NanoAOD; checked before anything that needs McM, so an
    # unreachable McM cannot make this pass silently
    for era in eras:
        if era not in conditions or "NANO" not in (
            conditions[era].get("prod_steps") or []
        ):
            continue
        for version in list(
            (conditions[era].get("NANO") or {}).get("versions") or [None]
        ):
            params = resolve_step_params(
                conditions, era, "NANO", version=version or None
            )
            content = params.get("eventcontent")
            if not writes_flat_nano(content):
                problems.append(
                    f"{era}/NANO eventcontent={norm(content)!r} writes EDM, not flat NanoAOD: "
                    "the delivered sample would carry nanoaodFlatTable products instead of flat "
                    "branches, and FLAF could not read it"
                )

    for era in eras:
        if era not in CAMPAIGNS:
            problems.append(
                f"{era}: no central campaign mapping in {Path(__file__).name}"
            )
            continue
        if not args.quiet:
            print(f"\n=== {era} ===")
        # anything DSProd will actually run but has no central counterpart mapped is reported,
        # never silently skipped -- unverified settings are exactly how the 2023 recipe drifted
        for step in conditions[era]["prod_steps"]:
            versions = list((conditions[era].get(step) or {}).get("versions") or [None])
            for version in versions:
                key = f"{step}:{version}" if version else step
                if key not in CAMPAIGNS[era]:
                    unchecked.append(f"{era}/{key}: no central campaign mapped")
        for step_key, (campaign, seq_idx) in CAMPAIGNS[era].items():
            step, _, version = step_key.partition(":")
            try:
                request = resolve(campaign, cache)
            except ConnectionError as exc:
                print(
                    f"McM unreachable ({exc}); only the output-format check ran",
                    file=sys.stderr,
                )
                break
            if request is None:
                unchecked.append(
                    f"{era}/{step_key}: no McM request found for {campaign}"
                )
                continue
            sequences = request.get("sequences") or []
            if seq_idx >= len(sequences):
                unchecked.append(
                    f"{era}/{step_key}: {request['_id']} has no sequence {seq_idx}"
                )
                continue
            central = sequences[seq_idx]
            ours = resolve_step_params(conditions, era, step, version=version or None)

            bad = []
            for our_key, mcm_key in KEYS.items():
                if mcm_key not in STRICT and mcm_key not in ADVISORY:
                    continue
                a, b = norm(ours.get(our_key)), norm(central.get(mcm_key))
                if a == b:
                    continue
                line = (
                    f"{era}/{step_key} {our_key}: ours={a!r} central={b!r} "
                    f"({request['_id']})"
                )
                if mcm_key in STRICT:
                    bad.append(line)
                    problems.append(line)
                elif not args.quiet:
                    print(f"  note  {line}")
            if not args.quiet:
                mark = "FAIL" if bad else "ok"
                print(f"  {mark:5} {step_key:14} vs {request['_id']}")
                for line in bad:
                    print(f"          {line.split(' ', 1)[1]}")

    if unchecked:
        print("\nnot checked (no central reference):", file=sys.stderr)
        for u in unchecked:
            print(f"  {u}", file=sys.stderr)
    if problems:
        print("\nMISMATCH with the central recipe:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print("\nall checked steps match the central recipe")
    return 0


if __name__ == "__main__":
    sys.exit(main())
