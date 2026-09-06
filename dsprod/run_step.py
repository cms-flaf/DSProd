"""Generic cmsDriver runner for the fused GEN->NANO production.

Generalizes HNLProd's run_prod.py:
  * conditions are resolved per (era, step[, nano version]) by layered merge;
  * each step runs cmsDriver.py inside its own CMSSW release via the vendored cmsEnv.sh
    (wrapped in the cmssw-el8 container when the worker OS differs), so no extra RunKit
    dependency (envToJson) is needed;
  * the final NANO step fans out over the requested NanoAOD versions.

RunProd (dsprod/tasks.py) drives this: run the GEN..MINIAOD chain once in a work dir, then
one NANO per version off the shared MiniAOD.
"""

import json
import os

# `ps_call` is imported where it is used, not here: the conditions resolution and the cmsDriver
# builder in this module are pure config logic, and run_tools/check_conditions.py exercises them
# without a law installation.

#: last-step name -> output file stem (for the staged per-seed file name)
STEP_TO_STEM = {
    "LHEGS": "sim",
    "DIGIPremixHLT": "rawHLT",
    "RECO": "reco",
    "MINIAOD": "miniAOD",
    "NANO": "nano",
}

#: cvmfs cmssw-<os> containers used to run a release on a different worker OS
SINGULARITY = {
    "el8": "/cvmfs/cms.cern.ch/common/cmssw-el8",
    "el9": "/cvmfs/cms.cern.ch/common/cmssw-el9",
}


def resolve_step_params(conditions, era, step, version=None):
    """Layered merge: default + default_step[step] + era.default + era[step] (+ NANO version)."""
    params = {}
    params.update(conditions.get("default", {}))
    params.update(conditions.get("default_step", {}).get(step, {}))
    era_cond = conditions[era]
    params.update(era_cond.get("default", {}))
    step_cond = dict(era_cond.get(step, {}))
    versions = step_cond.pop("versions", None)
    params.update(step_cond)
    if version is not None:
        if not versions or version not in versions:
            raise RuntimeError(
                f"NanoAOD version {version!r} not defined for {era}/{step}"
            )
        params.update(versions[version])
    return params


def cmssw_dir(step_params):
    return os.path.join(os.environ["ANALYSIS_PATH"], "soft", step_params["CMSSW"])


def _worker_os():
    with open("/etc/os-release") as f:
        for line in f:
            if line.startswith("VERSION_ID="):
                major = line.split("=")[1].strip().strip('"').split(".")[0]
                return f"el{major}"
    raise RuntimeError("cannot determine worker OS from /etc/os-release")


def _cmsenv_prefix(step_params):
    """Command prefix that enters the step's CMSSW runtime (container if OS differs)."""
    ana = os.environ["ANALYSIS_PATH"]
    target_os = step_params.get("OS_VERSION", "el8")
    env_vars = {
        "DEFAULT_CMSSW_BASE": cmssw_dir(step_params),
        "X509_USER_PROXY": os.environ.get("X509_USER_PROXY", ""),
        "HOME": os.environ.get("HOME", ""),
    }
    if "KRB5CCNAME" in os.environ:
        env_vars["KRB5CCNAME"] = os.environ["KRB5CCNAME"]
    env_str = " ".join(f'{k}="{v}"' for k, v in env_vars.items() if v)
    inner = f"env {env_str} bash {ana}/dsprod/cmsEnv.sh"
    if target_os != _worker_os():
        return f"{SINGULARITY[target_os]} --command-to-run {inner}"
    return inner


def build_cmsdriver(
    step,
    step_params,
    seed,
    n_evt,
    filein=None,
    fileout=None,
    gridpack=None,
    fragment_rel=None,
    n_threads=1,
    pileup_filelist=None,
):
    """Assemble the cmsDriver.py command line for one step."""
    out = fileout or f"{step}.root"
    customise_commands = ["process.MessageLogger.cerr.FwkReport.reportEvery = 100"]

    cmd = "cmsDriver.py"
    if step == "LHEGS":
        cmd += f" {fragment_rel}"
        customise_commands += [
            f"process.RandomNumberGeneratorService.externalLHEProducer.initialSeed=int({seed})",
            f'process.externalLHEProducer.args = cms.vstring("{gridpack}")',
            f"process.externalLHEProducer.nEvents = cms.untracked.uint32({n_evt})",
            f"process.source.firstRun = cms.untracked.uint32({seed})",
            f'process.generator.comEnergy = cms.double({step_params["comEnergy"]})',
        ]
        cmd += f' --beamspot {step_params["beamspot"]}'

    cmd += f" --python_filename {step}.py --eventcontent {step_params['eventcontent']}"
    cmd += f" --datatier {step_params['datatier']} --fileout file:{out}"
    cmd += f" --conditions {step_params['GlobalTag']} --step {step_params['step']}"
    cmd += f" --geometry {step_params['geometry']} --era {step_params['era']}"
    # the job's core allocation, unless the conditions pin a per-step value; cmsDriver
    # derives numberOfStreams from it. A single-threaded cmsRun in a multi-core slot wastes
    # the extra cores and does not get any faster.
    cmd += f" --mc -n {n_evt} --nThreads {int(step_params.get('nThreads', n_threads))}"
    pileup = step_params.get("pileup_input")
    if pileup and pileup_filelist and str(pileup).startswith(("dbs:", "das:")):
        # resolved once by PremixFileList instead of by a DAS query in every job
        pileup = f"filelist:{pileup_filelist}"
    # empty means "none": an era can drop a modifier that `default_step` sets for the others
    if step_params.get("procModifiers"):
        cmd += f" --procModifiers {step_params['procModifiers']}"
    if filein is not None:
        cmd += f" --filein file:{filein}"
    if "datamix" in step_params:
        cmd += f" --datamix {step_params['datamix']}"
    if "pileup" in step_params:
        cmd += f" --pileup {step_params['pileup']}"
    if pileup:
        cmd += f' --pileup_input "{pileup}"' ""
    cmd += "".join(f" --customise {x}" for x in step_params.get("customise", []))
    customise_commands += step_params.get("customise_commands", [])
    # match central compression on the persisted (final) tier
    out_module = step_params["eventcontent"].split(",")[0] + "output"
    customise_commands += [
        f'process.{out_module}.compressionAlgorithm = cms.untracked.string("LZMA")',
        f"process.{out_module}.compressionLevel = cms.untracked.int32(9)",
    ]
    cmd += " --customise_commands '" + "\\n".join(customise_commands) + "'"
    return cmd


def _link_fragment(step_params, fragment_path):
    """Symlink the gen fragment into the release's Configuration/GenProduction/python."""
    src_dir = os.path.join(
        cmssw_dir(step_params), "src", "Configuration", "GenProduction", "python"
    )
    os.makedirs(src_dir, exist_ok=True)
    name = os.path.basename(fragment_path)
    link = os.path.join(src_dir, name)
    if not os.path.exists(link):
        os.symlink(os.path.abspath(fragment_path), link)
    return os.path.join("Configuration", "GenProduction", "python", name)


def run_step(
    step,
    step_params,
    work_dir,
    seed,
    n_evt,
    filein=None,
    fileout=None,
    gridpack=None,
    fragment_path=None,
    n_threads=1,
    pileup_filelist=None,
    verbose=1,
):
    """Run one cmsDriver step in its CMSSW env, in work_dir."""
    fragment_rel = (
        _link_fragment(step_params, fragment_path) if step == "LHEGS" else None
    )
    driver = build_cmsdriver(
        step,
        step_params,
        seed,
        n_evt,
        filein=filein,
        fileout=fileout,
        gridpack=os.path.abspath(gridpack) if gridpack else None,
        fragment_rel=fragment_rel,
        n_threads=n_threads,
        pileup_filelist=pileup_filelist,
    )
    from .tools import ps_call

    cmd = f"{_cmsenv_prefix(step_params)} {driver}"
    ps_call([cmd], shell=True, cwd=work_dir, verbose=verbose)
    if fileout and n_evt:
        assert_step_events(step, step_params, work_dir, fileout, n_evt)


def assert_step_events(step, step_params, work_dir, fileout, n_evt):
    """Refuse a step whose output does not hold exactly the events it was asked for.

    `-n <n_evt>` is a request, and a step that returns fewer events fails silently: the file is
    valid, every later step copies the shortfall forward, and the only check downstream is that a
    merged file holds the sum of its own inputs -- which is just as true of short ones. One
    Run3_2023 job returned 999 of its 1000 events, so a delivered merged file holds 49 999 where
    the sample advertises 50 000, and nothing anywhere noticed.

    Checked per step rather than once at the end, because that is what says *where* the events
    went. Entering the release to count costs seconds against a step measured in hours.

    A generator that filters events would legitimately return fewer, and no DSProd process does
    today: such a fragment forces the decay instead. Should one be added, the number a job is
    asked for and the number it delivers stop being the same quantity, and this check -- not the
    fragment -- is what has to learn the difference.
    """
    produced = count_events(step_params, [os.path.join(work_dir, fileout)], work_dir)[0]
    if produced == n_evt:
        return
    raise RuntimeError(
        f"the {step} step was asked for {n_evt} events and its output {fileout} holds "
        f"{produced}. A short step is carried forward by every step after it and ends up in a "
        f"merged file that advertises a size it does not have, so the job stops here."
    )


def run_chain(
    conditions,
    era,
    first_step,
    last_step,
    seed,
    n_evt,
    work_dir,
    gridpack=None,
    fragment_path=None,
    previous_file=None,
    n_threads=1,
    pileup_filelist=None,
    verbose=1,
):
    """Run the linear prod_steps[first_step..last_step] in work_dir, chaining outputs.

    Returns the path (in work_dir) of the last produced step file.
    """
    prod_steps = conditions[era]["prod_steps"]
    i0 = prod_steps.index(first_step) if first_step else 0
    i1 = prod_steps.index(last_step) if last_step else len(prod_steps) - 1
    os.makedirs(work_dir, exist_ok=True)
    prev = previous_file
    last_out = None
    for i in range(i0, i1 + 1):
        step = prod_steps[i]
        out = f"{step}.root"
        params = resolve_step_params(conditions, era, step)
        filein = prev if i > i0 else previous_file
        run_step(
            step,
            params,
            work_dir,
            seed,
            n_evt,
            filein=filein,
            fileout=out,
            gridpack=gridpack,
            fragment_path=fragment_path,
            n_threads=n_threads,
            pileup_filelist=pileup_filelist,
            verbose=verbose,
        )
        prev = out
        last_out = os.path.join(work_dir, out)
    return last_out


def run_nano(
    conditions,
    era,
    version,
    seed,
    n_evt,
    work_dir,
    miniaod_file,
    n_threads=1,
    verbose=1,
):
    """Run the NANO step for one version off a shared MiniAOD; returns the output path."""
    params = resolve_step_params(conditions, era, "NANO", version=version)
    out = f"NANO_{version}.root"
    run_step(
        "NANO",
        params,
        work_dir,
        seed,
        n_evt,
        filein=os.path.basename(miniaod_file),
        fileout=out,
        n_threads=n_threads,
        verbose=verbose,
    )
    return os.path.join(work_dir, out)


def merge_params(step_params):
    """Params of the release that merges this NanoAOD version.

    `haddnano.py` ships in the release binaries only from CMSSW 15X on; the 13X releases that make
    NanoAOD v12 do not have it at all, so such a version names one that does via `merge_CMSSW`
    (`merge_SCRAM_ARCH` if its architecture differs too). Everything else is unchanged, and a
    version whose own release provides the script needs no override.
    """
    if not step_params.get("merge_CMSSW"):
        return step_params
    params = dict(step_params)
    params["CMSSW"] = step_params["merge_CMSSW"]
    if step_params.get("merge_SCRAM_ARCH"):
        params["SCRAM_ARCH"] = step_params["merge_SCRAM_ARCH"]
    return params


def hadd_nano(step_params, out_path, in_paths, work_dir, verbose=1):
    """Merge NanoAOD files with haddnano.py in the nano version's CMSSW env.

    haddnano.py correctly sums the Runs tree (genEventCount/genEventSumw) needed for
    normalization, unlike a plain hadd.
    """
    from .tools import PsCallError, ps_call

    args = " ".join([out_path] + list(in_paths))
    cmd = f"{_cmsenv_prefix(step_params)} haddnano.py {args}"
    try:
        ps_call([cmd], shell=True, cwd=work_dir, verbose=verbose)
    except PsCallError as exc:
        if exc.return_code == 127:  # command not found
            raise RuntimeError(
                f"haddnano.py is not available in {step_params['CMSSW']}: it ships in the release "
                "binaries only from CMSSW 15X on. Point this NanoAOD version at a release that "
                "has it with `merge_CMSSW` (and `merge_SCRAM_ARCH`) in the conditions."
            ) from exc
        raise


#: counts the entries of every requested file in one python process. Reports through a JSON file
#: rather than on stdout, and keyed by path rather than in order: ROOT prints its own warnings to
#: stdout, so any positional read of the output is a read of whatever ROOT said last.
_COUNT_EVENTS_SCRIPT = """import json
import sys

import ROOT

with open(sys.argv[1]) as in_file:
    request = json.load(in_file)
counts = {}
for path in request["paths"]:
    root_file = ROOT.TFile.Open(path)
    if not root_file or root_file.IsZombie():
        raise RuntimeError("cannot open %s" % path)
    tree = root_file.Get(request["tree"])
    if not tree:
        raise RuntimeError("%s holds no %s tree" % (path, request["tree"]))
    counts[path] = int(tree.GetEntries())
    root_file.Close()
with open(sys.argv[2], "w") as out_file:
    json.dump(counts, out_file)
"""


def count_events(step_params, paths, work_dir, tree="Events"):
    """Entries of `tree` in each of `paths`, in that order (counted in the nano CMSSW env).

    Every invocation enters that env -- a `scram runtime` in the release, inside a container when
    the worker OS differs -- which costs far more than the counting itself: a merge group counting
    its 50 inputs and its output one file at a time spent about 10 minutes of its 3 h slot on
    nothing else. So the whole list goes in one invocation.

    The file list and the counts are passed as JSON files in `work_dir`, which keeps a group's 50
    paths off the command line and the result off stdout (see `_COUNT_EVENTS_SCRIPT`).
    """
    if isinstance(paths, str):
        # a single path would be iterated character by character, and the entry count of every
        # character of it asked for
        raise TypeError("count_events takes a sequence of paths, not one path")
    paths = [str(p) for p in paths]
    script = os.path.join(work_dir, "_count_events.py")
    request_path = os.path.join(work_dir, "_count_events_in.json")
    counts_path = os.path.join(work_dir, "_count_events_out.json")
    with open(script, "w") as f:
        f.write(_COUNT_EVENTS_SCRIPT)
    with open(request_path, "w") as f:
        json.dump({"tree": tree, "paths": paths}, f)
    # a result left by an earlier call must not be read as this one's
    if os.path.exists(counts_path):
        os.remove(counts_path)
    from .tools import ps_call

    cmd = f"{_cmsenv_prefix(step_params)} python3 {script} {request_path} {counts_path}"
    _, out, _ = ps_call([cmd], shell=True, catch_stdout=True, verbose=0)
    return _read_event_counts(counts_path, paths, out)


def _read_event_counts(counts_path, paths, stdout=None):
    """The counts written by `_COUNT_EVENTS_SCRIPT`, in the order of `paths`.

    A file that is not in the result is an error rather than a zero: an unwritten count read as 0
    would turn a counting failure into a merge that reports having lost every event.
    """
    try:
        with open(counts_path) as f:
            counts = json.load(f)
    except (OSError, ValueError) as exc:
        tail = "\n      ".join((stdout or "").strip().split("\n")[-10:])
        raise RuntimeError(
            f"counting the entries of {len(paths)} file(s) produced no readable result "
            f"({type(exc).__name__}: {exc}). Last of what it printed:\n      "
            f"{tail or '<no output>'}"
        ) from exc
    missing = [p for p in paths if p not in counts]
    if missing:
        raise RuntimeError(
            f"the entry count of {len(missing)} of {len(paths)} file(s) is not in the result "
            f"(first: {missing[0]})"
        )
    return [int(counts[p]) for p in paths]
