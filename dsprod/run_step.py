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
    # empty means "none": an era can drop a modifier that `default_step` sets for the others
    if step_params.get("procModifiers"):
        cmd += f" --procModifiers {step_params['procModifiers']}"
    if filein is not None:
        cmd += f" --filein file:{filein}"
    if "datamix" in step_params:
        cmd += f" --datamix {step_params['datamix']}"
    if "pileup" in step_params:
        cmd += f" --pileup {step_params['pileup']}"
    if "pileup_input" in step_params:
        cmd += f" --pileup_input \"{step_params['pileup_input']}\""
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
    )
    from .tools import ps_call

    cmd = f"{_cmsenv_prefix(step_params)} {driver}"
    ps_call([cmd], shell=True, cwd=work_dir, verbose=verbose)


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


def hadd_nano(step_params, out_path, in_paths, work_dir, verbose=1):
    """Merge NanoAOD files with haddnano.py in the nano version's CMSSW env.

    haddnano.py correctly sums the Runs tree (genEventCount/genEventSumw) needed for
    normalization, unlike a plain hadd.
    """
    from .tools import ps_call

    args = " ".join([out_path] + list(in_paths))
    cmd = f"{_cmsenv_prefix(step_params)} haddnano.py {args}"
    ps_call([cmd], shell=True, cwd=work_dir, verbose=verbose)


def count_events(step_params, path, work_dir, tree="Events"):
    """Return the number of entries in `tree` of a nano file (opened in the nano CMSSW env)."""
    script = os.path.join(work_dir, "_count_events.py")
    with open(script, "w") as f:
        f.write(
            "import ROOT, sys\n"
            "f = ROOT.TFile.Open(sys.argv[1])\n"
            f'print(int(f.Get("{tree}").GetEntries()))\n'
        )
    from .tools import ps_call

    cmd = f"{_cmsenv_prefix(step_params)} python3 {script} {path}"
    _, out, _ = ps_call([cmd], shell=True, catch_stdout=True, verbose=0)
    return int(out.strip().splitlines()[-1])
