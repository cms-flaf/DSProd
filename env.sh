#!/usr/bin/env bash

# DSProd environment.
# Sourcing this sets up law + a python/ROOT stack. Per-era CMSSW releases are installed
# on demand by the production tasks (Phase 3) via `install_cmssw`, so sourcing env.sh does
# NOT trigger a CMSSW build.

function run_cmd {
    "$@"
    RESULT=$?
    if (( $RESULT != 0 )); then
        echo "Error while running '$@'"
        kill -INT $$
    fi
}

get_os_prefix() {
  local os_version=$1
  local for_global_tag=$2
  if (( $os_version >= 8 )); then
    echo el
  elif (( $os_version < 6 )); then
    echo error
  else
    if [[ $for_global_tag == 1 || $os_version == 6 ]]; then
      echo slc
    else
      echo cc
    fi
  fi
}

# Install a CMSSW release into soft/<CMSSW_VER> (idempotent; .installed flag guards it).
# $1=SCRAM_ARCH $2=CMSSW_VER $3=target_os_version $4=inst_type (unused hook for per-tier setup)
do_install_cmssw() {
  local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
  local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"

  export SCRAM_ARCH=$1
  local CMSSW_VER=$2
  if ! [ -f "$this_dir/soft/$CMSSW_VER/.installed" ]; then
    run_cmd mkdir -p "$this_dir/soft"
    run_cmd cd "$this_dir/soft"
    run_cmd source /cvmfs/cms.cern.ch/cmsset_default.sh
    if [ -d $CMSSW_VER ]; then
      echo "Removing incomplete $CMSSW_VER installation..."
      run_cmd rm -rf $CMSSW_VER
    fi
    echo "Creating $CMSSW_VER area in $PWD ..."
    run_cmd scramv1 project CMSSW $CMSSW_VER
    run_cmd cd $CMSSW_VER/src
    run_cmd eval `scramv1 runtime -sh`
    run_cmd mkdir -p "Configuration/GenProduction/python"
    run_cmd mkdir -p "$this_dir/soft/$CMSSW_VER/bin_ext"
    run_cmd ln -s $(which python3) "$this_dir/soft/$CMSSW_VER/bin_ext/python"
    run_cmd scram b -j8
    run_cmd cd "$this_dir"
    run_cmd touch "$this_dir/soft/$CMSSW_VER/.installed"
  fi
}

# Cross-OS aware wrapper around do_install_cmssw (uses cmssw-<os> when node OS != target OS).
install_cmssw() {
  local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
  local scram_arch=$1
  local cmssw_version=$2
  local node_os=$3
  local target_os=$4
  local inst_type=$5
  if [[ $node_os == $target_os ]]; then
    local env_cmd=""
    local env_cmd_args=""
  else
    local env_cmd="cmssw-$target_os"
    if ! command -v $env_cmd &> /dev/null; then
      echo "Unable to do a cross-platform installation for $cmssw_version SCRAM_ARCH=$scram_arch. $env_cmd is not available."
      return 1
    fi
    local env_cmd_args="--command-to-run"
  fi
  run_cmd $env_cmd $env_cmd_args /usr/bin/env -i HOME=$HOME bash "$this_file" install_cmssw $scram_arch $cmssw_version $target_os $inst_type
}

action() {
  local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
  local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"

  export PYTHONPATH="$this_dir:$PYTHONPATH"
  export LAW_HOME="$this_dir/.law"
  export LAW_CONFIG_FILE="$this_dir/config/law.cfg"

  export ANALYSIS_PATH="$this_dir"
  export ANALYSIS_DATA_PATH="$ANALYSIS_PATH/data"
  # Keep a proxy already provided by the batch system (CRAB sets X509_USER_PROXY on the worker);
  # only fall back to the workspace proxy on lxplus.
  export X509_USER_PROXY="${X509_USER_PROXY:-$ANALYSIS_DATA_PATH/voms.proxy}"
  run_cmd mkdir -p "$ANALYSIS_DATA_PATH"

  if [ -n "$DSPROD_ON_GRID" ]; then
    # Grid worker (CRAB): no AFS, no PyPI, and the system python3 is too old for luigi (needs
    # >=3.8). Use a cvmfs python3.9 from the CRAB-provided CMSSW plus the vendored (pure-python)
    # law + luigi shipped in the code tarball (soft/vendor). No venv, no pip, no network.
    run_cmd source /cvmfs/cms.cern.ch/cmsset_default.sh
    local grid_cmssw="${CMSSW_BASE:-}"
    if [ -z "$grid_cmssw" ]; then
      grid_cmssw="$( ls -d "${LAW_JOB_INIT_DIR:-/srv}"/CMSSW_*/ /srv/CMSSW_*/ 2>/dev/null | sort | tail -1 )"
    fi
    if [ -n "$grid_cmssw" ] && [ -d "${grid_cmssw%/}/src" ]; then
      pushd "${grid_cmssw%/}/src" >/dev/null
      eval `scramv1 runtime -sh`
      popd >/dev/null
    fi
    export PYTHONPATH="$this_dir/soft/vendor:$PYTHONPATH"
    echo "grid env: python3=$(command -v python3) ($(python3 --version 2>&1)), vendored law/luigi on PYTHONPATH"
  else
    # lxplus: law + luigi in a venv on the SYSTEM python3.9, with --system-site-packages so law's
    # WLCG targets use the system gfal2 (+ its working http/Davix plugin for davs://). Self-contained,
    # avoids the LCG/gfal library conflicts; built once, guarded by .installed.
    local dsprod_env="$this_dir/soft/dsprod_env"
    if [ ! -f "$dsprod_env/.installed" ]; then
      echo "Creating dsprod_env (law + luigi) ..."
      run_cmd /usr/bin/python3 -m venv --system-site-packages "$dsprod_env"
      source "$dsprod_env/bin/activate"
      run_cmd pip install --quiet --upgrade pip
      # --ignore-installed, because --system-site-packages otherwise hides the problem: pip
      # resolves luigi's own dependencies (typing_extensions, tenacity, ...) against lxplus's
      # copies and leaves them out of the venv. An HTCondor worker image does not ship them, so
      # law cannot be imported in the job and law_job.sh stops with the unhelpful "law not found
      # ... should be made available in bootstrap file". The system packages are still there for
      # what they are wanted for -- gfal2 is not a dependency of law or luigi.
      run_cmd pip install --quiet --ignore-installed luigi==3.7.3 law
      touch "$dsprod_env/.installed"
    else
      source "$dsprod_env/bin/activate"
    fi
    # Vendor pure-python law + luigi (+ deps) so grid-worker jobs can ship them in the code
    # tarball (workers have no PyPI and a too-old system python). Built once from the venv's pip;
    # the compiled tornado speedup is dropped so it stays arch-independent (pure-python fallback).
    if [ ! -d "$this_dir/soft/vendor/law" ]; then
      echo "Vendoring law + luigi into soft/vendor (for grid jobs) ..."
      run_cmd pip install --quiet --target "$this_dir/soft/vendor" luigi==3.7.3 law==0.1.20
      find "$this_dir/soft/vendor" -name "*.so" -delete 2> /dev/null
    fi
  fi

  if [ ! -z $ZSH_VERSION ]; then
    autoload bashcompinit
    bashcompinit
  fi
  source "$( law completion )" "" 2> /dev/null

  # CRAB submission (--workflow crab): the crab CLI needs a cmsenv, which would clobber the
  # law venv. Provide a `crab` wrapper on PATH that cmsenv's a DSProd CMSSW internally, so the
  # law parent keeps the venv and only the crab subprocess enters CMSSW.
  #
  # Both shims are written unconditionally. They used to be gated on a `soft/CMSSW_*` release
  # already existing, but on a fresh production area the releases are installed by InstallCMSSW
  # *during* the very run that then submits, so the gate was false and the whole law process ran
  # without them -- every CRAB submission of that run failed (see the `python` shim below).
  # Neither shim needs a release to exist: the wrapper looks one up when it is called.
  mkdir -p "$ANALYSIS_PATH/soft/bin"
  cat > "$ANALYSIS_PATH/soft/bin/crab" <<'CRABWRAP'
#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
# CRAB rewrites its task cache ~/.crab3 (via ~/.crab3.<pid>) on *every* command, status queries
# included -- so with $HOME on AFS a long production dies the moment the AFS token lapses:
#   PermissionError: [Errno 13] Permission denied: '/afs/cern.ch/user/x/xyz/.crab3.<pid>'
# and law reports it as a status-query failure for every job at once. DSProd keeps everything else
# off AFS, so give CRAB a home of its own too. law passes --proxy to submit/status/kill, so it
# never needs ~/.globus from the real home.
export HOME="${DSPROD_CRAB_HOME:-${TMPDIR:-/tmp}/dsprod_crab_home_$(id -u)}"
mkdir -p "$HOME" || exit 1
_c=$(ls -d "$ANALYSIS_PATH"/soft/CMSSW_*/ 2>/dev/null | sort | tail -1)
[ -n "$_c" ] && { cd "$_c/src" && eval $(scramv1 runtime -sh 2>/dev/null); cd - >/dev/null; }
# crab drops a crab.log wherever it is run from, and law calls status/kill without setting a
# directory, so they inherited the caller's cwd -- the production area. Run those from crab's own
# home. `submit` must keep its directory: law runs it with cwd set to the job-file directory and
# the generated config names `scriptExe` and `inputFiles` relative to it, which CRAB resolves
# against the cwd ("Cannot find the file crab_wrapper_*.sh specified in the JobType.scriptExe
# configuration parameter"). Its log then stays next to the job files, under data/jobs/.
case "$1" in
  submit) ;;
  *) cd "$HOME" || exit 1 ;;
esac
exec /cvmfs/cms.cern.ch/common/crab "$@"
CRABWRAP
  chmod +x "$ANALYSIS_PATH/soft/bin/crab"
  # law.contrib.cms's CMSSW sandbox runs bare `python` to dump its environment, but modern CMSSW
  # ships only `python3`, and the venv's `python` breaks under a cmsenv (ModuleNotFoundError:
  # _struct) -- which makes law fail every CRAB submission with "unknown job id". Provide a
  # `python` -> python3 shim so it resolves to whichever python3 is active (CMSSW's inside the
  # sandbox, the venv's outside).
  cat > "$ANALYSIS_PATH/soft/bin/python" <<'PYSHIM'
#!/bin/bash
exec python3 "$@"
PYSHIM
  chmod +x "$ANALYSIS_PATH/soft/bin/python"
  export PATH="$ANALYSIS_PATH/soft/bin:$PATH"

  # CMSSW_BASE only means something once a release is installed (env.sh:104, dsprod/tasks.py).
  local dsprod_cmssw=$(ls -d "$ANALYSIS_PATH"/soft/CMSSW_*/ 2>/dev/null | sort | tail -1)
  if [ -n "$dsprod_cmssw" ]; then
    export CMSSW_BASE="${dsprod_cmssw%/}"
  fi

  # law resolves a task name through the index it cached in .law/index, so a task added since
  # then is invisible and `law run` says only "task family '<name>' not found in index" -- a
  # confusing way to learn that the index is a cache. Refresh it whenever the task modules are
  # newer, which costs nothing on a normal source.
  if [ -f "$ANALYSIS_PATH/config/law.cfg" ]; then
    local law_index="$LAW_HOME/index"
    if [ ! -f "$law_index" ] || [ -n "$(find "$ANALYSIS_PATH/dsprod" -name '*.py' -newer "$law_index" -print -quit 2>/dev/null)" ]; then
      echo "Refreshing the law task index ..."
      law index --quiet 2>/dev/null || echo "WARNING: could not refresh the law task index"
    fi
  fi

  # Convenience: run a command inside DEFAULT_CMSSW_BASE (set per-step by the tasks in Phase 3).
  alias cmsEnv="env -i HOME=$HOME ANALYSIS_PATH=$ANALYSIS_PATH X509_USER_PROXY=$X509_USER_PROXY DEFAULT_CMSSW_BASE=\$DEFAULT_CMSSW_BASE KRB5CCNAME=$KRB5CCNAME $ANALYSIS_PATH/dsprod/cmsEnv.sh"

  echo "env.sh done!"
}

if [ "X$1" = "Xinstall_cmssw" ]; then
  do_install_cmssw "${@:2}"
elif [ "X$1" = "Xinstall" ]; then
  # install <scram_arch> <cmssw_version> [inst_type]  — cross-OS aware (used by the InstallCMSSW task)
  export PATH="/cvmfs/cms.cern.ch/common:$PATH"  # for cmssw-<os>
  os_version=$(cat /etc/os-release | grep VERSION_ID | sed -E 's/VERSION_ID="([0-9]+).*"/\1/')
  node_os=$(get_os_prefix $os_version)$os_version
  target_os=$(echo $2 | cut -d_ -f1)
  install_cmssw $2 $3 $node_os $target_os ${4:-gen}
else
  action "$@"
fi
