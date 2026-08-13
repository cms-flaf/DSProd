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
  export X509_USER_PROXY="$ANALYSIS_DATA_PATH/voms.proxy"
  run_cmd mkdir -p "$ANALYSIS_DATA_PATH"

  # law + luigi in a venv on the SYSTEM python3.9, with --system-site-packages so law's WLCG targets
  # use the system gfal2 (+ its working http/Davix plugin for davs://). This is self-contained and
  # avoids the LCG/gfal library conflicts; built once, guarded by .installed.
  local dsprod_env="$this_dir/soft/dsprod_env"
  if [ ! -f "$dsprod_env/.installed" ]; then
    echo "Creating dsprod_env (law + luigi) ..."
    run_cmd /usr/bin/python3 -m venv --system-site-packages "$dsprod_env"
    source "$dsprod_env/bin/activate"
    run_cmd pip install --quiet --upgrade pip
    run_cmd pip install --quiet luigi==3.7.3 law
    touch "$dsprod_env/.installed"
  else
    source "$dsprod_env/bin/activate"
  fi

  if [ ! -z $ZSH_VERSION ]; then
    autoload bashcompinit
    bashcompinit
  fi
  source "$( law completion )" "" 2> /dev/null

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
