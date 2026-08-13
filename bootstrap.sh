#!/usr/bin/env bash

# Job bootstrap.
#  - HTCondor (CERN, AFS mounted): source the workspace env.sh at {{analysis_path}}.
#  - CRAB (WLCG worker, no AFS): the DSProd code is shipped as dsprod_code.tar.gz via CRAB
#    inputFiles; unpack it and source its env.sh (which sets ANALYSIS_PATH to the unpack dir,
#    builds the venv, and installs CMSSW from cvmfs on demand).
action() {
    # law postfixes shipped input files with a hash (e.g. dsprod_code_<hash>.tar.gz) and
    # symlinks them into the job home (this script's CWD), so match by glob rather than a
    # fixed name. Present => CRAB worker (no AFS): unpack + source the shipped env.sh.
    # Absent => HTCondor (AFS mounted): source the workspace env.sh.
    local tarball
    tarball="$( ls dsprod_code*.tar.gz 2>/dev/null | head -1 )"
    if [ -n "$tarball" ]; then
        mkdir -p dsprod_code && tar -xzf "$tarball" -C dsprod_code
        export DSPROD_ON_GRID=1
        source dsprod_code/env.sh
    else
        source "{{analysis_path}}/env.sh"
    fi
}
action
