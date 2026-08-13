#!/usr/bin/env bash

# Job bootstrap.
#  - HTCondor (CERN, AFS mounted): source the workspace env.sh at {{analysis_path}}.
#  - CRAB (WLCG worker, no AFS): the DSProd code is shipped as dsprod_code.tar.gz via CRAB
#    inputFiles; unpack it and source its env.sh (which sets ANALYSIS_PATH to the unpack dir,
#    builds the venv, and installs CMSSW from cvmfs on demand).
action() {
    if [ -f "dsprod_code.tar.gz" ]; then
        mkdir -p dsprod_code && tar -xzf dsprod_code.tar.gz -C dsprod_code
        source dsprod_code/env.sh
    else
        source "{{analysis_path}}/env.sh"
    fi
}
action
