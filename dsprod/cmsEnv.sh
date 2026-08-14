#!/bin/bash
# Vendored from RunKit/cmsEnv.sh: run a command inside a CMSSW runtime.
# DEFAULT_CMSSW_BASE selects which CMSSW area to source.
CURRENT_DIR=$PWD
cd $DEFAULT_CMSSW_BASE/src
source /cvmfs/cms.cern.ch/cmsset_default.sh
eval $(scramv1 runtime -sh 2>/dev/null)
cd $CURRENT_DIR
"$@"
