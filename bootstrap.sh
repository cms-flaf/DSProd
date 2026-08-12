#!/usr/bin/env bash

# HTCondor job bootstrap: law renders {{analysis_path}} at submission time.
action() {
    source "{{analysis_path}}/env.sh"
}
action
