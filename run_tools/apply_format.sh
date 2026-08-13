#!/bin/bash

# Apply (or, with --dry-run, only check) the DSProd code formatting: black for Python and
# yamllint for YAML, on the files changed on this branch vs origin/main. This is the same
# check the `Formatting Check` GitHub workflow runs on a pull request, so running it before
# committing keeps CI green. Requires `black` and `yamllint` on PATH (both are in flaf_env).

if [[ $1 == "--dry-run" ]]; then
    DRY_RUN=true
    DRY_RUN_PREFIX="(dry run) "
else
    DRY_RUN=false
    DRY_RUN_PREFIX=""
fi

this_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

IFS_PREV=$IFS
IFS=$'\n'
declare -a PYTHON_FILES=()
declare -a YAML_FILES=()
for file in $(git log --name-only --pretty="" origin/main..HEAD | sort | uniq); do
    if [ ! -f "$file" ]; then
        continue
    fi
    if [[ $file == *.py ]]; then
        PYTHON_FILES+=("$file")
    elif [[ $file == *.yaml || $file == *.yml || $file == .yamllint ]]; then
        YAML_FILES+=("$file")
    fi
done

if [ ${#PYTHON_FILES[@]} -gt 0 ]; then
    echo "${DRY_RUN_PREFIX}Applying Python formatting to: ${PYTHON_FILES[@]}"
    if [ "$DRY_RUN" = true ]; then
        black --check --diff "${PYTHON_FILES[@]}"
    else
        black "${PYTHON_FILES[@]}"
    fi
fi

if [ ${#YAML_FILES[@]} -gt 0 ]; then
    echo "Checking YAML formatting for: ${YAML_FILES[@]}"
    yamllint -s -c "$this_dir/.yamllint" "${YAML_FILES[@]}"
fi

IFS=$IFS_PREV
