#!/usr/bin/env bash

# Drive a long production: one `law run`, kept alive under a lock, with a log.
#
# The largest single delay measured in the Run3_2023BPix production was supervision, not physics.
# `data/jobs/` holds 14 driver invocations over 7 days -- the driver dies roughly daily -- and one
# of those gaps, nothing polled between 09-01 16:20 and 09-03 14:12, is 27 h of the 68.4 h the
# production took to reach 99.4 %. The CRAB tasks keep running while the driver is gone; what stops is
# polling, resubmission of what failed, and every merge downstream of it.
#
# Usage:
#   source env.sh
#   run_tools/drive.sh [options] law run RunProd --setup <setup>.yaml --workflow crab
#
# See docs/operations/long-productions.md.

set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MAX_RESTARTS=${DRIVE_MAX_RESTARTS:-20}
MIN_PROXY_HOURS=${DRIVE_MIN_PROXY_HOURS:-24}
MIN_MYPROXY_DAYS=${DRIVE_MIN_MYPROXY_DAYS:-5}
STALE_LOCK_HOURS=${DRIVE_STALE_LOCK_HOURS:-1}
BACKOFF_SECONDS=${DRIVE_BACKOFF_SECONDS:-60}
BACKOFF_MAX_SECONDS=${DRIVE_BACKOFF_MAX_SECONDS:-1800}
# a leg that ran this long was not a crash loop, so the restart budget starts over
HEALTHY_RUN_SECONDS=${DRIVE_HEALTHY_RUN_SECONDS:-3600}
MYPROXY_SERVER=${DRIVE_MYPROXY_SERVER:-myproxy.cern.ch}

LOCK="$REPO/data/driver.lock"
LOG_DIR="$REPO/data/logs"
OWN_LOCK=0

usage() {
    cat << USAGE
Usage: source env.sh && run_tools/drive.sh [options] law run <task> [law args]

Runs the given law command, logs it to data/logs/driver_<UTC>.log, holds a lock so that two
drivers cannot drive the same production area, and restarts law when it dies -- but only for exit
codes where running the same command again can help (see docs/operations/long-productions.md).

Options:
  --max-restarts <n>       consecutive restarts before giving up (default: $MAX_RESTARTS)
  --min-proxy-hours <h>    refuse to start below this VOMS proxy lifetime (default: $MIN_PROXY_HOURS)
  --min-myproxy-days <d>   refuse to start below this MyProxy lifetime, CRAB only (default: $MIN_MYPROXY_DAYS)
  --stale-lock-hours <h>   age at which a lock whose pid is gone may be taken over (default: $STALE_LOCK_HOURS)
  -h, --help               this text

Every option also has an environment form (DRIVE_MAX_RESTARTS, DRIVE_MIN_PROXY_HOURS,
DRIVE_MIN_MYPROXY_DAYS, DRIVE_STALE_LOCK_HOURS, DRIVE_BACKOFF_SECONDS,
DRIVE_BACKOFF_MAX_SECONDS, DRIVE_HEALTHY_RUN_SECONDS).

The credential pre-flight only reads: it never creates, renews or removes a proxy, so a refusal
leaves exactly the credential that was there.
USAGE
}

now_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }
this_host() { hostname -f 2> /dev/null || hostname; }
say() { echo "[drive $(now_utc)] $*"; }

fmt_duration() {
    local s=$1
    # the lock records the start time of a driver on another machine, so clock skew can make an
    # age negative
    ((s < 0)) && s=0
    printf '%dh%02dm' $((s / 3600)) $(((s % 3600) / 60))
}

# ---------------------------------------------------------------------------- options

while (($# > 0)); do
    case "$1" in
        -h | --help)
            usage
            exit 0
            ;;
        --max-restarts)
            MAX_RESTARTS=${2:?"--max-restarts needs a value"}
            shift 2
            ;;
        --min-proxy-hours)
            MIN_PROXY_HOURS=${2:?"--min-proxy-hours needs a value"}
            shift 2
            ;;
        --min-myproxy-days)
            MIN_MYPROXY_DAYS=${2:?"--min-myproxy-days needs a value"}
            shift 2
            ;;
        --stale-lock-hours)
            STALE_LOCK_HOURS=${2:?"--stale-lock-hours needs a value"}
            shift 2
            ;;
        --)
            shift
            break
            ;;
        -*)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *) break ;;
    esac
done

CMD=("$@")
if ((${#CMD[@]} == 0)); then
    echo "no command given" >&2
    usage >&2
    exit 2
fi

if ! command -v law > /dev/null 2>&1; then
    echo "law is not on PATH. Source the production environment first:" >&2
    echo "  cd $REPO && source env.sh" >&2
    exit 2
fi

# A driver that locks one area while law drives another would leave both unguarded: env.sh exports
# ANALYSIS_PATH, and law resolves data/ from it, so the two must be the same checkout.
if [[ -n ${ANALYSIS_PATH:-} ]]; then
    sourced_area="$(cd "$ANALYSIS_PATH" 2> /dev/null && pwd)"
    if [[ $sourced_area != "$REPO" ]]; then
        echo "the sourced env.sh belongs to a different production area:" >&2
        echo "  ANALYSIS_PATH: ${sourced_area:-$ANALYSIS_PATH}" >&2
        echo "  this script:   $REPO" >&2
        echo "Source $REPO/env.sh, or run that area's own drive.sh." >&2
        exit 2
    fi
fi

# ---------------------------------------------------------------------------- log

mkdir -p "$LOG_DIR" || exit 1
LOG="$LOG_DIR/driver_$(date -u +%Y%m%dT%H%M%SZ).log"
echo "driver log: $LOG"
# tee so an interactive operator still sees the run; everything below this line, refusals
# included, is on the record
exec > >(tee -a "$LOG") 2>&1

# ---------------------------------------------------------------------------- lock

read_owner_field() { sed -n "s/^$1 //p" "$LOCK/owner" 2> /dev/null | head -1; }

pid_alive() {
    local pid=$1
    [[ $pid =~ ^[0-9]+$ ]] || return 1
    # /proc, not `kill -0`: kill reports EPERM for a live process owned by someone else, which
    # would read as "gone" and hand the lock to a second driver
    if [[ -d /proc ]]; then
        [[ -d /proc/$pid ]]
    else
        kill -0 "$pid" 2> /dev/null
    fi
}

write_owner() {
    {
        echo "host $(this_host)"
        echo "pid $$"
        echo "started_epoch $(date +%s)"
        echo "started $(now_utc)"
        echo "log $LOG"
        echo "command ${CMD[*]}"
    } > "$LOCK/owner"
}

# 0 = a stale lock was cleared and mkdir is worth retrying, 1 = leave it alone
inspect_lock() {
    local host pid started started_epoch age i
    for i in 1 2 3; do
        host=$(read_owner_field host)
        [[ -n $host ]] && break
        sleep 1
    done
    if [[ -z ${host:-} ]]; then
        say "REFUSING: $LOCK exists but carries no owner record."
        say "  A driver that took it a second ago has not written it yet. Look again, and only if"
        say "  nothing is driving this area: rm -rf '$LOCK'"
        return 1
    fi
    pid=$(read_owner_field pid)
    started=$(read_owner_field started)
    started_epoch=$(read_owner_field started_epoch)
    [[ $started_epoch =~ ^[0-9]+$ ]] || started_epoch=0
    age=$(($(date +%s) - started_epoch))
    say "lock found: host=$host pid=$pid started=$started age=$(fmt_duration $age)"
    say "  command: $(read_owner_field command)"

    if [[ $host != "$(this_host)" ]]; then
        # the production area is shared between machines, so the pid in the record means nothing
        # here -- it may not exist locally while it is very much alive there
        say "REFUSING: the lock belongs to $host, where pid $pid cannot be checked from $(this_host)."
        say "  Check there ('ps -p $pid'), and only if it is gone: rm -rf '$LOCK'"
        return 1
    fi
    if pid_alive "$pid"; then
        say "REFUSING: pid $pid is alive here -- a driver is already running for this area."
        return 1
    fi
    if ((age < STALE_LOCK_HOURS * 3600)); then
        say "REFUSING: pid $pid is gone, but the lock is only $(fmt_duration $age) old"
        say "  (stale after ${STALE_LOCK_HOURS} h). A lock this fresh is more likely half-written"
        say "  than abandoned. Wait, pass --stale-lock-hours, or rm -rf '$LOCK'"
        return 1
    fi
    say "the lock is stale: pid $pid is gone and it is $(fmt_duration $age) old; taking it over"
    # rename, so that two drivers racing on the same stale lock cannot both proceed
    if ! mv "$LOCK" "$LOCK.stale.$$" 2> /dev/null; then
        say "REFUSING: could not move the stale lock aside; another driver got there first."
        return 1
    fi
    rm -rf "$LOCK.stale.$$"
    return 0
}

take_lock() {
    local i
    for i in 1 2; do
        if mkdir "$LOCK" 2> /dev/null; then
            write_owner
            OWN_LOCK=1
            say "lock taken: $LOCK"
            return 0
        fi
        inspect_lock || return 1
    done
    say "REFUSING: could not take $LOCK"
    return 1
}

release_lock() {
    if ((OWN_LOCK)); then
        rm -rf "$LOCK"
        OWN_LOCK=0
    fi
}

# law runs as a child and is waited for, so that a signal sent to the driver reaches law too.
# Without the forward, `kill <driver pid>` looked like it did nothing: bash defers a trap until the
# foreground command returns, so the driver sat there for the remaining hours of the law run.
on_signal() {
    local sig=$1
    say "received SIG$sig; not restarting"
    if [[ -n ${LAW_PID:-} ]] && kill -0 "$LAW_PID" 2> /dev/null; then
        say "passing SIG$sig to law (pid $LAW_PID) and waiting for it to stop"
        kill -"$sig" "$LAW_PID" 2> /dev/null
        wait "$LAW_PID" 2> /dev/null
    fi
    release_lock
    case $sig in
        INT) exit 130 ;;
        *) exit 143 ;;
    esac
}

LAW_PID=
trap release_lock EXIT
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

# ---------------------------------------------------------------------------- credentials

# `--workflow crab`, `--workflow=crab` or the per-task form `--RunProd-workflow crab`: only a CRAB
# run needs the MyProxy delegation.
is_crab_run() {
    local i n=${#CMD[@]}
    for ((i = 0; i < n; i++)); do
        case "${CMD[i]}" in
            *workflow=crab) return 0 ;;
            *workflow)
                [[ ${CMD[$((i + 1))]:-} == crab ]] && return 0
                ;;
        esac
    done
    return 1
}

# Read-only by design. CreateVomsProxy.run() (dsprod/tools.py) removes the proxy before creating a
# new one, and `voms-proxy-init` cannot run unattended, so a restart that tried to renew would take
# away the only credential the production has and get nothing back.
preflight() {
    local proxy=${X509_USER_PROXY:-} left need identity user secs best h m s
    if [[ -z $proxy || ! -f $proxy ]]; then
        say "REFUSING: no VOMS proxy at '${proxy:-<X509_USER_PROXY unset>}'. Create one yourself:"
        say "  voms-proxy-init --voms cms -rfc -valid 192:00 --out ${proxy:-\$X509_USER_PROXY}"
        return 1
    fi
    left=$(voms-proxy-info --timeleft --file "$proxy" 2> /dev/null)
    [[ $left =~ ^[0-9]+$ ]] || left=0
    need=$((MIN_PROXY_HOURS * 3600))
    if ((left < need)); then
        say "REFUSING: the VOMS proxy has $((left / 3600))h$(((left % 3600) / 60))m left; the threshold is ${MIN_PROXY_HOURS} h."
        say "  Renew it yourself -- this script never creates or removes a credential:"
        say "  voms-proxy-init --voms cms -rfc -valid 192:00 --out $proxy"
        return 1
    fi
    say "VOMS proxy: $((left / 3600))h left ($proxy)"

    if ! is_crab_run; then
        say "MyProxy: not checked (the command asks for no crab workflow)"
        return 0
    fi
    if ! command -v myproxy-info > /dev/null 2>&1; then
        say "REFUSING: myproxy-info is not on PATH, so the delegation CRAB needs cannot be checked."
        return 1
    fi
    identity=$(voms-proxy-info --identity --file "$proxy" 2> /dev/null)
    best=0
    # law looks the delegation up under the sha1 of the identity, DSProd's CRAB gate also tries the
    # plain subject (dsprod/crab.py setup_job_manager); check both, so the two cannot disagree
    for user in "$(printf '%s' "$identity" | sha1sum | awk '{print $1}')" "$identity"; do
        [[ -n $user ]] || continue
        read -r h m s <<< "$(myproxy-info -s "$MYPROXY_SERVER" -l "$user" 2> /dev/null \
            | sed -n 's/^ *timeleft: *\([0-9]\+\):\([0-9]\+\):\([0-9]\+\).*/\1 \2 \3/p' | head -1)"
        [[ ${h:-} =~ ^[0-9]+$ ]] || continue
        secs=$((h * 3600 + m * 60 + s))
        ((secs > best)) && best=$secs
    done
    need=$((MIN_MYPROXY_DAYS * 24 * 3600))
    if ((best < need)); then
        say "REFUSING: the MyProxy delegation has $((best / 3600)) h left; the threshold is ${MIN_MYPROXY_DAYS} days."
        say "  The CRAB server renews each job's credential from it. Delegate it yourself:"
        say "  myproxy-init -d -n -s $MYPROXY_SERVER"
        say "  # verify: myproxy-info -d -s $MYPROXY_SERVER"
        return 1
    fi
    say "MyProxy delegation: $((best / 3600))h left ($MYPROXY_SERVER)"
    return 0
}

# ---------------------------------------------------------------------------- exit codes

# law leaves luigi's patched return codes in place (law/patches.py patch_default_retcodes), and
# luigi reports the *most severe* condition of the run (luigi/retcodes.py takes the max), so the
# code says what may be retried. Never configure `[luigi_retcode] task_failed: 1`: it collapses
# "some branches failed, resume" into the same 1 that a typo in the command line returns.
classify() {
    case "$1" in
        0) echo "done|the requested task is complete" ;;
        10) echo "fatal|luigi already_running: another worker holds this task" ;;
        20) echo "resume|luigi missing_data: an external input was missing (storage unreachable?)" ;;
        30) echo "resume|luigi not_run: the root task did not run to completion" ;;
        40) echo "resume|luigi task_failed: a task failed -- how a long production normally ends a leg" ;;
        50) echo "resume|luigi scheduling_error: complete()/requires() raised (a listing that failed, or a bug)" ;;
        60) echo "fatal|luigi unhandled_exception: an internal error, before any task ran" ;;
        1) echo "fatal|law aborted: unknown task family, unimportable module, or a bad command line" ;;
        130) echo "stopped|SIGINT: someone interrupted the run" ;;
        143) echo "stopped|SIGTERM: someone stopped the run" ;;
        137) echo "resume|SIGKILL: the node killed law (out of memory, or a login-node limit)" ;;
        *) echo "fatal|unclassified exit code" ;;
    esac
}

# ---------------------------------------------------------------------------- run

take_lock || exit 4

say "area:    $REPO"
say "host:    $(this_host) (pid $$)"
say "command: ${CMD[*]}"

if [[ ${CMD[0]} != "law" ]]; then
    say "WARNING: the command does not start with 'law'; the exit-code classification below"
    say "  assumes luigi's return codes and will be meaningless for anything else"
fi

attempt=0
while :; do
    preflight || {
        say "not starting law; the credential was left exactly as it was"
        exit 3
    }
    say "starting law (attempt $((attempt + 1)), up to $((MAX_RESTARTS + 1)))"
    started=$(date +%s)
    "${CMD[@]}" &
    LAW_PID=$!
    wait "$LAW_PID"
    code=$?
    LAW_PID=
    ran=$(($(date +%s) - started))
    verdict=$(classify "$code")
    say "law exited $code after $(fmt_duration $ran): ${verdict#*|}"

    case "${verdict%%|*}" in
        done)
            say "done"
            exit 0
            ;;
        stopped)
            say "not restarting"
            exit "$code"
            ;;
        fatal)
            say "not restarting: the same command would fail the same way"
            exit "$code"
            ;;
    esac

    if ((ran >= HEALTHY_RUN_SECONDS)); then
        ((attempt > 0)) && say "that leg ran $(fmt_duration $ran), so this is not a crash loop; restart budget reset"
        attempt=0
    fi
    attempt=$((attempt + 1))
    if ((attempt > MAX_RESTARTS)); then
        say "giving up after $MAX_RESTARTS consecutive restarts without a leg lasting $(fmt_duration $HEALTHY_RUN_SECONDS)"
        exit "$code"
    fi
    delay=$BACKOFF_SECONDS
    for ((i = 1; i < attempt; i++)); do
        delay=$((delay * 2))
        ((delay >= BACKOFF_MAX_SECONDS)) && {
            delay=$BACKOFF_MAX_SECONDS
            break
        }
    done
    say "restarting in ${delay}s"
    sleep "$delay"
done
