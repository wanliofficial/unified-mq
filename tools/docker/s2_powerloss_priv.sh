#!/bin/sh
# Run the S2-B dm-flakey power-loss driver with the privileges it needs.
#
# The driver has to create a loop device, build a dm-flakey table on it, mkfs an
# ext4 filesystem and mount it.  All four need root, and the mount must land in
# the HOST's mount namespace -- the broker container is started by the Docker
# daemon, which resolves its bind-mount source in the host's namespace.  A mount
# made inside an ordinary container's namespace would be invisible to the daemon
# and the broker would write to the wrong directory, so the test would pass for
# the wrong reason.
#
# Three ways to get there, in order of preference:
#
#   1. real root:  sudo python3 tools/docker/s2_powerloss.py --scenario powerloss
#   2. this script: the driver runs on the host, and only the privileged
#      commands (losetup/dmsetup/mkfs.ext4/mount/sync) are routed through
#      tools/docker/host_priv.sh, which executes them in the host's mount
#      namespace via a privileged container.  The rest of the driver (the
#      release, the probes, the broker container) runs untouched.
#   3. no privileges at all:  python3 tools/docker/s2_powerloss.py --skip-prereq
#      prints the plan without touching the machine.
#
# Option 2 exists because a CI host's operator account often has no sudo
# password but does have Docker; without it, the one Gate item that needs a
# real device would stay permanently unverifiable.
#
# Usage:
#   sh tools/docker/s2_powerloss_priv.sh [--scenario powerloss] [--fsync N] ...

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
RELEASE="$ROOT/_build/t14/rel/unified_mq"
PYTHON=${S2B_PYTHON:-/usr/bin/python3}

if [ ! -x "$RELEASE/bin/unified_mq" ]; then
    echo "release missing: $RELEASE (run: rebar3 as t14 release)" >&2
    exit 1
fi

if ! "$PYTHON" -c "import paho.mqtt.client" 2>/dev/null; then
    echo "$PYTHON cannot import paho-mqtt; the harness needs it" >&2
    exit 1
fi

# dm-flakey is not loaded by default on every kernel; without it the
# `dmsetup create` fails with "unknown target".  Loading it needs the host's
# kmod, so it goes through the same privileged path.
sh "$ROOT/tools/docker/host_priv.sh" sh -c \
    'dmsetup targets 2>/dev/null | grep -q "^flakey" || modprobe dm-flakey'

# The privileged commands go through host_priv.sh, which puts them in the
# host's mount namespace.  The loop image stays out of /tmp on purpose: this
# host's /tmp is tmpfs (design J11), and a power-loss test on a RAM disk would
# test nothing.
export S2B_HOST_EXEC="sh $ROOT/tools/docker/host_priv.sh"
export S2B_DM_IMAGE=${S2B_DM_IMAGE:-$ROOT/tools/docker/powerloss.img}
export S2B_DM_MOUNT=${S2B_DM_MOUNT:-$ROOT/tools/docker/powerloss-mount}
export S2B_DM_NAME=${S2B_DM_NAME:-s2b-powerloss}

cd "$ROOT"
exec "$PYTHON" tools/docker/s2_powerloss.py "$@"
