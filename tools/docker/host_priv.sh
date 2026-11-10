#!/bin/sh
# Run ONE privileged host command in the host's mount namespace.
#
# Used by tools/docker/s2_powerloss.py for the operations that need root:
# losetup, dmsetup, mkfs.ext4, mount, umount and sync.  Two properties matter:
#
#   * root: the device-mapper ioctls and mount(2) are privileged.
#   * the HOST's mount namespace: the broker container is started by the Docker
#     daemon, which resolves its bind-mount source path in the host's namespace.
#     A filesystem mounted in a container's own namespace would be invisible to
#     the daemon, and the broker would write to the wrong directory -- the test
#     would pass for the wrong reason.
#
# `nsenter -t 1 -m` gives exactly that: PID 1's mount namespace, and (because
# nsenter implies --root for -m) the host's root filesystem, so the host's own
# dmsetup/losetup/mke2fs are used instead of whatever the image carries.
#
# The container is --privileged so it can reach /dev/loop-control and
# /dev/mapper/control, and --pid=host so `nsenter -t 1` resolves to the host's
# init.  /lib/modules is mounted read-only for modprobe dm-flakey.
#
# Usage: sh tools/docker/host_priv.sh <command> [args...]

set -eu

IMAGE=${S2B_PRIV_IMAGE:-erlang:28.3.1.0}

exec docker run --rm \
    --privileged \
    --pid=host \
    -v /lib/modules:/lib/modules:ro \
    "$IMAGE" \
    nsenter -t 1 -m -- "$@"
