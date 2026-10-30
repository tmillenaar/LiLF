#!/bin/bash
set -euo pipefail

MARCH="${MARCH:-znver2}"
MTUNE="${MTUNE:-znver2}"
OUTPUT_IMAGE="${OUTPUT_IMAGE:-FLoCs-LBAamd3-261026.simg}"

apptainer build --fakeroot --force \
    --build-arg MARCH="${MARCH}" \
    --build-arg MTUNE="${MTUNE}" \
    "${OUTPUT_IMAGE}" \
    FLoCs-LBAamd3-261026
