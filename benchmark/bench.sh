#!/bin/sh

set -euo pipefail


if [ -z "$(which plow)" ]; then
    2>&1 echo "Need plow!"
    exit 1
fi
