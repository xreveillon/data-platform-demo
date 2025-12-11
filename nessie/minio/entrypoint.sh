#!/bin/bash
set -ex

DATA_DIR="/data"
ARCHIVE="/tmp/minio-preload-data.tar.gz"

mkdir -p "$DATA_DIR"

if [ -z "$(ls -A $DATA_DIR)" ]; then
    echo "Extracting pre-loaded data..."
    tar -xzf "$ARCHIVE" -C "$DATA_DIR"
    echo "Done."
fi

exec minio "$@"
