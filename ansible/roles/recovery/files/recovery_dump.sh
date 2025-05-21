#!/bin/bash
set -e
SECONDS=0

echo "Starting restore of: $1 into $2"

# we have 16 VCPUs on the VMs currently, so hopefuly -j 8 is ok 
pg_restore -U postgres -d "$2" -j 8 "$1"

seconds=$SECONDS
ELAPSED="Elapsed: $((seconds / 3600))hrs $(((seconds / 60) % 60))min $((seconds % 60))sec"

echo "Finished restore of: $1, took: $ELAPSED"
