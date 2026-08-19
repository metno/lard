#!/bin/bash
set -e
SECONDS=0

echo "Creating a list of only data to restore in the dump, removing indexes"

pg_restore -l "$1" > /tmp/restore.list
grep -v 'INDEX' /tmp/restore.list | grep -v 'CONSTRAINT' > /tmp/no_indexes.list 

echo "Starting restore of: $1 into $2"

# we have 16 VCPUs on the VMs currently, so hopefuly -j 8 is ok 
pg_restore -U postgres --clean --if-exists -L /tmp/no_indexes.list -d "$2" -j 8 "$1"

seconds=$SECONDS
ELAPSED="Elapsed: $((seconds / 3600))hrs $(((seconds / 60) % 60))min $((seconds % 60))sec"

echo "Finished restore of: $1, took: $ELAPSED"

echo "Building indexes..."

pg_restore -U postgres -d "$2" -j 8 -I "$1"

echo "Finished restoring indexes"
