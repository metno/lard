#!/bin/bash
set -e
SECONDS=0

# Config variables for readability
DUMP_FILE="$1"
DB_NAME="$2"
JOBS=8

echo "Starting restore (pre-data) of: $DUMP_FILE into $DB_NAME"
# This drops tables automatically if they exist. No manual TRUNCATE needed.
pg_restore -U postgres --clean --if-exists --section=pre-data -d "$DB_NAME" "$DUMP_FILE"

echo "Starting restore (data) of: $DUMP_FILE into $DB_NAME"
pg_restore -U postgres --section=data -d "$DB_NAME" -j "$JOBS" "$DUMP_FILE"

seconds=$SECONDS
ELAPSED="Elapsed: $((seconds / 3600))hrs $(((seconds / 60) % 60))min $((seconds % 60))sec"
echo "Finished schema and data restore of: $DUMP_FILE, took: $ELAPSED"

SECONDS=0
echo "Restoring post-data (indexes)..."

# Safely capped at 16GB total max memory footprint (8 jobs * 2GB) on 32GB system
PGOPTIONS="-c maintenance_work_mem=2GB" pg_restore -U postgres --section=post-data -d "$DB_NAME" -j "$JOBS" "$DUMP_FILE"

seconds=$SECONDS
ELAPSED="Elapsed: $((seconds / 3600))hrs $(((seconds / 60) % 60))min $((seconds % 60))sec"
echo "Finished restoring indexes of $DUMP_FILE, took: $ELAPSED"

echo "Running analyze..."
psql -d "$DB_NAME" -c "ANALYZE;"

echo "Done restore, and executing analyze"