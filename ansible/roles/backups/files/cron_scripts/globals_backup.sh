#!/bin/bash
set -e

if repmgr node check --role | grep "primary"; then
    sudo -u postgres pg_dumpall --globals-only | s3cmd put - s3://lard/backups/"globals_$(date +%Y%m%d%H%M%S).sql"

    # create a file to get picked up by the node exporter with up to date time of last backup
    metric_file="/var/lib/node_exporter/globals_backup.prom"
    echo "last_globals_dump $(date +%s)" > "${metric_file}.tmp"
    mv "${metric_file}.tmp" "$metric_file"
fi
