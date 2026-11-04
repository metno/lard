#!/bin/bash
set -e

if repmgr node check --role | grep "primary"; then
    sudo -u postgres pg_dump lard_restricted -Fc | s3cmd put - s3://lard/backups/"lard_restricted_$(date +%Y%m%d%H%M%S).dump"

    # create a file to get picked up by the node exporter with up to date time of last backup
    metric_file="/var/lib/node_exporter/lard_restricted_backup.prom"
    echo "last_lard_restricted_dump $(date +%s)" > "${metric_file}.tmp"
    mv "${metric_file}.tmp" "$metric_file"
fi
