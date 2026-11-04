#!/bin/bash
set -e

if repmgr node check --role | grep "primary"; then
    # TODO: check the path `persistence` is right
    tar czf - persistence | s3cmd put - s3://lard/backups/"stinfo_$(date +%Y%m%d%H%M%S).tar.gz"

    # create a file to get picked up by the node exporter with up to date time of last backup
    metric_file="/var/lib/node_exporter/stinfo_backup.prom"
    echo "last_stinfo_backup $(date +%s)" > "${metric_file}.tmp"
    mv "${metric_file}.tmp" "$metric_file"
fi
