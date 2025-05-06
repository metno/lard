#!/bin/bash
set -e

if repmgr node check --role | grep "standby"; then
    sudo -u postgres pg_basebackup -D - -X fetch -Ft -z -P | s3cmd put - s3://lard/backups/basebackup/"$(date +%Y%m%d%H%M%S)"
    # push metrics to both a and b gateways
    echo "last_base_backup $(date +%s)" | curl --data-binary @- https://monitoring-a.met.no/obsklim/pushgateway/metrics/job/lard_backup
    echo "last_base_backup $(date +%s)" | curl --data-binary @- https://monitoring-b.met.no/obsklim/pushgateway/metrics/job/lard_backup
fi