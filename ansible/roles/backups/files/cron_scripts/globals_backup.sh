#!/bin/bash

if repmgr node check --role | grep "primary"; then
    sudo -u postgres pg_dumpall --globals-only | s3cmd put - s3://lard/backups/"globals_$(date +%Y%m%d%H%M%S).sql"
    # push metrics to both a and b gateways
    echo "last_globals_backup $(date +%s)" | curl --data-binary @- https://monitoring-a.met.no/obsklim/pushgateway/metrics/job/lard_backup
    echo "last_globals_backup $(date +%s)" | curl --data-binary @- https://monitoring-b.met.no/obsklim/pushgateway/metrics/job/lard_backup
fi