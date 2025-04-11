#!/bin/bash

if repmgr node check --role | grep "primary"; then
    sudo -u postgres pg_dump lard_restricted --compress=gzip | s3cmd put - s3://lard/backups/"lard_restricted_$(date +%Y%m%d%H%M%S).sql.gz"
    # push metrics to both a and b gateways
    echo "last_lard_restricted_backup $(date +%s)" | curl --data-binary @- https://monitoring-a.met.no/obsklim/pushgateway/metrics/job/lard_backup
    echo "last_lard_restricted_backup $(date +%s)" | curl --data-binary @- https://monitoring-b.met.no/obsklim/pushgateway/metrics/job/lard_backup
fi