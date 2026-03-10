#!/bin/bash
set -e

if repmgr node check --role | grep "primary"; then
    # TODO: check the path `persistence` is right
    tar czf - persistence | s3cmd put - s3://lard/backups/"stinfo_$(date +%Y%m%d%H%M%S).tar.gz"
    # push metrics to both a and b gateways
    echo "last_stinfo_backup $(date +%s)" | curl --data-binary @- https://monitoring-a.met.no/obsklim/pushgateway/metrics/job/stinfo_backup
    echo "last_stinfo_backup $(date +%s)" | curl --data-binary @- https://monitoring-b.met.no/obsklim/pushgateway/metrics/job/stinfo_backup
fi
