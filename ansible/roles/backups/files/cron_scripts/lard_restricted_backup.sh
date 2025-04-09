#!/bin/bash

if repmgr node check --role | grep "primary"; then
    sudo -u postgres pg_dump lard_restricted | gzip -9 -c | s3cmd put - s3://lard/backups/"lardrestricted_$(date +%Y%m%d%H%M%S).sql.gz"
fi