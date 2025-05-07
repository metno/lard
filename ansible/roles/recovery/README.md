## Recovery using a basebackup
https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-PITR-RECOVERY

get the backup on the VM
`s3cmd get --recursive s3://lard/backups/basebackup/`
NOTE: eventually need better naming of these, so have recent base + incremental 

Unzip the backup (base.tar.gz) to a temporary folder if need to sew together with the pg_combinebackup tool
`mkdir temp`
`tar -xvzf base.tar.gz -C temp`

#### Stop postgres
`sudo pg_ctlcluster 17 main stop`

If any chance of recovery, then consider copying out the current files in the /main directory to somewhere else (s3?).
Or at the very least the contents of the cluster's pg_wal subdirectory, as it might contain WAL files which were not archived before the system went down.

#### Clean up old files, move / unzip backup files
Need to use postgres user for this part since need to do stuff in its data folder (/mnt/ssd-data).
`sudo su postgres`

Delete contents of the data dir:
/mnt/ssd-data$ `rm -r postgresql/17/main/*`
 
Unzip to the right folder:
`tar -xvzf base.tar.gz -C postgresql/17/main`

Or move the data to the right spot (if already unziped, perhaps if had to put parts together from incremental backups with pg_combinebackup):
`cp -r /temp /postgresql/17/main`

#### Prepare to start the database in the desired way
Delete files in pg_wal/ if this is an old backup where you will transplant recent wal files? ... keep the folders archive_status and summaries? 
can try copying any pg_wal/ files that were on the cluster that died, if have saved those as mentioned above 

Delete the lines in postgres.auto.conf that say the 
primary_conninfo ...
primary_slot_name  ...
(if bringing up for staging, since its not relevant anymore... but relevant if bringing back real cluster, but maybe the primary has changed)

If actually doing this as a recovery, create an empty file called recovery.signal in the data directory (if actually bringing it up in recovery, but not if making new standalone DB on staging)
Delete the file standby.signal (if bringing it back up for recovery, or if just making a standalone DB)
If create recovery.signal set recovery configuration settings in postgresql.conf, need at least a restore command something like:
restore_command = 'cp /mnt/server/archivedir/%f %p'

#### Start postgres
`sudo pg_ctlcluster 17 main start`

Can see log / progress here: `tail -f /var/log/postgresql/postgresql-17-main.log`