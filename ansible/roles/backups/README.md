## Recovery example --- where the database has been deleted

### Recreate the database 
`sudo -u postgres psql`
`create database lard;`
`create database lard_restricted;`

### Get the data back
Postgres [documentation](https://www.postgresql.org/docs/8.1/backup.html#BACKUP-DUMP-RESTORE)
Find the name of the most recent data file in the s3 bucket
`s3cmd ls s3://lard/backups`
Get this data as a file localy 
`s3cmd get s3://lard/backups/lard__XXXXXX.sql.gz - | gunzip > lard_latest`
Put the data in the database
`sudo -u postgres psql -U postgres -d lard < lard_latest`
Or avoid moving it over (stream)
`s3cmd get s3://lard/backups/lard_XXXXXX.sql.gz - | gunzip | sudo -u postgres psql -U postgres -d lard -f -`
Go in and have a look around... 
`sudo -u postgres psql -d lard`

### Globals 
We are currently also backing up the globals (less frequently), which are not used here. This would include [cluster level](https://www.postgresql.org/docs/current/backup-dump.html#BACKUP-DUMP-ALL) things like roles. 
Most likely if we recreate the VM & database using ansible the right roles will be created (repmgr, etc.), but maybe good to have it in case.  

### Other notes:
Can look at diffs between the backups without moving them over from the s3
`diff <(s3cmd get s3://lard/backups/lard_20250408082524.sql.gz - | gunzip ) <(s3cmd get s3://lard/backups/lard_20250408082620.sql.gz - | gunzip )`