## Recovery example --- where the database has been deleted

### Recreate the database 
`sudo -u postgres psql`
`create database lard`

### Get the data back
Find the name of the most recent data file in the s3 bucket
`s3cmd ls s3://lard/backups`
Get this data as a file localy (may be possible to stream it to a | somehow?)
`s3cmd get s3://lard/backups/lard__XXXXXXX lard__latest`
Put the data in the database
`sudo -u postgres psql -U postgres -d lard < lard__latest`
Go in and have a look around... 
`sudo -u postgres psql -d lard`

### Globals 
We are currently also backing up the globals (less frequently), which are not used here. This would include [cluster level](https://www.postgresql.org/docs/current/backup-dump.html#BACKUP-DUMP-ALL) things like roles. 
Most likely if we recreate the VM & database using ansible the right roles will be created (repmgr, etc.), but maybe good to have it in case.  