https://www.postgresql.org/docs/current/warm-standby.html

https://www.postgresql.org/docs/current/warm-standby-failover.html

## NOTES:
Pretty sure we want a hot (or potentially warm, if we don't want to load balance on reads) standby with asynchronous streaming replication from the primary
If we wanted a synchronous replication, this would mean either we would have to have both the primary and the standby up at all times or potentially multiple standbys?
https://www.postgresql.org/docs/current/warm-standby.html#SYNCHRONOUS-REPLICATION

Found this setup for kvalobs, but they run postgres 13: 
https://gitlab.met.no/met/obsklim/bakkeobservasjoner/data-og-kvalitet/kvalobs/ansible/-/blob/master/roles/postgres/tasks/repmgr.yml
https://gitlab.met.no/met/obsklim/bakkeobservasjoner/data-og-kvalitet/kvalobs/ansible/-/tree/master/roles/postgres/templates/etc/postgresql/13/main 

## if creating a new replica / standby 
https://www.postgresql.org/docs/current/app-pgbasebackup.html
Then can use pg_basebackup