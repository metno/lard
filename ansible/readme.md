## README for LARD setup on openstack(2)

#### Useful ansible commands:

```terminal
ansible-inventory -i inventory.yml --graph

ansible servers -m ping -u ubuntu -i inventory.yml
```

#### Dependencies to install

```terminal
python3 -m venv {your_dir}
source {your_dir}/bin/activate

pip install -r requirements.txt
ansible-galaxy collection install -fr requirements.yml
```

### Get access to OpenStack

You need to create application credentials in the project you are going to
create the instances in, so that the ansible scripts can connect to the right
ostack_cloud which in our case needs to be called lard.

The file should exist in `~/.config/openstack/clouds.yml`.
If have MET access see what is written at the start of the readme [here](https://gitlab.met.no/it/infra/ostack-ansible21x-examples)
or in the authentication section [here](https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads).

### Add your public key to the Ostack GUI

Go to "Compute" then "Key Pairs" and import your public key for use in the provisioning step.

### Provision!

The IPs in `inventory.yml` should correspond to floating ips you have requested
in the network section of the open stack GUI. If you need to delete the old VMs
(compute -> instances) and Volumes (volumes -> volumes) you can do so in the
ostack GUI.

> \[!CAUTION\] For some reason when deleting things to build up again one of the IPs
> did not get disassociated properly, and I had to do this manually (network ->
> floating IPs).

The vars for the network and addssh tasks are encrypted with ansible-vault
(ansible-vault decrypt roles/networks/vars/main.yml, ansible-vault decrypt
roles/addshhkeys/vars/main.yml, ansible-vault decrypt
roles/vm_format/vars/main.yml). But if this has been setup before in the ostack
project, these have likely already been run and therefore already exits so you
could comment out this role from provision.yml. Passwords are in [ci_cd variables](https://gitlab.met.no/met/obsklim/bakkeobservasjoner/lagring-og-distribusjon/db-products/poda/-/settings/ci_cd).

```terminal
ansible-playbook -i inventory.yml -e ostack_key_name=xxx provision.yml 
```

After provisioning the next steps may need to ssh into the hosts, and thus you need to add them to your known hosts.
Ansible appears to be crap at this, so its best to do it before running the next step by going:
`ssh ubuntu@157.249.*.*`
For all the VMs.
If cleaning up from tearing down a previous set of VMs you may also need to remove them first:
`ssh-keygen -f "/home/louiseo/.ssh/known_hosts" -R "157.249.*.*"`

### Configure!

The third IP being passed in here is the one that gets associated with the primary, and moved when doing a switchover.
*NOTE:* The floating IP association times out, but this is ignored as it is a known bug.

```term
ansible-playbook -i inventory.yml -e primary_floating_ip='157.249.*.*' -e db_password=xxx -e repmgr_password=xxx configure.yml 
```

The parts to do with the floating ip that belongs to the primary (ipalias) are based on:
https://gitlab.met.no/ansible-roles/ipalias/-/tree/master?ref_type=heads

### Connect to database

```
PGPASSWORD=xxx psql -h 157.249.*.* -p 5432 -U lard_user -d lard
```

### Checking the cluster

Become postgres user: sudo su postgres

```
postgres@lard-b:/home/ubuntu$ repmgr -f /etc/repmgr.conf node check
Node "lard-b":
        Server role: OK (node is primary)
        Replication lag: OK (N/A - node is primary)
        WAL archiving: OK (0 pending archive ready files)
        Upstream connection: OK (N/A - node is primary)
        Downstream servers: OK (1 of 1 downstream nodes attached)
        Replication slots: OK (node has no physical replication slots)
        Missing physical replication slots: OK (node has no missing physical replication slots)
        Configured data directory: OK (configured "data_directory" is "/mnt/ssd-data/16/main")
```

```
postgres@lard-a:/home/ubuntu$ repmgr -f /etc/repmgr.conf node check
Node "lard-a":
        Server role: OK (node is standby)
        Replication lag: OK (0 seconds)
        WAL archiving: OK (0 pending archive ready files)
        Upstream connection: OK (node "lard-a" (ID: 1) is attached to expected upstream node "lard-b" (ID: 2))
        Downstream servers: OK (this node has no downstream nodes)
        Replication slots: OK (node has no physical replication slots)
        Missing physical replication slots: OK (node has no missing physical replication slots)
        Configured data directory: OK (configured "data_directory" is "/mnt/ssd-data/16/main")
```

While a few of the configurations are found in /etc/postgresql/16/main/postgresql.conf (particularly in the ansible block at the end), many of them
can only be seen in /mnt/ssd-data/16/main/postgresql.auto.conf (need sudo to see contents).

### Perform switchover

This should only be used when both VMs are up and running, like in the case of planned maintenance on one datarom.
Then we would use this script to switch the primary to the datarom that will stay available ahead of time.

*Make sure you are aware which one is the master, and put the names the right way around in this call.*

```
ansible-playbook -i inventory.yml -e name_primary=lard-a -e name_standby=lard-b -e primary_floating_ip='157.249.*.*' switchover.yml
```

This should also be possible to do manually, but might need to follow what is done in the ansible script (aka restarting postgres on both VMs), then performing the switchover:
`repmgr standby switchover -f /etc/repmgr.conf --siblings-follow` (need to be postgres user)

### Promote standby (assuming the primary is down)

Make sure you are know which one you want to promote!\
This is used in the case where the primary has gone down (e.g. unplanned downtime of a datarom).

**Manually:**
SSH into the standby
`repmgr -f /etc/repmgr.conf cluster show`
Check the status (The primary should say its 'uncreachable')
`repmgr -f /etc/repmgr.conf standby promote`
Then promote the primary (while ssh-ed into that VM)
You can the check the status again (and now the old primary will say failed)

Then move the ip in the ostack gui (see in network -> floating ips, dissasociate it then associated it with the ipalias port on the other VM)

#### Later, when the old primary comes back up

The cluster will be in a slightly confused state, because this VM still thinks its a primary (although repmgr tells it the other one is running as a primary as well). If the setup is running as asynchronous we could lose data that wasn't copied over before the crash, if running synchronously then there should be no data loss.

SSH into the new primary
`repmgr -f /etc/repmgr.conf cluster show`
says:

- node "lard-a" (ID: 1) is running but the repmgr node record is inactive

SSH into the old primary
`repmgr -f /etc/repmgr.conf cluster show`
says:

- node "lard-b" (ID: 2) is registered as standby but running as primary

With a **playbook** (rejoin_ip is the ip of the node that has been down and should now be a standby not a primary):

```
ansible-playbook -i inventory.yml -e rejoin_ip=157.249.*.* -e primary_ip=157.249.*.* rejoin.yml 
```

Or **manually**:
Make sure the pg process is stopped (see fast stop command) if it isn't already

Become postgres user:
`sudo su postgres`
Test the rejoin (host is the IP of the new / current primary, aka the other VM)
`repmgr node rejoin -f /etc/repmgr.conf -d 'host=157.249.*.* user=repmgr dbname=repmgr connect_timeout=2' --force-rewind=/usr/lib/postgresql/16/bin/pg_rewind --verbose --dry-run`
Perform a rejoin
`repmgr node rejoin -f /etc/repmgr.conf -d 'host=157.249.*.* user=repmgr dbname=repmgr connect_timeout=2' --force-rewind=/usr/lib/postgresql/16/bin/pg_rewind --verbose`

### for testing:

Take out one of the replicas (or can shut off instance in the openstack GUI):
`sudo pg_ctlcluster 16 main -m fast stop`
For bringing it back up (or turn it back on):
`sudo pg_ctlcluster 16 main start`

### for load balancing at MET

This role creates a user and basic db for the loadbalancer to test the health of the db. Part of the role is allowed to fail on the secondary ("cannot execute \_\_\_ in a read-only transaction"), as it should pass on the primary and be replicated over. The hba conf change needs to be run on both.

The vars are encrypted, so run: ansible-vault decrypt roles/bigip/vars/main.yml

Then run the bigip role on the VMs:

```
ansible-playbook -i inventory.yml -e bigip_password=xxx bigip.yml
```

### Links:

https://www.enterprisedb.com/postgres-tutorials/postgresql-replication-and-automatic-failover-tutorial#replication
