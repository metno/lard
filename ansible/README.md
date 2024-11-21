## README for LARD setup on OpenStack 2

### Get access to OpenStack

You need to create application credentials in the project you are going to
create the instances in, so that the ansible scripts can connect to the right
`ostack_cloud` (in our case it's `lard`).

The file should exist in `~/.config/openstack/clouds.yml`.
If you have MET access see what is written at the start of the readme [here](https://gitlab.met.no/it/infra/ostack-ansible21x-examples)
or in the authentication section [here](https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads).

### Dependencies

- Python 3.10+

- On your terminal run the following:

  ```terminal
  python3 -m venv ~/.venv/lard
  source ~/.venv/lard/bin/activate

  pip install -r requirements.txt
  ansible-galaxy collection install -fr requirements.yml
  ```

### Provision!

> [!IMPORTANT]
> Add your public key to the Ostack GUI.
> Go to "Compute" then "Key Pairs" and import your public key for later use during this step.

The IPs associated to the hosts in `inventory.yml` should correspond to
floating IPs you have requested in the network section of the OpenStack GUI.
These IPs are stored in the `ansible_host` variables inside each `host_vars\host_name.yml`.

If you need to delete the old VMs (Compute -> Instances) and Volumes (Volumes
-> Volumes) you can do so in the OpenStack GUI.

> [!CAUTION]
> When deleting things to build up again, if for some reason one of the IPs
> does not get disassociated properly, you have to do it manually from the GUI (Network -> Floating IPs).

Private variables are encrypted with ansible-vault and stored inside different files per role in `group_vars/servers/vault`.
Passwords can be found in [CICD variables](https://gitlab.met.no/met/obsklim/bakkeobservasjoner/lagring-og-distribusjon/db-products/poda/-/settings/ci_cd).

```terminal
ansible-playbook -i inventory.yml -e key_name=... provision.yml -J
```

> [!NOTE]
> If the network has already been setup and you only need to rebuild the VMs, you can do so with
>
> ```terminal
> ansible-playbook -i inventory.yml -e key_name=... provision.yml --skip-tags network -J
> ```

### Configure!

The floating IP (`fip`) being passed in here is the one that gets associated with the primary, and moved when doing a switchover.

> [!NOTE]
> The floating IP association times out, but this is ignored as it is a known bug.

```term
ansible-playbook -i inventory.yml -e fip=... -e db_password=... -e repmgr_password=... configure.yml -J
```

The parts to do with the floating IP that belongs to the primary (ipalias) are based on this [repo](https://gitlab.met.no/ansible-roles/ipalias/-/tree/master?ref_type=heads).

#### SSH into the VMs

It might be helpful to create host aliases and add them to your `~/.ssh/config` file,
so you don't have to remember the IPs by heart. An example host alias looks like the following:

```ssh
Host lard-a
    HostName 157.249.*.*
    User ubuntu
```

Then run:

```terminal
ssh lard-a
```

#### Connect to database

```
PGPASSWORD=... psql -h 157.249.*.* -p 5432 -U lard_user -d lard
```

> [!NOTE]
> Unfortunately the ssh alias does not work for psql,
> but you can define a separate service inside `~/.pg_service.conf`
>
> ```
> [lard-a]
> host=157.249.*.*
> port=5432
> user=lard_user
> dbname=lard
> password=...
> ```

### Checking the status of the cluster

After `ssh`ing on the server and becoming postgres user (`sudo su postgres`), you can check the repmgr status with:

```terminal
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

```terminal
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

While a few of the configurations are found in
`/etc/postgresql/16/main/postgresql.conf` (particularly in the ansible block at the end), many of them
can only be seen in `/mnt/ssd-data/16/main/postgresql.auto.conf` (need sudo to see contents).

### Perform switchover

This should only be used when both VMs are up and running, like in the case of planned maintenance on one data room.
Then you would use this script to switch the primary to the data room that will stay available ahead of time.
*Make sure you are aware which one is the master, and put the names the right way around in this call.*

```
ansible-playbook -i inventory.yml -e primary=... -e standby=... -e fip=... switchover.yml -J
```

This should also be possible to do manually, you might need to follow what is done in the ansible script (aka restarting postgres on both VMs),
then performing the switchover (as the `postgres` user):

```terminal
repmgr standby switchover -f /etc/repmgr.conf --siblings-follow 
```

### Promote standby (assuming the primary is down)

This is used in the case where the primary has gone down (e.g. unplanned downtime of a data room).
Make sure you are know which one you want to promote!

**Manually:**

1. `ssh` into the standby

1. Check the status

   ```terminal
   repmgr -f /etc/repmgr.conf cluster show
   ```

   The primary should say its **uncreachable**

1. Then promote the standby to primary (while `ssh`-ed into the standby VM)

   ```terminal
   repmgr -f /etc/repmgr.conf standby promote
   ```

1. You can the check then status again (and now the old primary will say **failed**)

1. Then move the ip in the OpenStack GUI (see in network -> floating ips, dissasociate it then associated it with the ipalias port on the other VM)

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

With a **playbook** (`rejoin_ip` is the ip of the primary node that has been down and should now be a standby):

```
ansible-playbook -i inventory.yml -e rejoin_ip=... -e primary_ip=... rejoin.yml 
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

#### Useful ansible commands:

```terminal
ansible-inventory -i inventory.yml --graph

ansible servers -m ping -u ubuntu -i inventory.yml
```
