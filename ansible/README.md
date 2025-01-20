# LARD on OpenStack 2

## Get access to OpenStack

You need to create application credentials in the project you are going to
create the instances in, so that the ansible scripts can connect to the right
`ostack_cloud` (in our case it's `lard`).

The file should exist in `~/.config/openstack/clouds.yml`.
If you have MET access see what is written at the start of the readme [here](https://gitlab.met.no/it/infra/ostack-ansible21x-examples)
or in the authentication section [here](https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads).

## Dependencies

- Python 3.10+

- On your terminal run the following:

  ```terminal
  python3 -m venv ~/.venv/lard
  source ~/.venv/lard/bin/activate

  pip install -r requirements.txt
  ansible-galaxy collection install -fr requirements.yml
  ```

- [yamlfmt](https://github.com/google/yamlfmt) for formatting

## Setup

> [!IMPORTANT]
> Add your public key to the Ostack GUI.
> Go to "Compute" then "Key Pairs" and import your public key for later use during this step.

The IPs associated to the hosts in `inventory.yml` should correspond to
floating IPs you have requested in the network section of the OpenStack GUI.
These IPs are stored in the `ansible_host` variables inside each `host_vars\<hostname>.yml`.

Private variables are encrypted with `ansible-vault` and stored inside different files in `group_vars/servers/vault`.
You can either decrypt them beforehand, or pass the `-J` flag to ansible when running the playbooks.
Passwords can be found in [CICD variables](https://gitlab.met.no/met/obsklim/bakkeobservasjoner/lagring-og-distribusjon/db-products/poda/-/settings/ci_cd).

### 1. Provision!

Here we create the network and the VMs.

```terminal
ansible-playbook -i inventory.yml -e key_name=... provision.yml
```

> [!NOTE]
> If the network has already been setup and you only need to rebuild the VMs, you can do so with
>
> ```terminal
> ansible-playbook -i inventory.yml -e key_name=... provision.yml --skip-tags network
> ```

### 2. Configure!

In this step we format the VMs, exchange their SSH keys, setup the postgres
replication, and associate a floating IP to the primary host, which will be moved
to one of the standbys when doing a switchover.

> [!NOTE]
> The floating IP association times out, but this is ignored as it is a known bug.

```term
ansible-playbook -i inventory.yml -e db_password=... -e repmgr_password=... configure.yml
```

The parts to do with the floating IP that belongs to the primary (ipalias) are based on this [repo](https://gitlab.met.no/ansible-roles/ipalias/-/tree/master?ref_type=heads).

### 3. SSH into the VMs and connect to postgres

It might be helpful to create host aliases and add them to your `~/.ssh/config` file,
so you don't have to remember the IPs by heart. An example host alias looks like the following:

```ssh
Host lard-a
    HostName <IP>
    User ubuntu
```

Then you can simply run:

```terminal
ssh lard-a
```

To connect to postgres you can define a [service](https://www.postgresql.org/docs/current/libpq-pgservice.html) in
`~/.pg_service.conf`, like so:

```ini
[lard-a]
host=<IP>
port=5432
user=lard_user
dbname=lard
password=...
```

And then

```terminal
psql service=lard-a
```

### 4. Checking the status of the cluster

After `ssh`ing on the server and becoming postgres user (`sudo su postgres`), you can check the repmgr status with:

```terminal
postgres@lard-a:/home/ubuntu$ repmgr -f /etc/repmgr.conf node check
Node "lard-a":
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
postgres@lard-b:/home/ubuntu$ repmgr -f /etc/repmgr.conf node check
Node "lard-b":
        Server role: OK (node is standby)
        Replication lag: OK (0 seconds)
        WAL archiving: OK (0 pending archive ready files)
        Upstream connection: OK (node "lard-b" (ID: 2) is attached to expected upstream node "lard-a" (ID: 1))
        Downstream servers: OK (this node has no downstream nodes)
        Replication slots: OK (node has no physical replication slots)
        Missing physical replication slots: OK (node has no missing physical replication slots)
        Configured data directory: OK (configured "data_directory" is "/mnt/ssd-data/16/main")
```

While a few of the configurations are found in `/etc/postgresql/16/main/postgresql.conf`, many of them
can only be seen in `/mnt/ssd-data/16/main/postgresql.auto.conf` (need `sudo` to see contents).

### 5. Deploy LARD ingestor

This is as simple as running

```terminal
ansible-playbook -i inventory.yml deploy.yml
```

### 6. Teardown

If you need to delete the old VMs (`Compute → Instances`) and volumes (`Volumes → Volumes`) you can do so in the OpenStack GUI.

> [!CAUTION]
> When deleting things to build up again, if for some reason one of the IPs
> does not get disassociated properly, you have to do it manually from the GUI (`Network → Floating IPs`).

## Switchover

> [!NOTE]
> In the following we assume the primary is `lard-a` and the standby is `lard-b`

### 1. Planned downtime

This should only be used when both VMs are up and running, like in the case of planned maintenance on one data room.
You can use this script to switch the primary to the data room that will stay available ahead of time.

**Make sure you are aware which one is the primary, and put the names the right way around in this call.**

```
ansible-playbook -i inventory.yml -e primary=lard-a -e standby=lard-b switchover.yml
```

This should also be possible to do manually, you might need to follow what is done in the ansible script (aka restarting postgres on both VMs),
then performing the switchover (as the `postgres` user):

```terminal
repmgr standby switchover -f /etc/repmgr.conf --siblings-follow 
```

### 2. Unplanned downtime

This is used in the case where the primary has gone down (e.g. unplanned downtime of a data room).
Make sure you know which one you want to promote!

```terminal
ansible-playbook -i inventory.yml -e primary=lard-a -e standby=lard-b rejoin.yml
```

This can also be done manually following the following steps:

#### A. Promote standby node to primary

1. `ssh` into the standby and become `postgres` user.

1. Check the status:

   ```terminal
   postgres@lard-b:~$ repmgr -f /etc/repmgr.conf cluster show
   ```

   The primary should say it's **unreachable**.

1. Then promote the standby to primary:

   ```terminal
   postgres@lard-b:~$ repmgr -f /etc/repmgr.conf standby promote
   ```

1. You can then check the status again (and now the old primary will say **failed**).

1. Then move the IP in the OpenStack GUI (`Network → Floating IPs`, dissasociate
   it then associated it with the ipalias port on the other VM).

1. Restart LARD ingestion service in the new primary

   ```terminal
   ubuntu@lard-b:~$ sudo systemctl start lard_ingestion.service
   ```

#### B. Rejoin old primary

The cluster will be in a slightly confused state, because this VM still thinks
its a primary (although repmgr tells it the other one is running as a primary
as well). If the setup is running as asynchronous we could lose data that
wasn't copied over before the crash, if running synchronously then there should
be no data loss.

1. `ssh` into the new primary

   ```terminal
   postgres@lard-b:~$ repmgr -f /etc/repmgr.conf cluster show
   ...
   node "lard-b" (ID: 2) is running but the repmgr node record is inactive
   ```

1. `ssh` into the old primary

   ```terminal
   postgres@lard-a:~$ repmgr -f /etc/repmgr.conf cluster show
   ...
   node "lard-a" (ID: 1) is registered as standby but running as primary
   ```

1. With a **playbook**:

   ```terminal
   ansible-playbook -i inventory.yml -e primary=lard-a -e standby=lard-b rejoin.yml --skip-tags promote
   ```

   where `primary` is the host name of the primary node that has been down and should now be a standby.

#### Testing

1. Take out one of the replicas (or can shut off instance in the openstack GUI):

   ```terminal
   sudo pg_ctlcluster 16 main -m fast stop
   ```

1. To bring it back up (or turn it back on):

   ```terminal
   sudo pg_ctlcluster 16 main start
   ```

### Load balancing

The `bigip` role creates a user and basic database for the load balancer to test the health
of the lard database.
The database is created only on the primary node and replicated over to the standby.
The hba conf change needs to be run on both.

To run the bigip role on the VMs use:

```terminal
ansible-playbook -i inventory.yml -e bigip_password=... bigip.yml
```

### Links:

https://www.enterprisedb.com/postgres-tutorials/postgresql-replication-and-automatic-failover-tutorial#replication

### Useful ansible commands:

```terminal
ansible-inventory -i inventory.yml --graph

ansible servers -m ping -u ubuntu -i inventory.yml
```
