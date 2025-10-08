# LARD on OpenStack 2

## Get access to OpenStack

You need to create application credentials in the project you are going to
create the instances in, so that the ansible scripts can connect to the right
`ostack_cloud` (in our case it's `lard`).

The file should exist in `~/.config/openstack/clouds.yml`.
If you have MET access see what is written at the start of the readme [here](https://gitlab.met.no/it/infra/ostack-ansible21x-examples)
or in the authentication section [here](https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads).

## Dependencies

- [uv](https://docs.astral.sh/uv/)

- On your terminal run the following:

  ```terminal
  uv run ansible-galaxy collection install -fr requirements.yml
  ```

- [yamlfmt](https://github.com/google/yamlfmt) for formatting

- I recommend setting an abbreviation/alias in your shell to make commands less of a chore. Here is an example for fish:
  ```fish
  abbr --add play uv run ansible-playbook
  abbr --add vault uv run ansible-vault
  abbr --add ansible-lint uv run ansible-lint
  ```

## Setup

The IPs associated to the hosts in the inventories should correspond to
floating IPs that have been requested in the network section of the OpenStack GUI.
These IPs are stored in the `ansible_host` variables inside each
`host_vars\<hostname>.yml` file.

Private variables are encrypted with `ansible-vault` and stored in the
`vault` subdirectories of group, host, or play vars. In order to run the playbooks you can
either use an
[`ansible.cfg`](https://docs.ansible.com/ansible/latest/reference_appendices/config.html#default-vault-password-file)
file, or pass the `-J` flag when running the playbooks to get prompted for the
password.
The password can be found in [CICD variables](https://gitlab.met.no/met/obsklim/bakkeobservasjoner/lagring-og-distribusjon/db-products/poda/-/settings/ci_cd).

> [!TIP]
> If you need to change some of the private variables use `ansible-vault edit`

The example commands in the readme all run against staging, by passing the
staging inventory. If you want to run against production, change the inventory.

Our playbooks are all intented to be idempotent, so if the settings change, you
should be able to run the playbook again to get the deployment in the right
state without starting from scratch. To further speed this up, you can pass a
tag to only run certain plays from the playbook with `--tags`

### 1. Provision!

The first step is to set up a personal key pair on OpenStack, create the project network and the VMs.

> [!NOTE]
> This playbook has 2 variables set in an ignored file (vars/ostack_key_vars.yml)
> which you will need to create yourself:
>
> `ostack_key_name` is a simple label that will be associated to the
> public ssh key stored in `ostack_key_file` (this needs to be an absolute path,
> e.g. `/home/user/.ssh/key.pub`).


```terminal
uv run ansible-playbook -i staging.yml playbooks/provision.yml
```

> [!NOTE]
> If you only need to rebuild the VMs, you can do so with
>
> ```terminal
> # delete current instances and volumes if needed
> uv run ansible-playbook -i staging.yml playbooks/teardown.yml
>
> # rebuild
> uv run ansible-playbook -i staging.yml playbooks/provision.yml -t vm
> ```
>
> There are also separate tags for the other tasks (namely, `addkey` and `network`), so you can use whatever combination you need.

### 2. Configure!

In this step we exchange SSH keys between the instances, set up the postgres
replication, and associate a floating IP to the primary host, which will be moved
to one of the standbys when doing a switchover.

> [!WARNING]
> This playbook takes a definition of which node is the primary from the
> inventory vars. If someone else has been touching prod or we've been dealing
> with an outage, you should probably check that this var is correct

```term
uv run ansible-playbook -i staging.yml playbooks/configure.yml
```

The floating IP association can time out, but this is ignored as it is a known bug.
The parts to do with the floating IP that belongs to the primary (ipalias) are based on this [repo](https://gitlab.met.no/ansible-roles/ipalias/-/tree/master?ref_type=heads).

### 3. SSH into the VMs and connect to postgres

It might be helpful to create host aliases and add them to your `~/.ssh/config` file,
so you don't have to remember the IPs by heart (since we don't have a DNS setup at the moment).
An example host alias looks like the following:

```conf
Host lard-a
    HostName <IP>
    User ubuntu
    ProxyJump <jump_host>
```

Then you can simply type:

```terminal
ssh lard-a
```

To connect to postgres you can define a [service](https://www.postgresql.org/docs/current/libpq-pgservice.html) in
`~/.pg_service.conf`, like so:

```ini
[lard-a]
host=<IP>
port=5432
user=lard_readonly
dbname=lard
password=...
```

And then

```terminal
psql service=lard-a
```

Our `pg_hba.conf` only allows connections for the non-readonly `lard_user`
locally, so you will have to ssh in first to use that.

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
        Configured data directory: OK (configured "data_directory" is "/mnt/ssd-data/17/main")
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
        Configured data directory: OK (configured "data_directory" is "/mnt/ssd-data/17/main")
```

While a few of the configurations are found in `/etc/postgresql/17/main/postgresql.conf`, many of them
can only be seen in `/mnt/ssd-data/17/main/postgresql.auto.conf` (need `sudo` to see contents).

### 5. Deploy LARD

This is as simple as running

```terminal
uv run ansible-playbook -i staging.yml playbooks/deploy.yml
```

### 6. Teardown

> [!CAUTION]
> When deleting things to build up again, if for some reason one of the IPs
> does not get disassociated properly, you have to do it manually from the GUI (`Network → Floating IPs`).

This playbook removes the host ssh keys from your `known_hosts` file
(preventing issues in case you were to rebuild them), and deletes the VMs with
their associated volumes.
Again, there are different tags you can specify if you only need to perform a subset of actions (`ssh`, `vm`, `volume`).

```terminal
uv run ansible-playbook -i staging.yml playbooks/teardown.yml
```

## Switchover

> [!IMPORTANT]
> In this section we assume that the current primary is `lard-a`, and that the
> standby we want to promote is `lard-b`. Make sure you are aware which one is
> the primary, and put the names the right way around when needed.

### 1. Planned downtime

This should only be used when both VMs are up and running, like in the case of planned maintenance on one data room.
You can use this script to switch the primary to the data room that will stay available ahead of time.

The difference between this and failover is that here clean (not needing
pg_rewind) demotion of the primary is performed. In the case of failover, this
is not possible as the primary is inaccessible, so we do it the dirty way.

```
uv run ansible-playbook -i staging.yml -e old=lard-a -e new=lard-b playbooks/switchover.yml
```

This can also be done manually, you need to follow what is done in the ansible script (aka restarting postgres on both VMs),
then performing the switchover (as the `postgres` user):

```terminal
repmgr standby switchover --siblings-follow
```

and move the IP alias to the new primary.

### 2. Unplanned downtime

This is used in the case where the primary has gone down (e.g. unplanned downtime of a data room).

```terminal
uv run ansible-playbook -i staging.yml -e old=lard-a -e new=lard-b playbooks/failover.yml
```

This can also be done manually following these steps:

1. `ssh` into the standby and become `postgres` user.

1. Check the status:

   ```terminal
   postgres@lard-b:~$ repmgr cluster show
   ```

   The primary should say it's **unreachable**.

1. Then promote the standby to primary:

   ```terminal
   postgres@lard-b:~$ repmgr standby promote
   ```

1. You can then check the status again (and now the old primary will say **failed**).

1. Then move the IP in the OpenStack GUI (`Network → Floating IPs`, dissasociate
   it then associated it with the ipalias port on the other VM).

1. Restart LARD ingestion service in the new primary

   ```terminal
   ubuntu@lard-b:~$ sudo systemctl start lard_ingestion.service
   ```

1. The cluster will be in a slightly confused state, because this VM still thinks
   its a primary (although repmgr tells it the other one is running as a primary
   as well). If the setup is running as asynchronous we could lose data that
   wasn't copied over before the crash, if running synchronously then there should
   be no data loss.

   - `ssh` into the new primary

     ```terminal
     postgres@lard-b:~$ repmgr cluster show
     ...
     node "lard-b" (ID: 2) is running but the repmgr node record is inactive
     ```

   - `ssh` into the old primary

     ```terminal
     postgres@lard-a:~$ repmgr cluster show
     ...
     node "lard-a" (ID: 1) is registered as standby but running as primary
     ```

   - Rejoin the primary:

     ```terminal
     uv run ansible-playbook -i staging.yml -e old=lard-a -e new=lard-b playbooks/failover.yml --tags "rejoin"
     ```

#### Testing

1. Take out one of the replicas (or can shut off instance in the openstack GUI):

   ```terminal
   sudo pg_ctlcluster 17 main -m fast stop
   ```

1. To bring it back up (or turn it back on):

   ```terminal
   sudo pg_ctlcluster 17 main start
   ```

### Links:

https://www.enterprisedb.com/postgres-tutorials/postgresql-replication-and-automatic-failover-tutorial#replication

### Useful ansible commands:

```terminal
uv run ansible-inventory -i staging.yml --graph

uv run ansible servers -m ping -u ubuntu -i staging.yml
```
