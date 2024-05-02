## README for LARD setup on openstack(2)

#### Useful ansible commands:
```
ansible-inventory -i inventory.yml --graph

ansible servers -m ping -u ubuntu -i inventory.yml
```

#### Dependencies to install
```
pip install -r requirements.txt

ansible-galaxy collection install openstack.cloud

ansible-galaxy collection install community.postgresql

``` 

### Get access to OpenStack
You need to create application credentials in the project you are going to create the instances in, so that the ansible scripts can connect to the right ostack_cloud which in our case needs to be called lard.

The file should exist here: 
~/.config/openstack/clouds.yml

If have met access see what is written at the start of the readme here: 
https://gitlab.met.no/it/infra/ostack-ansible21x-examples

Or in the authentication section here: 
https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads

### Add your public key to the Ostack GUI
Go to "Compute" then "Key Pairs" and import your public key for use in the provisioning step. 

### Provision!
The IPs in inventory.yml should correspond to floating ips you have requested in the network section of the open stack GUI. If you need to delete the old VMs (compute -> instances) and Volumes (volumes -> volumes) you can do so in the ostack GUI. *For some reason when deleting things to build up again one of the IPs did not get disassociated properly, and I had to do this manually (network -> floating IPs).* 

The vars for the network task are encrypted with ansible-vault (ansible-vault decrypt roles/networks/vars/main.yml). 
But if this has been setup before in the ostack project, these have likely already been run and therefore already exits so you could comment out this role from provision.yml.

```
ansible-playbook -i inventory.yml -e ostack_key_name=xxx provision.yml 
```

### Configure!
The third IP being passed in here is the one that gets associated with the primary, and moved when doing a switchover. 

```
ansible-playbook -i inventory.yml -e primary_floating_ip='157.249.*.*' -e db_password=xxx -e repmgr_password=xxx configure.yml 
```

The parts to do with the floating ip that belongs to the primary are based on: 
https://gitlab.met.no/ansible-roles/ipalias/-/tree/master?ref_type=heads

### Connect to database
```
PGPASSWORD=xxx psql -h 157.249.*.* -p 5432 -U lard_user -d lard
```

### Perform switchover
Make sure you are aware which one is the master, and put the names the right way around in this call. 
This should only be used when both VMs are up and running, like in the case of planned maintenance on one datarom. Then we would use this script to switch the primary to the datarom that will stay available ahead of time. 

```
ansible-playbook -i inventory.yml -e name_primary=lard-a -e name_standby=lard-b -e primary_floating_ip='157.249.*.*' switchover.yml
```

### Promote standby (assuming the primary is down)
Make sure you are know which one you want to promote!  
This is used in the case where the primary has gone down (e.g. unplanned downtime of a datarom). 

**Manually:**
ssh into the standby
`repmgr -f /etc/repmgr.conf cluster show`
check status 
`repmgr -f /etc/repmgr.conf standby promote`
promote the primary (while ssh-ed into that vmp)
Then move the ip in the ostack gui (see in network -> floating ips)

#### Later, when the old primary comes back up
The cluster will be in a slightly confused state, because this VM still thinks its a primary (although repmgr tells it the other one is running as a primary as well). If the setup is running as asynchronous we could lose data that wasn't copied over before the crash, if running synchronously then there should be no data loss. 

**TODO** - document how to demote / remake the old primary so its a standby...

### for testing:
Take out one of the replicas: 
`sudo pg_ctlcluster 16 main -m fast stop`
For bringing it back up:
`sudo pg_ctlcluster 16 main start`