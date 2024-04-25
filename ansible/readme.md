## README for LARD setup on openstack(2)

#### Useful ansible commands:
```
ansible-inventory -i inventory.yml --graph

ansible-galaxy collection install openstack.cloud

ansible-galaxy collection install community.postgresql

ansible servers -m ping -u ubuntu -i inventory.yml
```

### Get access to OpenStack
You need to create application credentials in the project you are going to create the instances in, so that the ansible scripts can connect to the right ostack_cloud which in our case needs to be called lard.

The file should exist here: 
~/.config/openstack/clouds.yml

If have met access see what is written at the start of the readme here: 
https://gitlab.met.no/it/infra/ostack-ansible21x-examples

Or in the authentication section here: 
https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads

### Provision!
The IPs in inventory.yml should correspond to floating ips you have requested in the network section of the open stack GUI. *For some reason when deleting things to build up again one of the IPs did not get disassociated properly, and I had to do this manually.* 

This the vars for the network task are encrypted with ansible-vault (ansible-vault decrypt roles/networks/vars/main.yml)
But if this has been setup before in the ostack project, these have likely already been run and therefore already exits so you could comment out this role from provision.yml.

```
ansible-playbook -i inventory.yml provision.yml 
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

```
ansible-playbook -i inventory.yml -e name_primary=lard-a -e name_standby=lard-b -e primary_floating_ip='157.249.*.*' switchover.yml
```
