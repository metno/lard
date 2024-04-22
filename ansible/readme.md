https://gitlab.met.no/it/infra/ostack-ansible21x-examples

https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads

https://gitlab.met.no/ansible-roles/ipalias/-/tree/master?ref_type=heads

#### Useful ansible commands:
ansible-inventory -i openstack.yml --graph

ansible-galaxy collection install ansible.posix

ansible-galaxy collection install openstack.cloud

ansible-galaxy collection install community.postgresql

ansible servers -m ping -u ubuntu -i inventory.ini

### Provision!
This the vars for the network task are encrypted with ansible-vault (ansible-vault decrypt roles/networks/vars/main.yml)
But if this has been setup before in the ostack project, these have likely already been run and therefore already exits so you could comment out this role from provision.yml.

ansible-playbook -e vm1_ip='157.249.*.*' -e vm2_ip='157.249.*.*' -e primary_floating_ip='157.249.*.*' -e ostack_network_cidr='*.*.*.*/24' provision.yml 

### Configure
ansible-playbook -i inventory.ini -e vm1_ip='157.249.*.*' -e vm2_ip='157.249.*.*' -e primary_floating_ip='157.249.*.*' -e ostack_network_cidr='*.*.*.*/24' -e db_password=xxx -e repmgr_password=xxx configure.yml 

### Connect to database
PGPASSWORD=xxx psql -h 157.249.*.* -p 5432 -U lard_user -d lard

### Perform switchover
ansible-playbook -e name_primary=lard-a -e primary_ip='157.249.*.*' -e name_standby=lard-b -e standby_ip='157.249.*.*' -e primary_floating_ip='157.249.*.*' switchover.yml

