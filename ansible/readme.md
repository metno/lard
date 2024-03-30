https://gitlab.met.no/it/infra/ostack-ansible21x-examples

https://gitlab.met.no/it/infra/ostack-doc/-/blob/master/ansible-os.md?ref_type=heads

#### Useful ansible commands:
ansible-inventory -i openstack.yml --graph

ansible-galaxy collection install ansible.posix

ansible-galaxy collection install community.postgresql

### Create network and security group
This file is encrypted with ansible-vault (ansible-vault decrypt create-project-network.yml create-project-security-group.yml)

*But if this has been setup before in the ostack project, these have likely already been run and therefore already exits.*

ansible-playbook -i openstack.yml -e ostack_cloud=lard create-project-network.yml

ansible-playbook -i openstack.yml -e ostack_cloud=lard create-project-security-group.yml

### Create 1 VM with an existing floating ip (that way its easier to set it one that maybe we want to reuse), then create and attach a volume 
#### (TODO: expand to 2, with one in A and one in B in one script?) 
ansible-playbook -i openstack.yml -e ostack_cloud=lard -e availability_zone=ext-a -e ostack_key_name=louiseo-yubikey -e name_stuff=lard-a -e vm_ip='157.249.*.*' create-project-vm.yml

ansible-playbook -i openstack.yml -e ostack_cloud=lard -e availability_zone=ext-b -e ostack_key_name=louiseo-yubikey -e name_stuff=lard-b -e vm_ip='157.249.*.*' create-project-vm.yml

### Format and mount the volume
#### for now have to check where it got mounted in the ostack gui, as well as get the VMs assigned floating IP
#### Do this for both VMs
ansible-playbook -i openstack.yml -e ostack_cloud=lard -e vm_ip='157.249.*.*' -e mount_point='/dev/vdb' format-mount-disk.yml 

### Install postgres, move data to the mount...
#### Do this for both VMs
ansible-playbook -i openstack.yml -e ostack_cloud=lard -e vm_ip='157.249.*.*' -e repmgr_password='xxx' install-postgres.yml

### Turn one of the VMs into a primary
#### involves creating the lard db, and the schema
ansible-playbook -i openstack.yml -e ostack_cloud=lard -e vm_ip='157.249.*.*' -e name_stuff=lard-a -e standby_host='157.249.*.*'  -e db_password='xxx' primarystandbysetup/primary_new.yml

### Turn the other VM into a standby / replica (assumes the primary exists, and that IP must also be passed)
#### using pg_basebackup
ansible-playbook -i openstack.yml -e ostack_cloud=lard -e vm_ip='157.249.*.*' -e name_stuff=lard-b -e primary_host='157.249.*.*' -e db_password='xxx' primarystandbysetup/standby_new.yml

### Connect to database
PGPASSWORD=xxx psql -h 157.249.*.* -p 5432 -U lard_user -d lard

### Share SSH keys
Run the share ssh keys ansible script each way (so that each vm shares to the other)

### Perform switchover
ansible-playbook -i openstack.yml -e ostack_cloud=lard -e standby_ip='157.249.*.*' -e primary_ip='157.249.*.*' primarystandbysetup/switchover_promote_standby.yaml
