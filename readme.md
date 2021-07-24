# Todo

Things that need to be done when starting a project:
* add user using `airflow users create -e {email} -p {password} -r Admin -u {username} -f {firstname} -l {lastname}`
* add Google Cloud connection in Airflow UI

## Ansible

`hosts.ini` file should have following format:

```
[cloud]
sandbox ansible_host={ip} ansible_ssh_user={ssh user} ansible_ssh_private_key_file={ssh key}
```

## Provisioning

* Instance, Ubuntu 20, firewall rule, static ip, public key for ssh
* Firewall rules
* Static IP
* Database
