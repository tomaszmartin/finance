# Todo

Things that need to be done when starting a project:
* add user using `airflow users create -e {email} -p {password} -r Admin -u {username} -f {firstname} -l {lastname}`
* add Google Cloud connection in Airflow UI or with command
    ```
    airflow connections -a \
    --conn_id=<CONNECTION_ID> \
    --conn_type=google_cloud_platform \
    --conn_extra='{ "extra__google_cloud_platform__key_path":" '`
            `'<GCP_CREDENTIALS_ABSOLUTE_PATH.json>", '`
        `'"extra__google_cloud_platform__project": '`
            `'"<GCP_PROJECT_NAME>", '`
        `'"extra__google_cloud_platform__scope":  '`
            `'"https://www.googleapis.com/auth/cloud-platform"}'
    ```

## Ansible

`hosts.ini` file should have following format:

```
[cloud]
sandbox ansible_host={ip} ansible_ssh_user={ssh user} ansible_ssh_private_key_file={ssh key}
```