# About the project

This project contains a set of Airflow DAGs that extract information about multiple financial instruments.

It's main purpose is to provide a data that will be later used as a backend for mobile application: 

* Polish stock market equities and indices prices.
* Information about companies traded on Polish stock exchange.
* Currency exchange rates.
* Cryptocurrency exchange rates.

# DAGs

Data is extracted using Airflow DAGs, which utilize 3 different sources:

* Official Polsih Stock Exchange [website](https://www.gpw.pl).
* [exchangerate.host API](https://exchangerate.host/#/) for currency exchange rates.
* [CoinAPI](https://www.coinapi.io) for cryptocurrency data.

There are following DAGs defined in the project:
* **Equities** 
    * Historical prices that are extracted from [offical website](https://www.gpw.pl), first saved in `html` format on Google Cloud Storage, and then parsed and saved in BigQuery table.
    * Current prices that are extracted from [offical website](https://www.gpw.pl), first saved in `html` format on Google Cloud Storage, and then parsed and saved in Firestore.
    * Informations about the equities, first saved in `html` format on Google Cloud Storage, and then parsed and saved in BigQuery table.
* **Indices** 
    * Historical prices that is extracted for execution date, first saved in `html` format on Google Cloud Storage, and then parsed and saved in BigQuery table.
    * Current prices that are extracted from [offical website](https://www.gpw.pl), first saved in `html` format on Google Cloud Storage, and then parsed and saved in Firestore.
* **Currencies**
    * Exchange rates downloaded from [exchangerate.host API](https://exchangerate.host/#/), first saved in `json` format on Google Cloud Storage, and then saved in BigQuery table.
    * Current prices that are extracted from [exchangerate.host API](https://exchangerate.host/#/), first saved in `json` format on Google Cloud Storage, and then parsed and saved in Firestore.
* **Cryptocurrencies**
    * Prices of Cryptocurrencies from [CoinAPI](https://www.coinapi.io), first saved in `json` format on Google Cloud Storage, and then saved in BigQuery table.
    * Current prices that are extracted from [CoinAPI](https://www.coinapi.io), first saved in `json` format on Google Cloud Storage, and then parsed and saved in Firestore.

There are three components used in terms of storing data:

* Google Cloud Storage with serves as a data lake.
* BigQuery that works as a data warehouse solution for historical data, which is best suited for analysis and can handle large amounts of data very efficiently.
* Firestore that holds current data and works as a mobile backend. Only the most fresh data is kept inside Firestore so the size wof data kept their will stay small on more or less the same level.

# Data model

The final data model has the following

# Running the project

## Local

Creating local environment:

1. Create virtual environment by running `python3.9 -m venv .env`.
2. Launch virtual environment using `source .venv` command.
3. Install Airflow `pip install apache-airflow`.
4. Install other dependencies `pip install -r requirements.txt`.
5. Initialize Airflow database `airflow db init` (just the first time).
6. Add admin user to Airflow `airflow users create --username user --password pass --firstname f --lastname l --email e --role Admin`.
7. Run `airflow webserver` and `airflow scheduler`.
8. Local structure should mimic the behaviour in production.

All this steps have to be done for the first launch. Next runs will require steps 2, 4 (if You add new dependencies) and 7.

## Production

Production environment is still a simplified case. It builds and configures Google Cloud Compute Engine that hosts Airflow using SequentialExecutor and SQLite database as metastore. For real use case You would probably want to separate database and compute, change executor etc. but this provides the simplest working setup.

This envornment is built using three Ansible playbooks - one for provisioning, another for configuration of the environment, and final for deploying the app. Before You run any of the playbooks You need to add some local files:

* Create local `secrets` directory.
* Add `secrets/gcp.json` file with Service Account key to the GCP project where You want to build the environment.
* Add `secrets/ssl.cert` and `secrets/ssl.key` files for securing Airflow webserver. You can generate them on Your own with following command `openssl req -newkey rsa:2048 -nodes -keyout ssl.key -x509 -days 365 -out ssl.crt`.
* Create `secrets/vars.yml` file with following following structure:

```
---
ansible_ssh_user: username
ansible_ssh_public_key_file: ~/.ssh/id_rsa.pub
ansible_ssh_private_key_file: ~/.ssh/id_rsa

airflow_fernet_key: genrated fernet key
airflow_db_path: "{{ app_home }}/airflow.db"
airflow_db_conn: "sqlite:////{{airflow_db_path}}"
airflow_admin: username
airflow_pass: password
airflow_email: email

gcp_project: project
```

See the meaning of the variables below:
* `ansible_ssh_user` is Your ssh username corresponding to the ssh key. `ansible_ssh_public_key_file` and `ansible_ssh_private_key_file` are the locations of Your ssh key files.
* `airflow_fernet_key` is the fernet key used to encrypt airflow metastore, more info [here](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html).
* `airflow_db_path` is the path to SQLite database. This can stay the same.
* `airflow_db_conn` is the SQLAlchemy connection url. If SQLite is used it should stay the same.
* `airflow_admin`, `airflow_pass` and `airflow_email` are information about the Airflow user that will allow to login to Airflow webserver.
* `gcp_project` name of the Google Cloud Platform project where app will live.

After adding these files You can:

* Build the environment (Compute Engine instance etc.) by running `ansible-playbook provision.yml`.
* Configure the environment `ansible-playbook configure.yml -i hosts.ini`.
* Deploy the app by running `ansible-playbook deploy.yml -i hosts.ini`.

After deployment You should add connections to the Airflow:
* Google Cloud Platform connection named `google_cloud`:
    * Kefile JSON: contents of `json` key for Service Account with permission to BigQuery and Google Cloud Storage
* HTTP connection for [CoinAPI](https://www.coinapi.io):
    * Host: `rest.coinapi.io`
    * Schema: `https`
    * Extra: `{'X-CoinAPI-Key' : 'APIKEY'}`
