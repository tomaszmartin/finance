# About the project

This project contains a set of Airflow DAGs that extract information about multiple financial instruments.

It's main purpose is to provide a data that will be later used as a backend for mobile application: 

* Polish stock market equities and indices prices.
* Information about companies traded on Polish stock exchange.
* Currency exchange rates.
* Cryptocurrency exchange rates.

# DAGs

Data is extracted using Airflow DAGs, which utilize 3 different sources:

* Official Polish Stock Exchange [website](https://www.gpw.pl).
* [exchangerate.host API](https://exchangerate.host/#/) for currency exchange rates.

There are following DAGs defined in the project:
* **Equities** data from from [offical website](https://www.gpw.pl)
    * Historical prices are saved in BigQuery.
    * Current prices are saved in Firestore.
    * Dimensions are saved in BigQuery table.
* **Indices** data from from [offical website](https://www.gpw.pl)
    * Historical prices saved in BigQuery table.
    * Current prices are saved in Firestore.
* **Currencies** from [exchangerate.host API](https://exchangerate.host/#/)
    * Historical exchange rates saved in BigQuery table.
    * Current exchange rates saved in Firestore.

There are three components used in terms of storing data:

* Google Cloud Storage which serves as a Data Lake.
* BigQuery that works as a data warehouse solution for historical data, which is best suited for analysis and can handle large amounts of data very efficiently.

Solution used for executing DAGs is Airflow with LocalExecutor, since the amount of data running through the pipeline is manageable by a single machine. Airflow is used since it's a great tool for orchestrating tasks, scheduling them and observing the results.

The logic of DAGs is more or less similar:
* Data is extracted and stored in Data Lake `raw zone` in a format as close to the original as possible.
* Data is transformed/parsed and saved in Data Lake `master zone`, usually in `json new line` format.
* Data is then stored in the destination Database.
* There is some simple validation of the data.

# Data model

The final data model is divided into three datasets:
* `gpw` dataset that contains GPW data in four different tables:
    * historical `equities` prices.
    * historical `indices` prices.
    * `dim_equities_indicators` containing indicators information about the equities.
    * `dim_equities_info` containing information about the equities.
* `currencies` dataset containing currencies data:
    * historical exchange `rates`.
* `cryptocurrencies` containing crypto data:
    * historical `rates` of most popular coins.

In each DAG definition there is a schema for each of the tables, specified in `config.py` files in DAGs directory.

Star schema is used throughout the project. Facts are downloaded into `equities` and `indices` tables. Dimension tables are stored as a slowly changing dimensions since some of them is bound to change periodically (`indicators` table changes every quarter) and some may change, like for example the market on which company operates.
This allows to always pick the current data for some analysis, but still allow track the historical changes as company changed through time.

For example it allows to see average price of companies in each segment using query:

```
SELECT dim.sector, ROUND(SUM(facts.closing_price)/count(facts.closing_price), 2) as avg_price
FROM gpw.equities as facts
JOIN gpw.dim_equities_indicators AS dim ON facts.date=dim.date AND facts.isin_code=dim.isin_code
WHERE facts.date = "2021-10-07"
GROUP BY dim.sector
ORDER BY avg_price DESC
```

Which returns the following results:

| market                     | avg_price |
|----------------------------|-----------|
| Pharmaceuticals wholesales | 939.0     |
| other - leisure facilities | 768.0     |
| clothes & footwear         | 725.65    |

# Running the project

## Local

Creating local environment:

1. Create virtual environment by running `python3.9 -m venv .env`.
2. Launch virtual environment using `source .venv` command.
3. Install Airflow `pip install apache-airflow`.
4. Install other dependencies `pip install -r requirements.txt`.
5. Initialize Airflow database `airflow db init` (just the first time).
6. Add admin user to Airflow `airflow users create --username user --password pass --firstname f --lastname l --email e --role Admin`.
7. Run `airflow standalone`.

All this steps have to be done for the first launch. Next runs will require steps 2, 4 (if You add new dependencies) and 7.

## Production

Production environment is still a simplified case. It builds and configures Google Cloud Compute Engine that hosts Airflow using SequentialExecutor and SQLite database as metastore. For real use case You would probably want to separate database and compute, change executor etc. but this provides the simplest working setup.

This environment is built using three Ansible playbooks - one for provisioning, another for configuration of the environment, and final for deploying the app. Before You run any of the playbooks You need to add some local files:

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

## Connections

After deployment You should add connections to the Airflow:
* Google Cloud Platform connection named `google_cloud`:
    * Keyfile JSON: contents of `json` key for Service Account with permission to BigQuery and Google Cloud Storage
* HTTP connection for [CoinAPI](https://www.coinapi.io):
    * Host: `rest.coinapi.io`
    * Schema: `https`
    * Extra: `{"X-CoinAPI-Key": "APIKEY"}`
