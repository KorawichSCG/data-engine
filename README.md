data-engine
===========

The python code for explore and develop the data catalog engine that easy to plug-in any data source with catalog configuration which
show how data drive end-to-end.

The definition of core engine is class registries of system connection.
So the core is the base class of all probably datasets.

---

concept
-------

Concept base on one file config will define all system

- What data we serve?

- Where is data?

- Define the standard connection url of data source (for `fsspec`)

    - file url `protocal://username/path/subpath/filename.extention?param=value`

      Window: `file:///C:/folder/sub-folder/file.extention`

      Linux: `file://username/directory/sub-dir/file.extention`

      S3: `s3://bucket/path/`

    - database url `protocal://username:password@host:port/database?param=value`

      Postgresql: `postgresql://username:password@localhost/database`

      Mysql: `mysql+pymysql://username:password@host/database?charset=utf8mb4`

      Mongodb: `mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/?authSource=admin`

      Mongodb Cluster: `mongodb://username:password@example.com:27017,example2.com:27017,...,example.comN:27017/database?key=value&keyN=valueN`

    Note: If the username/password or path includes the following characters:
    ```
    : / ? # [ ] @
    ```
    those characters must be converted using [percent encoding](https://datatracker.ietf.org/doc/html/rfc3986#section-2.1).

    Reference: https://github.com/mongodb/specifications/blob/master/source/connection-string/connection-string-spec.rst


Which data do you want?

How do you use this data?

----

The process is mapping of catalog and node, that mean head and tail of process are catalogs.

- catalog-input ==> catalog-node ==> catalog-output

So we should define parameters transfer between walking of process.


Example Scenario
--------------------

1) Get the requirement for get 3 files of data source ingest to the Postgres database
    - customer.csv
    - billing.csv
    - product.json

2) Prepare billing staging table filter value more than 0
    - customer_prepare
    - billing_prepare
    - product_prepare

3) Transform to data warehouse model
    - billing_fact_table
    - customer_dim_table
    - product_dim_table

4) Aggregate to data mart for data science or data analytic
    - billing_cus_monthly
    - billing_cus_weekly
    - billing_prod_monthly
    - billing_prod_weekly

Dependency Lib
--------------

- [fsspec](https://github.com/fsspec/filesystem_spec) - for connect all file systems such as local, s3, gcs

- postgresql: `psycopg` or `psycopg3` (We do not use `aiopg` - asyncio client for PostgreSQL because this lib base on `psycopg2`)

- mysql/mariadb: `aiomysql` - asyncio client form MySQL

- sqlite: `supersqlite`

- mssql: `pyodbc`

more details: [core engine](src/core/README.md)


Reference
---------

- README: https://github.com/pyenv/pyenv/blob/master/README.md

Data Framework

- Kedro: https://kedro.readthedocs.io/en/stable/01_introduction/01_introduction.html
- Metorikku: https://github.com/YotpoLtd/metorikku


License
-------

This project base under MIT license
