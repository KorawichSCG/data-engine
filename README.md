data-engine
===========

The python code for explore and develop data engine that easy to plug-in data source with configuration

We define core engine for create the main class of connection to data source first. 
So the core is the base class of all datasets.

Concept base on one file config will define all system

- Models - Define data profile

- ?? - Define process of models

---

concept
-------

What data we serve?

Where is data?

Which data do you want?

How do you use this data?

----

The process is mapping of catalog and node, that mean head and tail of process are catalogs.

- catalog-input ==> node ==> catalog-output

So we should define parameters transfer between walking of process.

Dependency Lib
--------------

- postgresql: `psycopg` or `psycopg3` (We do not use `aiopg` - asyncio client for PostgreSQL because this lib base on `psycopg2`)
- mysql/mariadb: `aiomysql` - asyncio client form MySQL
- sqlite: `supersqlite`
- mssql: `pyodbc`

Reference README: https://github.com/pyenv/pyenv/blob/master/README.md