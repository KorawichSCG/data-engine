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

- Define the standard connection url of data source (for `fsspec`)
  
    - file url `protocal://username/path/subpath/filename.extention`
      
      Window: `file:///C:/folder/sub-folder/file.extention`
    
      S3: `s3://bucket/path/`
      
    - database url `protocal://username:password@host:port/database`
    
      Postgresql: `postgresql://username:password@localhost/database`
    
      Mysql: `mysql+pymysql://username:password@host/database?charset=utf8mb4`
      
      Mongodb: `mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/?authSource=admin`
    
      Mongodb Cluster: `mongodb://username:password@example.com:27017,example2.com:27017,...,example.comN:27017/database?key=value&keyN=valueN`
    
    Note: If the username or password includes the following characters: 
    ```
    : / ? # [ ] @
    ```
    those characters must be converted using [percent encoding](https://datatracker.ietf.org/doc/html/rfc3986#section-2.1).
    
    Reference: https://github.com/mongodb/specifications/blob/master/source/connection-string/connection-string-spec.rst
  

Which data do you want?

How do you use this data?

----

The process is mapping of catalog and node, that mean head and tail of process are catalogs.

- catalog-input ==> node ==> catalog-output

So we should define parameters transfer between walking of process.

Dependency Lib
--------------

- [fsspec](https://github.com/fsspec/filesystem_spec) - for connect all file systems such as local, s3, gcs

- postgresql: `psycopg` or `psycopg3` (We do not use `aiopg` - asyncio client for PostgreSQL because this lib base on `psycopg2`)
- mysql/mariadb: `aiomysql` - asyncio client form MySQL
- sqlite: `supersqlite`
- mssql: `pyodbc`




Reference
---------

- README: https://github.com/pyenv/pyenv/blob/master/README.md

Data Framework

- Kedro: https://kedro.readthedocs.io/en/stable/01_introduction/01_introduction.html
- Metorikku: https://github.com/YotpoLtd/metorikku
