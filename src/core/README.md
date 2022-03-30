core-engine
===========

The `core` of data engine keeps main source code library that would not change anything when use it.

Now, I do not have idea to define the name of `core`

```
[core]
 ├── [engine]
 │  ├── __init__.py
 │  ├── config_control.py
 │  └── errors.py
 │
 ├── [io]
 │  ├── [database]
 │  │  ├── [plugins]
 │  │  ├── __init__.py
 │  │  └── postgresql_obj.py
 │  │
 │  └── [dataframe]
 │     ├── __init__.py
 │     ├── file_obj.py
 │     └── pandas_obj.py
 │
 ├── [tests]
 ├── [utils]
 ├── CHANGELOG.md
 └── README.md
```

- `io` - (Input/Output directory)
- `utils` - (Utilities directory)

### Type Define

#### Design phase
- `core.io.database.PostgresObj.TableDataset`
- `core.io.database.Postgres.TableDataset`
- `core.io.database.PostgresTable`
- `core.io.postgres.TableObject`
- `core.io.postgres.MeterializeViewDataset`
- `core.io.dataframe.pandas.csvDataset`
- `core.io.dataframe.pandas.parquetDataset`
- `core.io.dataframe.pandas.ParquetDataset`
- `core.io.dataframe.pyspark.ParquetDataset`
- `core.io.pyspark.ParquetDataset`

- `core.engine.node.PostgrestStatement`

#### Current phase
- `core.io.database.PostgresTable`
- `core.io.database.PostgresView`
- `core.io.database.PostgresMeterializeView`
- `core.io.database.PostgresFunction`
- `core.io.database.PostgresProcedure`

- `core.io.dataframe.PandasCSVFrame`
- `core.io.dataframe.PandasExcelFrame`
- `core.io.dataframe.PandasJsonFrame`
- `core.io.dataframe.PandasParquetFrame`

---

Database
--------
- Postgres SQL (`psycopg`)

DataFrame
---------
Order by data 1,000,000,000 rows x 9 columns

- datatable (`datatable`)
- cudf (`cudf`)
- Polars DataFrame (`pypolars`)
- Pyspark DataFrame (`pyspark`)
- Pandas DataFrame (`pandas`)

reference: https://h2oai.github.io/db-benchmark/