core-engine
===========

The `core` of data engine keeps main source code library that would not change anything when use it.

Now, I do not have idea to define the name of `core`

```
[core]
 ├── [engine]
 │  └── [plugins]
 │
 ├── [io]
 │  └── [plugins]
 │
 ├── [utils]
 ├── [tests]
 ├── CHANGELOG.md
 └── README.md
```

- `io` - (Input/Output directory)
- `utils` - (Utilities directory)

### Type Define

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

---

Database
--------
- Postgres SQL

DataFrame
---------
- datatable (`datatable`)
- cudf (`cudf`)
- Polars DataFrame (`pypolars`)
- Pyspark DataFrame (`pyspark`)
- Pandas DataFrame (`pandas`)

Order by data 1,000,000,000 rows x 9 columns

reference: https://h2oai.github.io/db-benchmark/