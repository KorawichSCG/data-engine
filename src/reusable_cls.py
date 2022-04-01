import os
import fnmatch
import yaml
from pathlib import Path

"""
create class for mapping data-source instance with configuration and target
concept
-------
    (i) pack code config: for compress storage of config files when send to deploy another system
        diagram
        -------
        config.yaml -> compiler -> config.byte.yaml -> interpreter -> config.yaml

The source config document
    connections := connections target that sync information with config
        Note: one connections for many profiles
    profile := use for create source in connections target
        Note: one profile for many ?? (load/save)

config
------
<source-name>:
    connections:
        format: <format-type>
        host: <host-name>
        port: <port>
        username: <username>
        password: <password>

    profile:
        path: <database-name>:<schema-name>:<table-name>/<files-path>:<files-sub-path>:<files-name>.<files-extension>
        database_name/file_path: <database-name>/<files-path>
        schema_name/file_sub_path: <schema-name>/<files-sub-path>
        table_name/file_name: <table-name>/<files-name>.<files-extension>
        features:
            <column-name>:
                datatype: <datatype>
                nullable: <nullable> (optional)
                check: <check> (optional)
                default: <default> (optional)
            <column-name>: <datatype> [<nullable>, ...]
            ...
        primary_key: [<column-name>, ...] (optional)
        foreign_key:
            <column-name>: <reference-table-name> <reference-column-name> (optional)
            ...
        unique: [<column-name>, ...] (optional)

    load:
        mode: [full_load, transaction]
        stream: <stream-flag>
        incremental: <incremental-flag>
        look_back: <look_back_data_rolling>
        ...
    save:
        mode: [append/transaction, overwrite/full_load, update/merge/delta, slowly_change_dimension/scd, ...]
        stream: <stream-flag>
        incremental: <incremental-flag>
        look_back: <look_back_data_rolling>
        ...

example
-------
ai_article_master:
    connections:
        format: postgresql
        path: scgh_prod_db:ai:ai_article_master
        load:
        save:
ai_actual_sales_transaction:
    connections:
        format: s3
        path: data/:actual_sales_transaction/YYYY/MM/DD:actual_sales_transaction_YYYYMMDD.csv
        load:
        save:
"""
AI_APP_PATH: str = os.getenv('AI_APP_PATH', os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))


class fileObject:
    """
    files object
        i/o mapping with read
        `mmap` mapping with memory
        reference: https://realpython.com/python-mmap/
    example
    -------
        import mmap

        def regular_io_find(filename):
            with open(filename, mode="r", encoding="utf-8") as file_obj:
                text = file_obj.read()
                print(text.find(" the "))

        def mmap_io_find(filename):
            with open(filename, mode="r", encoding="utf-8") as file_obj:
                with mmap.mmap(file_obj.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_obj:
                    print(mmap_obj.find(b" the "))
    """
    pass


if __name__ == '__main__':
    pass
