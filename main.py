import os
from pathlib import Path
from src.core.engine import ConfigDefaultMapping, ConfigDefaultConvert

os.environ['PROJ_PATH'] = str(Path(__file__).parent)


def test_pg_db():
    # config01 = ConfigDefaultMapping('catalog_pg_customer', 'catalog', 'pg')
    # print(config01.model)
    # for name, detail in config01.model.schemas.items():
    #     print(f"{name}: {detail.details}")
    # print("*" * 150)
    # config02 = ConfigDefaultMapping('catalog_pg_billing', 'catalog', 'pg')
    # print(config02.model)
    # for name, detail in config02.model.schemas.items():
    #     print(f"{name}: {detail.details}")
    print("*" * 150)
    config03 = ConfigDefaultMapping('catalog_pg_sales', 'catalog', 'pg')
    # for name, detail in config03.model.schemas.items():
    #     print(f"{name}: {detail.details}")
    print(config03.model.schemas)
    # print(type(config03.model))


def test_local_csv():
    config01 = ConfigDefaultMapping('catalog_file_customer', 'catalog', 'csv')
    print("*" * 150)
    print(config01.model.ps_sub_path)
    print(config01.model.ps_file_name)
    print("*" * 150)


if __name__ == '__main__':
    # test_pg_db()
    test_local_csv()
