import os
import pandas as pd
import core.engine.log_parser
from core.engine.log_parser import setup_log
from src.core.engine.config_control import ConfigDefaultMapping
from src.core.io.path_url import path_join

PROJ_PATH = os.environ.get('PROJ_PATH', path_join(os.path.dirname(__file__), '../../../..'))
setup_log(path_join(PROJ_PATH, 'conf/logging.yaml'))
logger = core.engine.log_parser.get_logger(__name__)

pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)


def test_pg_db():
    config01 = ConfigDefaultMapping('catalog_pg_customer', 'catalog', 'pg')
    print(config01.model)
    for name, detail in config01.model.schemas.items():
        print(f"{name}: {detail.details}")
    print("*" * 150)
    config02 = ConfigDefaultMapping('catalog_pg_billing', 'catalog', 'pg')
    print(config02.model)
    for name, detail in config02.model.schemas.items():
        print(f"{name}: {detail.details}")
    print("*" * 150)
    config03 = ConfigDefaultMapping('catalog_pg_sales', 'catalog', 'pg')
    # for name, detail in config03.model.schemas.items():
    #     print(f"{name}: {detail.details}")
    print(config03.model.schemas)
    # print(type(config03.model))


def test_local_csv():
    config01 = ConfigDefaultMapping.load(
        'catalog.catalog_file_customer:csv',
        ext_params={'run_date': '2022-03-25 00:00:00'}
    )
    print("-" * 150)
    for k, v in config01.model.schemas.items():
        print(f"{k} = {v.details}")
    print("-" * 150)
    print(config01.model.df)
    config02 = ConfigDefaultMapping.load(
        'catalog.catalog_file_billing:csv',
        ext_params={'run_date': '2022-03-26 00:00:00'}
    )
    print("-" * 150)
    for k, v in config02.model.schemas.items():
        print(f"{k} = {v.details}")
    print("-" * 150)
    print(config02.model.df)


def test_local_excel():
    config01 = ConfigDefaultMapping.load(
        'catalog.catalog_file_promotion:excel',
        ext_params={'run_date': '2022-03-25 00:00:00'}
    )
    for k, v in config01.model.schemas.items():
        print(f"{k} = {v.details}")
    print("-" * 150)
    print(config01.model.df)


def test_local_json():
    config01 = ConfigDefaultMapping.load(
        'catalog.catalog_file_product:json',
        ext_params={'run_date': '2022-03-01 00:00:00'}
    )
    for k, v in config01.model.schemas.items():
        print(f"{k} = {v.details}")
    print("-" * 150)
    print(config01.model.df)


if __name__ == '__main__':
    logger.info(f"Start main process in project: {PROJ_PATH}")
    # logger.info("*" * 100)
    # test_pg_db()
    logger.info("*" * 100)
    test_local_csv()
    # logger.info("*" * 100)
    # test_local_excel()
    # logger.info("*" * 100)
    # test_local_json()
