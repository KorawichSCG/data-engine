import os
import pandas as pd
import core.utils.log_parser
from src.core.utils.log_parser import setup_log
from src.core.engine.config_control import ConfigDefaultMapping
from src.core.io.path_utils import path_join

PROJ_PATH = os.environ.get('PROJ_PATH', path_join(os.path.dirname(__file__), '../../../..'))
setup_log(path_join(PROJ_PATH, 'conf/logging.yaml'))
logger = core.utils.log_parser.get_logger(__name__)

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
    # cls.load('catalog.catalog_name:csv'
    config01 = ConfigDefaultMapping.load(
        'catalog.catalog_file_customer:csv',
        external_parameters={'run_date': '2022-03-25 00:00:00'}
    )
    print("-" * 150)
    for k, v in config01.model.schemas.items():
        print(f"{k} = {v.details}")
    print("-" * 150)
    # print(config01.model.rows_num)
    print(config01.model.df.dtypes)
    print(config01.model._columns())
    # print(config01.model.file_founds)
    # print("*" * 150)
    # config02 = ConfigDefaultMapping(
    #     'catalog_file_billing',
    #     'catalog',
    #     'csv',
    #     external_parameters={'run_date': '2022-03-26 00:00:00'}
    # )
    # for k, v in config02.model.schemas.items():
    #     print(f"{k} = {v.details}")
    # print("-" * 150)
    # print(config02.model.df)


def test_local_excel():
    config01 = ConfigDefaultMapping(
        'catalog_file_promotion',
        'catalog',
        'excel',
        external_parameters={'run_date': '2022-03-25 00:00:00'}
    )
    for k, v in config01.model.schemas.items():
        print(f"{k} = {v.details}")
    print("-" * 150)
    print(config01.model.df)


def test_local_json():
    pass


if __name__ == '__main__':
    logger.info(f"Start main process in project: {PROJ_PATH}")
    # logger.info("*" * 100)
    # test_pg_db()
    logger.info("*" * 100)
    test_local_csv()
    # logger.info("*" * 100)
    # test_local_excel()
