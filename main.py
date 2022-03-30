import os
import src.core.io.log_parser
from src.core.engine.config_control import ConfigDefaultMapping
from src.core.io.path_utils import path_join

PROJ_PATH = os.environ.get('PROJ_PATH', path_join(os.path.dirname(__file__), '../../../..'))
src.core.io.log_parser.setup_log(path_join(PROJ_PATH, 'conf/logging.yaml'))
logger = src.core.io.log_parser.get_logger(__name__)


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
    for k, v in config01.model.schemas.items():
        print(f"{k} = {v.details}")
    # print("*" * 150)


if __name__ == '__main__':
    # test_pg_db()
    test_local_csv()
    # print(PROJ_PATH)
