from datasets.models import AITableObject, AIDataFrameObject
# from .datasets.pipelines import SQLStatementNode, PyFunctionNode
from typing import Optional, Union, Set, Dict, Any, List
from core.engine import ConfigConverter

PROJ_PATH = r'D:\korawica\Work\dev02_miniproj\GITHUB\data-engine'
PROJ_ENV = 'sandbox'


class ConfigModelParser(ConfigConverter):
    """
    Mapping models configuration that default type
    for `datasets.models.AITableObject`
    """
    CONF_SUB_PATH: str = 'models'
    CLASS_VALIDATE: List[object] = [
        AITableObject, AIDataFrameObject
    ]


# class ConfigPipelineParser(ConfigConverter):
#     """
#     Mapping pipelines configuration that default type
#     for `datasets.pipelines.SQLStatementNode`
#     """
#     CONF_SUB_PATH: str = 'pipelines'
#     CLASS_VALIDATE: List[object] = [
#         SQLStatementNode, PyFunctionNode
#     ]


if __name__ == '__main__':
    model3 = ConfigModelParser('catalog_pg_db_customer', 'catalog', 'db')
    print("=" * 100)
