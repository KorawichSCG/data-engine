from datasets.models import AITableObject, AIDataFrameObject
# from .datasets.pipelines import SQLStatementNode, PyFunctionNode
from typing import Optional, Union, Set, Dict, Any, List
from core.engine import ConfigConvert

PROJ_PATH = r'D:\korawica\Work\dev02_miniproj\GITHUB\data-engine'
PROJ_ENV = 'sandbox'


class ConfigModelParser(ConfigConvert):
    """
    Mapping models configuration that default type
    for `datasets.models.AITableObject`
    """
    CONF_SUB_PATH: str = 'models'
    CLASS_VALIDATE: List[object] = [
        AITableObject, AIDataFrameObject
    ]


# class ConfigPipelineParser(ConfigConvert):
#     """
#     Mapping pipelines configuration that default type
#     for `datasets.pipelines.SQLStatementNode`
#     """
#     CONF_SUB_PATH: str = 'pipelines'
#     CLASS_VALIDATE: List[object] = [
#         SQLStatementNode, PyFunctionNode
#     ]
