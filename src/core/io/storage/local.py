import os
import re
import itertools
from pathlib import Path
from typing import Any, Dict, Union, Optional
from src.core.utils import path_join, str_to_bool
from src.core.io import parse_config, load_dotenv

PROJ_PATH = path_join(Path(__file__).parent, '../../../..')
load_dotenv(path_join(PROJ_PATH, 'conf'))


class CSVColumn:
    """
    CSV column object
    """
    def __init__(
            self,
            ps_col: Union[str, Dict[str, Any]]
    ):
        self.ps_col_datatype: Optional[str] = None
        self.ps_col_nullable: Optional[str] = None
        self.ps_col_default: Optional[str] = None
        self.ps_col_desc: Optional[str] = None
        if isinstance(ps_col, str):
            self.convert_from_string(ps_col)
        self.convert_from_mapping(ps_col)

    def convert_from_string(self, _ps_col: str):
        """
        Convert column statement form string to standard mapping
        example
        -------
            (i)    order_sales_value: "string"
        """
        pass

    def convert_from_mapping(self, _ps_col: Dict[str, Any]):
        """
        Mapping schemas with dictionary
        example
        -------
        (i)   customer_id:
                    datatype: "string"
                    nullable: "false"
                    default: ""
        """
        pass

    def __str__(self):
        return f'{self.ps_col_datatype} {("null" if self.ps_col_nullable else "not null")}'

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ps_col_datatype})'

    @property
    def datatype(self):
        return self.ps_col_datatype

    @property
    def nullable(self):
        return self.ps_col_nullable

    @property
    def default(self):
        return self.ps_col_default

    @property
    def desc(self):
        return self.ps_col_desc

    @property
    def details(self):
        return {
            'datatype': self.ps_col_datatype,
            'nullable': self.ps_col_nullable,
            'default': self.ps_col_default,
            'description': self.ps_col_desc
        }


class CSVFile:
    """
    CSV files object
    """
    def __init__(
            self,
            server_conn: str,
            sub_path: str,
            file_name: str,
            file_type: str,
            **kwargs
    ):
        self.server_conn = server_conn
        self.sub_path: str = sub_path
        self.file_name: str = file_name
        self.file_type: str = file_type

    @property
    def columns(self):
        pass

    def read(self):
        pass


class LocalCSVFile(CSVFile):
    """
    Local csv files object
    config
    ------
        <files-alias-name>:
            type: io.datasets.LocalCSVFile
            properties:
                catalog_name: <files-name>
                schemas:
                    (i)   <column-name>:
                                datatype: <datatype>
                                ...

                    (ii)  <column-name>: <datatype> []
                          ...

                header:
                encoding:
                ...
            retentions:
                ...
    example
    -------
        (i)   catalog_file_customer:
                    type: io.datasets.LocalCSVFile
                    properties:

    """
    CONF_DELIMITER = os.path.sep
    CONF_NAME_DELIMITER = ':'
    ROOT_PATH = os.environ['PROJ_PATH']
    SUB_PATH = f'/files'

    def __init__(
            self,
            catalog_name: str,
            properties: Dict[str, Any],
            **kwargs
    ):
        # DEBUG: debug arguments
        print("Start convert config file ...")
        print(f"{catalog_name=}")
        print("properties= ...")
        for k, v in properties.items():
            print(f"{k}: {v}")

        self.ps_server_conn = 'file'
        self.ps_cat_name: list = [properties.pop('catalog_name', catalog_name)]

        # DEBUG: path of root
        print(Path(self.ROOT_PATH, *self.ps_cat_name).parts)
        print(Path(Path(self.ROOT_PATH, *self.ps_cat_name).parts))

        self.ps_file_name: str = self.ps_cat_name.pop(-1)
        self.ps_sub_path: str = self.ps_cat_name.pop(-1) if self.ps_cat_name else self.SUB_PATH
        self.ps_file_type: str = properties.pop('catalog_name', catalog_name)

        # Properties for Postgres table
        self.ps_file_header: bool = str_to_bool(properties.pop('header', False))
        self.ps_file_encoding: str = properties.pop('encoding', 'utf-8')

        # Optional arguments for Postgres table
        self.ps_file_retentions: Optional[Dict[str, Any]] = kwargs.pop('retentions', {})

        super(LocalCSVFile, self).__init__(
            self.ps_server_conn,
            self.ps_sub_path,
            self.ps_file_name,
            self.ps_file_type
        )

    def _schemas(self):
        pass

    @property
    def schemas(self):
        pass

    @staticmethod
    def get_str_or_list(props, key) -> list:
        return _return_key if isinstance((_return_key := props.pop(key, [])), list) else [_return_key]