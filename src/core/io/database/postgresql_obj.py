import os
import re
import itertools
from pathlib import Path
from typing import Any, Dict, Union, Optional
from src.core.utils import path_join, str_to_bool
from src.core.io import parse_config, load_dotenv
from .plugins.postgresql_plugin import (
    TableObject, ViewObject, MaterializedViewObject, FunctionObject, ProcedureObject
)

os.environ.setdefault('PROJ_PATH', path_join(Path(__file__).parent, '../../../..'))
load_dotenv(path_join(os.environ['PROJ_PATH'], 'conf'))
CONF_DB = parse_config(
    f'{os.environ["PROJ_PATH"]}/conf/config.yaml')['datasets'][f'postgresql.{os.environ["PROJ_ENV"]}']


class PostgresColumn:
    """
    Postgres column object
    config
    ------
        (i)   <column-name>:
                    datatype: <datatype>
                    unique: <False, [True, true, false]>
                    nullable (optional): <null, [not null]>
                    defaults (optional): <defaults>
                    check (optional): <check-statement>
                    primary_key (optional): <False, [True, true, false]>
                    foreign_key (optional): <reference_table(reference_column)>
                    comment (optional): <comment>

        (ii)  <column-name>: <datatype> [<nullable>, <default>, ..., --<comment>]
    """
    COL_CONSTS: set = {
        'unique', 'not null', 'null', 'primary key', 'references', 'constraint', 'check', 'default', '--'
    }

    __slots__ = 'ps_col_datatype', 'ps_col_unique', 'ps_col_nullable', 'ps_col_check', 'ps_col_desc', \
                'ps_col_primary_key', 'ps_col_foreign_key', 'ps_col_default'

    def __init__(
            self,
            ps_col: Union[str, Dict[str, Any]]
    ):
        self.ps_col_datatype: Optional[str] = None
        self.ps_col_unique: Optional[str] = None
        self.ps_col_nullable: Optional[bool] = None
        self.ps_col_check: Optional[str] = None
        self.ps_col_default: Optional[str] = None
        self.ps_col_desc: Optional[str] = None
        self.ps_col_primary_key: Optional[bool] = None
        self.ps_col_foreign_key: Optional[dict] = None
        if isinstance(ps_col, str):
            self.convert_from_string(ps_col)
        self.convert_from_mapping(ps_col)

    def convert_from_string(self, _ps_col: str):
        """
        Convert column statement form string to standard mapping
        example
        -------
            (i)     order_sales_value: "double precision"
            (ii)    customer_id: "varchar( 15 ) NOT NULL PRIMARY KEY --The customer ID"
            (iii)   email: "varchar( 30 ) UNIQUE NOT NULL CHECK(email like '%@%.com')"
            (iv)    salary: "numeric CONSTRAINT positive_salary CHECK(salary > 0)"
            (v)     birth_date: "date CHECK (birth_date > '1900-01-01')"
            (vi)    joined_date: "date NOT NULL CHECK (joined_date > birth_date)"
            (vii)   order_id integer NOT NULL DEFAULT nextval('tablename_colname_seq')
        """
        _ps_detail_lower = _ps_col.lower()
        match_list: list = re.split(f"({'|'.join(list(self.COL_CONSTS))})", _ps_detail_lower.strip())
        match_datatype: str = match_list.pop(0).strip()
        match_dict: dict = dict(itertools.zip_longest(*[iter(match_list)] * 2, fillvalue=""))
        for k in self.COL_CONSTS:
            if k in match_dict:
                # Set True when column constraint in {`not null`, `null`, `unique`, `primary key`}
                match_dict[k] = True if (value := match_dict[k].strip()) == '' else value
            else:
                match_dict[k] = None if k in {'default', 'check', 'constraint', '--'} else False
        match_dict['nullable'] = not not_null if (not_null := match_dict.pop('not null')) else True
        match_dict['datatype'] = match_datatype
        match_dict['description'] = match_dict.pop('--')
        match_dict['primary_key'] = match_dict.pop('primary key')
        match_dict['foreign_key'] = match_dict.pop('references')
        match_dict.pop('null')
        self.convert_from_mapping(match_dict)

    def convert_from_mapping(self, _ps_col: Dict[str, Any]):
        """
        Mapping schemas with dictionary
        example
        -------
        (i)   customer_id:
                   datatype: "varchar( 15 )"
                   nullable: "false"
                   primary_key: "true"
                   check: "customer_id like 'CM-%'"
                   description: "The customer ID"

        (ii)  customer_id:
                   datatype: "varchar( 15 )"
                   nullable: "false"
                   primary_key: "true"
                   check: "customer_id like 'CM-%'"
                   description: "The customer ID"
        """
        self.ps_col_datatype: str = _ps_col.pop('datatype', 'text')
        self.ps_col_unique: bool = str_to_bool(_ps_col.pop('unique', False))
        self.ps_col_nullable: bool = str_to_bool(_ps_col.pop('nullable', False))
        self.ps_col_primary_key: bool = str_to_bool(_ps_col.pop('primary_key', False))
        self.ps_col_check: str = _ps_col.pop('check', None)
        self.ps_col_default: str = _ps_col.pop('default', None)
        self.ps_col_desc: Optional[str] = _ps_col.pop('description', None)
        self.ps_col_foreign_key = _ps_col.pop('foreign_key', None)

    def __str__(self):
        return f'{self.ps_col_datatype} {("null" if self.ps_col_nullable else "not null")}'

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ps_col_datatype})'

    @property
    def datatype(self):
        # TODO: create convert function for split number of specific data type
        # TODO: such as varchar( 64 ) -> varchar, length 64 or numeric( 20, 6 ) -> numeric, precision 20, scale 6
        return self.ps_col_datatype

    @property
    def nullable(self):
        return self.ps_col_nullable

    @property
    def unique(self):
        return self.ps_col_unique

    @property
    def check(self):
        return self.ps_col_check

    @property
    def default(self):
        return self.ps_col_default

    @property
    def primary_key(self):
        return self.ps_col_primary_key

    @property
    def foreign_key(self) -> Dict:
        return self.ps_col_foreign_key

    @property
    def desc(self):
        return self.ps_col_desc

    @property
    def details(self):
        return {
            'datatype': self.ps_col_datatype,
            'unique': self.ps_col_unique,
            'nullable': self.ps_col_nullable,
            'primary_key': self.ps_col_primary_key,
            'foreign_key': self.ps_col_foreign_key,
            'check': self.ps_col_check,
            'default': self.ps_col_default,
            'description': self.ps_col_desc
        }


class PostgresTable(TableObject):
    """
    Postgres table object
    config
    ------
        <table-alias-name>:
            type: io.datasets.PostgresTable
            properties:
                catalog_name: <table-name>
                schemas:
                    (i)   <column-name>:
                                datatype: <datatype>
                                nullable (optional): <nullable>
                                check (optional): <check>
                                default (optional): <default>
                                comment (optional): <comment>
                                ...

                    (ii)  <column-name>: <datatype> [<nullable>, <default>, ...]
                          ...

                primary_key (optional): [<column-name>, ...]
                foreign_key (optional):
                    <column-name>: <reference-table-name>(<reference-column-name>)
                    ...
                unique (optional): [<column-name>, ...]
                constraints (optional):
                    <constraint-name>: <constraint-detail>
                    ...
                ...
            retentions:
                ...
    example
    -------
        (i)   customer_table:
                    type: 'io.datasets.PostgresTable'
                    columns:
                        customer_id:
                            datatype: 'serial'
                            nullable: 'False'
                            comment: 'The customer ID'
                        customer_name: 'varchar( 128 )'
                        customer_profile: 'varchar( 256 )'
                        register_date: 'date'
                        update_datetime: 'timestamp'
                    primary_key: ['customer_id']

        (ii)  order_transaction_table:
                    type: 'io.datasets.PostgresTable'
                    columns:
                        order_id: 'serial'
                        article_code: 'varchar( 128 )'
                        order_value: 'numeric( 20, 6 )'
                        order_quantity: 'double precision'
                        customer_owner: 'integer'
                        create_datetime: 'timestamp'
                    primary_key: ['order_id', 'article_code', 'customer_owner', 'create_datetime']
                    foreign_key:
                        customer_owner: 'customer_table(customer_id)'
                    unique: ['order_id']
    """
    # TODO: Change way to read config database connection with different environment
    CONF_DB = CONF_DB
    CONF_DELIMITER = '.'
    SCHEMA_NAME = 'public'

    def __init__(
            self,
            catalog_name: str,
            properties: Dict[str, Any],
            **kwargs
    ):
        self.ps_db_conn: Dict[str, Any] = self.CONF_DB['connection']
        self.ps_cat_name: list = properties.pop('catalog_name', catalog_name).split(self.CONF_DELIMITER)
        self.ps_tbl_name: str = self.ps_cat_name.pop(-1)
        self.ps_schema_name: str = self.ps_cat_name.pop(-1) if self.ps_cat_name else self.SCHEMA_NAME
        self.ps_tbl_type: str = properties.pop('catalog_type', catalog_name)
        self.ps_cols: Optional[Dict[str, Any]] = properties.pop('schemas', None)

        # Properties for Postgres table
        self.ps_tbl_primary_key: list = self.get_str_or_list(properties, 'primary_key')
        self.ps_tbl_unique = self.get_str_or_list(properties, 'unique')
        self.ps_tbl_foreign_key = properties.pop('foreign_key', {})
        self.ps_tbl_constraint = properties.pop('constraints', {})

        # Optional arguments for Postgres table
        self.ps_tbl_retentions: Optional[Dict[str, Any]] = kwargs.pop('retentions', {})

        super(PostgresTable, self).__init__(
            self.ps_db_conn,
            self.ps_schema_name,
            self.ps_tbl_name
        )

    def __getattribute__(self, attr):
        if attr in super(PostgresTable, self).__excluded__:
            raise AttributeError(f"{self.__class__} does not have attribute `{attr}`")
        return super(PostgresTable, self).__getattribute__(attr)

    def _schemas(self) -> Dict[str, PostgresColumn]:
        """Generate raw configuration from schemas"""
        return {k: PostgresColumn(v) for k, v in self.ps_cols.items()}

    @property
    def schemas(self):
        """Mapping optional properties and raw schemas together"""
        return self._schemas()

    @property
    def retention(self):
        return self.ps_tbl_retentions

    def validate_mapping(self):
        """Validate between configuration and existing"""
        pass

    @staticmethod
    def get_str_or_list(props, key):
        return _return_key if isinstance((_return_key := props.pop(key, [])), list) else [_return_key]
