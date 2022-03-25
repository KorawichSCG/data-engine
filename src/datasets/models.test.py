import itertools
import re
import src.core.io.plugins.database as db
from typing import Any, Dict, Union, Optional
from src.core.io import parse_config

PROJ_PATH = r'D:\korawica\Work\dev02_miniproj\GITHUB\data-engine'


class AIColumnObject:
    """
    config
    ------
        (i)   <column-name>:
                    datatype: <datatype>
                    unique: <False, [True, true, false]>
                    nullable (optional): <null, [not null]>
                    defaults (optional): <defaults>
                    primary_key (optional): <False, [True, true, false]>
                    check (optional): <check-statement>
                    comment (optional): <comment>
        (ii)  <column-name>: <datatype> [<nullable>, <default>, ...]

    example
    -------
        (i.i)    customer_id:
                    datatype: "varchar( 15 )"
                    nullable: "false"
                    primary_key: "true"
                    check: "customer_id like 'CM-%'"
                    comment: "The customer ID"

        (i.ii)   customer_id:
                    datatype: "varchar( 15 )"
                    nullable: "false"
                    primary_key: "true"
                    check: "customer_id like 'CM-%'"
                    comment: "The customer ID"

        (ii.i)   order_sales_value: "double precision"
        (ii.ii)  customer_id: "varchar( 15 ) NOT NULL PRIMARY KEY [The customer ID]"
        (ii.iii) email: "varchar( 30 ) UNIQUE NOT NULL CHECK(email like '%@%.com')"
        (ii.iv)  salary: "numeric CONSTRAINT positive_salary CHECK(salary > 0)"
        (ii.v)   birth_date: "date CHECK (birth_date > '1900-01-01')"
        (ii.vi)  joined_date: "date NOT NULL CHECK (joined_date > birth_date)"
        (ii.vii) order_id integer NOT NULL DEFAULT nextval('tablename_colname_seq')
        (ii.iix)
        (ii.ix)
        (ii.x)
        (ii.xi)
    """
    COLUMN_CONSTRAINTS: set = {'unique', 'not null', 'null', 'primary key', 'constraint', 'check'}

    def __init__(self, ps_column_detail: Union[str, Dict[str, Any]]):
        self.ps_column_datatype: Optional[str] = None
        self.ps_column_unique: Optional[str] = None
        self.ps_column_nullable: Optional[bool] = None
        self.ps_column_primary_key: Optional[bool] = None
        self.ps_column_foreign_key: Optional[dict] = {'reference_table': None, 'reference_column': None}
        self.ps_column_check: Optional[str] = None
        self.ps_column_comment: Optional[str] = None
        if isinstance(ps_column_detail, str):
            self.convert_from_string(ps_column_detail)
        else:
            self.convert_from_mapping(ps_column_detail)

    def __str__(self):
        return f'{self.__class__.__name__}({self.ps_column_datatype}, {self.ps_column_nullable})'

    def __repr__(self):
        return f'Column({self.ps_column_datatype}, {self.ps_column_nullable})'

    def convert_from_string(self, _ps_detail: str):
        _ps_detail_lower = _ps_detail.lower()
        regex_comment = ".* ?"
        before_comment, split_comment, after_comment = re.split(
            f"(\[{regex_comment}\])", _ps_detail_lower, maxsplit=2
        ) if (match_comment := re.findall(f"\[({regex_comment})\]", _ps_detail_lower)) else (_ps_detail_lower, '', '')

        match_list: list = re.split(f"({'|'.join(list(self.COLUMN_CONSTRAINTS))})", before_comment.strip())
        match_datatype = match_list.pop(0)
        match_dict: dict = dict(itertools.zip_longest(*[iter(match_list)] * 2, fillvalue=""))
        for k in self.COLUMN_CONSTRAINTS:
            if k in match_dict:
                match_dict[k] = True if (value := match_dict[k].strip()) == '' else value
            else:
                match_dict[k] = None
        match_dict['nullable'] = not match_dict.pop('not null')
        match_dict['datatype'] = match_datatype.strip()
        match_dict['comment'] = match_comment[0].strip() if match_comment else None
        match_dict.pop('null')
        self.convert_from_mapping(match_dict)

    def convert_from_mapping(self, _ps_detail: Dict[str, Any]):
        self.ps_column_datatype = _ps_detail.pop('datatype', 'text')
        self.ps_column_unique = _ps_detail.pop('unique', False) or False
        self.ps_column_nullable = _ps_detail.pop('nullable', False) or False
        self.ps_column_primary_key = _ps_detail.pop('primary key', False) or False
        self.ps_column_check = _ps_detail.pop('check', '') or ''
        self.ps_column_comment = _ps_detail.pop('comment', None) or None

    @property
    def nullable(self):
        return self.ps_column_nullable

    @property
    def unique(self):
        return self.ps_column_unique

    @property
    def check(self):
        return self.ps_column_check

    @property
    def primary_key(self):
        return self.ps_column_primary_key

    @property
    def foreign_key(self) -> Dict:
        return self.ps_column_foreign_key

    @primary_key.setter
    def primary_key(self, set_primary: bool) -> None:
        self.ps_column_primary_key: Optional[bool] = set_primary

    @foreign_key.setter
    def foreign_key(self, reference: str) -> None:
        reference_table, reference_column = reference.split('.')
        self.ps_column_foreign_key['reference_table'] = reference_table
        self.ps_column_foreign_key['reference_column'] = reference_column

    @property
    def comment(self):
        return self.ps_column_comment

    @property
    def details(self):
        return {
            'datatype': self.ps_column_datatype,
            'unique': self.ps_column_unique,
            'nullable': self.ps_column_nullable,
            'primary key': self.ps_column_primary_key,
            'check': self.ps_column_check,
            'comment': self.ps_column_comment
        }


class AITableObject(db.TableObject):
    """
    config
    ------
        <table-name>:
            type: datasets.models.AITableObject
            columns:
                (i)   <column-name>:
                            datatype: <datatype>
                            nullable (optional): <nullable>
                            check (optional): <check>
                            default (optional): <default>
                            comment (optional): <comment>
                (ii)  <column-name>: <datatype> [<nullable>, <default>, ...]
                      ...
            primary_key (optional): [<column-name>, ...]
            foreign_key (optional):
                <column-name>: <reference-table-name> (<reference-column-name>)
                ...
            unique (optional): [<column-name>, ...]
            constraint (optional):
                <constraint-name>: <constraint-detail>
                ...
    example
    -------
        (i)   customer_table:
                    type: 'datasets.models.AITableObject'
                    columns:
                        customer_id:
                            datatype: ''
                            nullable: 'False'
                            comment: 'The customer ID'
                        customer_name: ''
                        customer_profile: ''
                        register_date: 'date'
                        update_datetime: 'timestamp'
                    primary_key: ['customer_id']
        (ii)  order_transaction_table:
                    type: 'datasets.models.AITableObject'
                    columns:
                        order_id: ''
                        article_code: ''
                        order_value: ''
                        order_quantity: ''
                        customer_owner: ''
                        create_datetime: 'timestamp'
                    primary_key: ['order_id', 'article_code', 'customer_owner', 'create_datetime']
                    foreign_key:
                        customer_owner: 'customer_table (customer_id)'
                    unique: ['order_id']
    """
    CONF_DB = parse_config(f'{PROJ_PATH}/conf/config.yaml')['datasets']['postgresql.sandbox']

    def __init__(self, ps_tbl_name: str, **kwargs):
        self.ps_dbs_conn: Dict[str, Any] = self.CONF_DB['connection']
        self.ps_sch_name: str = self.CONF_DB['schemas']['ai_schema']
        self.ps_columns: Dict[str, Any] = kwargs.pop('columns', None)
        super(AITableObject, self).__init__(
            self.ps_dbs_conn,
            self.ps_sch_name,
            ps_tbl_name,
            # kwargs.pop('columns', None),
            # kwargs.pop('primary_key', None),
            # kwargs.pop('foreign_key', None),
            # kwargs.pop('unique', None)
        )

    @property
    def columns_conf(self):
        return {k: AIColumnObject(v) for k, v in self.ps_columns.items()}


class AIDataFrameObject(db.TableObject):
    """
    config
    ------
        <dataframe-name>:
            type: datasets.models.AIDataFrameObject
            load_args:
                parameters (optional): [<parameter-key>, ...]
                statement: ''
            save_args:
                parameter (optional): [<parameter-key>, ...]
                statement: ''
    example
    -------
        customer_dataframe:
            type: 'datasets.models.AIDataFrameObject'
            load_args:
                parameters: ['database_name', 'ai_schema_name']
                statement: "
                    select  customer_id
                    ,       customer_name
                    ,       customer_profile
                    ,       register_date
                    ,       update_datetime
                    from    {database_name}.{ai_schema_name}.customer_table
                "
    """
    CONF_DB = parse_config(f'{PROJ_PATH}/conf/config.yaml')['datasets']['postgresql.sandbox']

    def __init__(self, ps_dfr_name: str, **kwargs):
        self.ps_dbs_conn: Dict[str, Any] = self.CONF_DB['connection']
        self.ps_sch_name: str = self.CONF_DB['schemas']['ai_schema']
        self.ps_dfr_name = ps_dfr_name
        super(AIDataFrameObject, self).__init__(
            self.ps_dbs_conn,
            self.ps_sch_name,
            self.ps_dfr_name
            # kwargs.pop('columns', None),
            # kwargs.pop('primary_key', None),
            # kwargs.pop('foreign_key', None),
            # kwargs.pop('unique', None)
        )


class AIFunctionObject(db.FunctionObject):
    pass
