import pandas as pd
import psycopg
from typing import Dict, Any, Optional, List, Union, Tuple
from psycopg.sql import SQL
from psycopg.rows import tuple_row


class PostgresConn:
    """
    PostgresSQL connection class
    """

    def __init__(self, db_conn: Dict[str, Any]):
        self.error_stm: str = ""
        self.db_conn: Dict[str, Any] = db_conn
        self.db_conn_conf: Dict[str, Any] = {}
        self.db_cursor_conf: Dict[str, Any] = {
            "row_factory": tuple_row
        }

    @property
    def db_name(self) -> str:
        return self.db_conn['dbname']

    @property
    def connectable(self) -> bool:
        try:
            with psycopg.connect(connect_timeout=1, **self.db_conn):
                return True
        except psycopg.Error as err:
            print(
                f"{type(err).__module__.removesuffix('.errors')}:{type(err).__name__}: {str(err).rstrip()}"
            )
            return False

    def execute(self, query) -> None:
        with psycopg.connect(**self.db_conn) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL(query))

    def query(
            self,
            query: str,
            result_type: Optional[str] = None,
            force_type: Optional[Any] = None
    ) -> Union[pd.DataFrame, List[Any]]:
        result_type = result_type or 'df'
        assert result_type in {"list", "df"}
        with psycopg.connect(**self.db_conn) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL(query))
                data = cur.fetchall()
                if result_type != 'df':
                    return data
                cols = [i[0] for i in cur.description]
                return pd.DataFrame(data, columns=cols, dtype=force_type)

    def state(self):
        """
        SELECT * FROM pg_stat_activity;
        SELECT * FROM pg_stat_activity;
        select  pid as process_id
        ,       usename as username
        ,       datname as database_name
        ,       client_addr as client_address
        ,       application_name
        , backend_start
        , state
        , state_change
        from pg_stat_activity;

        SELECT * from pg_stat_statements ORDER by total_time DESC;
        SELECT *
        FROM pg_available_extensions
        WHERE name = 'pg_stat_statements' and
            installed_version is not null;
        """
        pass

    def explain(
            self,
            query: str,
            costs: bool = True,
            summary: bool = True,
            buffers: bool = True,
            analyze: bool = True,
            settings: bool = True,
            verbose: bool = True,
    ) -> Dict[str, list]:
        """
        check query:
            costs: False, analyze: True, timing: False, summary: False, buffers: False, settings: True, verbose: True
        analysis query:
            costs: True, analyze: True, timing: True, summary: True, buffers: True, settings: True, verbose: True
        """
        for param in {costs, analyze, verbose, settings, summary, buffers}:
            assert param in {True, False}
        with psycopg.connect(**self.db_conn) as conn:
            with conn.cursor(**self.db_cursor_conf) as cur:
                try:
                    cur.execute(SQL(f"""explain( format json, costs {costs}, analyze {analyze}, verbose {verbose},
                    settings {settings}, summary {summary}, buffers {buffers}) ({query})"""))
                    data = cur.fetchone()
                    result = {"QUERY PLAN": list(data)}
                    conn.rollback()
                except psycopg.Error as err:
                    result = {}
                    print(
                        f"{type(err).__module__.removesuffix('.errors')}:{type(err).__name__}: {str(err).rstrip()}"
                    )
        return result


class PostgresObject(PostgresConn):
    """
    Postgres object
    """
    # TODO: change way to inherit `PostgresConn` because excluded `query` and `execute` method
    OBJECT_TYPE: Optional[str] = None

    def __init__(
            self,
            db_conn: Dict[str, Any],
            schema_name: str,
            obj_name: str,
            auto_execute: bool = False
    ):
        super(PostgresObject, self).__init__(db_conn)
        self.schema_name: str = schema_name
        self.obj_name: str = obj_name
        self.auto_execute: bool = auto_execute
        self.statement: list = []

    def __str__(self):
        return f'{self.__class__.__name__}({self.obj_name_full}, connectable: {self.connectable})'

    @property
    def obj_name_full(self) -> str:
        return f"{self.db_name}.{self.schema_name}.{self.obj_name}"

    @property
    def schema_exists(self) -> bool:
        if self.connectable:
            result_df = super(PostgresObject, self).query(
                f"""select exists( select from {self.db_name}.information_schema.schemata
                    where schema_name = '{self.schema_name}' ) as check_exists"""
            )
            return result_df['check_exists'].to_dict().get(0, False)
        return False

    # @property
    # def auto_execute(self) -> bool:
    #     return self.auto_execute
    #
    # @auto_execute.setter
    # def auto_execute(self, _auto_execute: bool) -> None:
    #     self.auto_execute: bool = _auto_execute

    def execute(self, query: Optional[str] = None) -> None:
        if query:
            super(PostgresObject, self).execute(query)
        else:
            if self.statement:
                with psycopg.connect(**self.db_conn) as conn:
                    with conn.cursor() as cur:
                        try:
                            for _query in self.statement:
                                cur.execute(SQL(_query))
                        except psycopg.Error as err:
                            conn.rollback()
                            print(
                                f"{type(err).__module__.removesuffix('.errors')}:{type(err).__name__}: {str(err).rstrip()}"
                            )
            self.statement: list = []


class ColumnObject:
    """
    Column object
    -------------
    detail
    ------
    PostgreSQL includes the following column constraints:
        - NOT NULL – ensures that values in a column cannot be NULL.
        - UNIQUE – ensures the values in a column unique across the rows within the same table.
        - PRIMARY KEY – a primary key column uniquely identify rows in a table. A table can
            have one and only one primary key. The primary key constraint allows you to define
            the primary key of a table.
        - CHECK – a CHECK constraint ensures the data must satisfy a boolean expression.
        - FOREIGN KEY – ensures values in a column or a group of columns from a table exists
            in a column or group of columns in another table. Unlike the primary key, a table
            can have many foreign keys.

    PostgreSQL datatype:
        - Integer Types
            - smallint: int2: small-range integer -32768 to +32767
            - integer/int: int4: typical choice for integer	-2147483648 to +2147483647
            - bigint: int8: large-range integer	-9223372036854775808 to +9223372036854775807

        - Arbitrary Precision Numbers:
            - numeric(precision, scale)/numeric(precision)/numeric: variable: up to 131072 digits before the decimal point;
              up to 16383 digits after the decimal point
            - decimal: variable: The types decimal and numeric are equivalent
            NOTE: So the number 23.5141 has a precision of 6 and a scale of 4

        - Floating-Point Types
            - real: float(1) to float(24): 6 decimal digits precision
            - double precision: float(25) to float(53) and float: 15 decimal digits precision
            NOTE: When rounding values, the numeric type rounds ties away from zero, while (on most machines)
            the real and double precision types round ties to the nearest even number.

        - Serial Types
            - smallserial: serial2: small autoincrementing integer 1 to 32767
            - serial: serial4: autoincrementing integer 1 to 2147483647
            - bigserial: serial8: large autoincrementing integer 1 to 9223372036854775807

        - Monetary Types
            - money: currency amount -92233720368547758.08 to +92233720368547758.07
            NOTE: SELECT '12.34'::float8::numeric::money;
            SELECT '52093.89'::money::numeric::float8;

        - Boolean Type
            - boolean: 1 byte: [true, yes, on, 1] or [false, no, off, 0]

        - Character Types
            - character varying(n), varchar(n): variable-length with limit
            - character(n), char(n): fixed-length, blank padded
            - char: 1 byte: single-byte internal type
            - name: 64 bytes: internal type for object names
            - text: variable unlimited length

        - Date/Time Types
            - timestamp [ (p) ] without tim zone: 8 bytes: 4713 BC to 294276 AD
            - timestamp [ (p) ] with tim zone: 8 bytes: 4713 BC to 294276 AD
            - date: 4 bytes: 4713 BC to 5874897 AD
            - time [ (p) ] without tim zone: 8 bytes: 00:00:00 to 24:00:00
            - time [ (p) ] with tim zone: 12 bytes: 00:00:00+1559 to 24:00:00-1559
            - interval [ field ] [ (p) ]: 16 bytes: -178000000 years to 178000000 years
            NOTE: fields
                YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, YEAR TO MONTH, DAY TO HOUR, DAY TO MINUTE,
                DAY TO SECOND, HOUR TO MINUTE, HOUR TO SECOND, MINUTE TO SECOND
            NOTE:

    NOTE:  By using the CHECK constraint, you can make sure that data is updated to the database correctly
        By default, PostgreSQL gives the CHECK constraint a name using the following pattern: `{table}_{column}_check`
        Another way to create check like,
            `ALTER TABLE prices_list ADD CONSTRAINT valid_range_check CHECK (valid_to >= valid_from);`
            `ALTER TABLE prices_list ADD CONSTRAINT price_discount_check CHECK (price > 0 AND discount >= 0
            AND price > discount);`
        NOTE: To add the NOT NULL constraint to a column of an existing table, you use the following form of the
        ALTER TABLE statement:
            `ALTER TABLE production_orders ALTER COLUMN material_id SET NOT NULL, ALTER COLUMN finish_date SET NOT NULL;`

    NOTE:   CREATE SEQUENCE tablename_colname_seq AS integer;
            CREATE TABLE tablename (colname integer NOT NULL DEFAULT nextval('tablename_colname_seq'));
            ALTER SEQUENCE tablename_colname_seq OWNED BY tablename.colname;
    """

    COLUMN_DATATYPE: Dict[str, str] = {
        'bool': ['boolean'],
        'object': ['varchar', 'character_varying', 'char', 'character', 'text', 'money', 'name'
                   'numeric', 'decimal', 'date', 'time without timezone', 'time'
                   'array', 'json', 'jsonb'],
        'int64': ['smallint', 'bigint', 'integer', 'int', 'smallserial', 'serial', 'bigserial'],
        'float64': ['real', 'double precision'],
        'datetime64[ns]': 'timestamp without timezone',
        'datetime64[ns, UTC]': ['timestamp with timezone', 'timezone'],
        'timedelta64[ns]': 'interval',
    }

    def __init__(
            self,
            column_name: str,
            column_position: int,
            nullable: bool,
            data_type: str,
            default: Optional[str] = None,
            constraints: Optional[list] = None
    ):
        self.col_name = column_name
        self.col_position = column_position
        self.col_datatype: Optional[str] = data_type
        self.col_nullable: Optional[bool] = nullable
        self.col_default = default
        self.col_constraints: Optional[list] = constraints
        self.col_foreign_table_name, self.col_foreign_column_name = self._get_foreign_key()
        self.col_primary_key = self._get_constraints('primary key')
        self.col_unique = self._get_constraints('unique')

    def __repr__(self):
        _repr = f"({self.col_position}) {self.col_datatype}" \
                f"{(' unique' if self.col_unique else '')}" \
                f"{(' not null' if self.col_nullable else ' null')}" \
                f"{(' primary key' if self.col_primary_key else '')}"
        if self.col_foreign_table_name:
            return _repr + f" reference( {self.col_foreign_table_name}.{self.col_foreign_column_name} )"
        return _repr

    def _get_foreign_key(self) -> Tuple[Optional[str], Optional[str]]:
        if self.col_constraints:
            for _constraint in self.col_constraints:
                if foreign_tb_name := _constraint.get('foreign_table_name'):
                    return foreign_tb_name, _constraint.get('foreign_column_name')
        return None, None

    def _get_constraints(self, const_type: str) -> bool:
        if self.col_constraints:
            for _constraint in self.col_constraints:
                if _constraint.get('constraint_type') == const_type:
                    return True
        return False

    @property
    def position(self):
        return self.col_position

    @property
    def nullable(self):
        return self.col_nullable

    @property
    def unique(self):
        return self.col_unique

    @unique.setter
    def unique(self, set_unique: bool):
        """
        SELECT
          constraint_type,
          tc.constraint_name,
          tc.table_name,
          kcu.column_name,
          format('ALTER TABLE "%s" RENAME CONSTRAINT %s TO unq_%s_%s;',
                 tc.table_name, tc.constraint_name, tc.table_name, kcu.column_name)
        FROM
          information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE constraint_type = 'UNIQUE'
              AND tc.constraint_name like 'uk\_%'
        """
        self.col_unique = set_unique

    # @property
    # def check(self):
    #     return self.col_check
    #
    # @check.setter
    # def check(self, set_check: bool):
    #     return self.col_check

    # @property
    # def primary_key(self):
    #     return self.col_primary_key
    #
    # @primary_key.setter
    # def primary_key(self, set_primary: bool) -> None:
    #     """
    #     ALTER TABLE
    #     """
    #     self.col_primary_key: Optional[bool] = set_primary

    @property
    def foreign_key(self):
        """
        SELECT
          tc.constraint_name,
          tc.table_name,
          kcu.column_name,
          ccu.table_name  AS foreign_table_name,
          ccu.column_name AS foreign_column_name,
          format('ALTER TABLE "%s" RENAME CONSTRAINT %s TO fk_%s_%s;',
                 tc.table_name, tc.constraint_name, tc.table_name, kcu.column_name)
        FROM
          information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
          JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE constraint_type = 'FOREIGN KEY'
              AND tc.constraint_name not like 'fk\_%'
        """
        return f'{self.col_foreign_table_name}.{self.col_foreign_column_name}'

    @foreign_key.setter
    def foreign_key(self, reference: str) -> None:
        """
        ALTER TABLE Company ADD CONSTRAINT fk_Clients
        FOREIGN KEY ( Company_Id )REFERENCES Clients(Client_ID)
        ON DELETE CASCADE
        ON UPDATE RESTRICT;
        """
        reference_table, reference_column = reference.split('.')
        self.col_foreign_key['reference_table'] = reference_table
        self.col_foreign_key['reference_column'] = reference_column

    # @property
    # def comment(self):
    #     return self.col_comment
    #
    # @comment.setter
    # def comment(self, set_comment: str):
    #     """
    #     ALTER TABLE
    #     """
    #     self.comment = set_comment


class TableObject(PostgresObject):
    """
    Table Object
    ------------
    example
    -------
        (i)     users:
                    columns:
                        id: "serial PRIMARY KEY"
                        username: "VARCHAR (50) NOT NULL"
                        password: "VARCHAR (50) NOT NULL"
                        email: "VARCHAR (50) NULL"
                    constraint:
                        username_email_notnull: "
                            CHECK (NOT (( username IS NULL  OR  username = '' ) AND ( email IS NULL  OR  email = '' )))
                            "
    raw-example
    -----------
        (i)     CREATE TABLE users (
                    id serial PRIMARY KEY,
                    customer_id INTEGER REFERENCES customers(customer_id),
                    username VARCHAR (50) NOT NULL,
                    password VARCHAR (50) NOT NULL,
                    email VARCHAR (50) NULL,
                    CONSTRAINT username_email_notnull
                        CHECK (
                            NOT (( username IS NULL  OR  username = '' ) AND ( email IS NULL  OR  email = '' ))
                        )
                    ;
    """
    OBJECT_TYPE = "table"
    __excluded__ = {'query', 'execute'}

    def __init__(
            self,
            db_conn: Dict[str, Any],
            schema_name: str,
            tbl_name: str,
            auto_execute: Optional[bool] = False
    ):
        super().__init__(db_conn, schema_name, tbl_name, auto_execute)
        self.tbl_name: str = self.obj_name
        self.tbl_name_full: str = self.obj_name_full
        self.tbl_columns: Dict[str, ColumnObject] = self.generate_columns()
        self.tbl_constraints: Dict[str, dict] = self.generate_constraints()
        self.alive: bool = self.exists

    def generate_columns(self) -> Dict[str, ColumnObject]:
        _columns: Dict[str, dict] = {
            col_name: v for v in self._columns().values() if (col_name := v.get('column_name'))
        }
        _constraints: Dict[str, list] = {}
        for value in self._constraints().values():
            if col_name := value.pop('column_name', None):
                if col_name in _constraints:
                    _constraints[col_name].append(value)
                else:
                    _constraints[col_name] = [value]
        return {
            col_name: ColumnObject(constraints=_constraints.get(col_name, None), **_property)
            for col_name, _property in _columns.items()
        }

    def generate_constraints(self) -> Dict[str, dict]:
        _results_constraints: Dict[str, dict] = {}
        for _values in self._constraints().values():
            values = {k: v for k, v in _values.items() if k not in {'foreign_table_name', 'foreign_column_name'}}
            if (const_name := values.pop('constraint_name')) in _results_constraints:
                _results_constraints[const_name].get('column_name').append(values.get('column_name'))
            else:
                _results_constraints[const_name]: dict = {
                    key: [value] if key == 'column_name' else value for key, value in values.items()
                }
        return _results_constraints

    def _columns(self) -> Dict[int, dict]:
        """
        { <row-number>: {
            column_name: str : <column-name>,
            nullable: bool : [True, False],
            default: (optional: str : None): <default-statement>,
            data_type: str : <data-type>
            }
        }
        """
        return super(TableObject, self).query(
            f"""select	column_name
            ,		column_position
            ,		column_nullable											as nullable
            ,		column_default											as default
            ,		concat(data_type, data_type_length, with_time_zone)		as data_type
            from	(	select	column_name
            ,		ordinal_position										as column_position
            , 		case when lower(is_nullable) = 'yes'
                         then true
                         else false
                    end	                                                    as column_nullable
            ,		column_default											as column_default
            ,		case when data_type like '%with%'
                         then substring(data_type, 1, position(' ' in data_type) - 1)
                         when data_type like 'char%'
                         then ('{{"character varying": "varchar", "character": "char"}}'::jsonb ->> data_type)
                         when data_type = 'interval' and interval_type is not null
                         then concat(data_type, ' ', lower(interval_type))
                         else lower(data_type)
                    end                                                     as data_type
            ,		case when data_type like '%with time zone%'
                         then substring(data_type, position(' ' in data_type))
                         else ''
                    end                                                     as with_time_zone
            ,		case when character_maximum_length is not null and character_maximum_length > 1
                         then concat('( ', character_maximum_length, ' )') 
                         when numeric_precision is not null and numeric_scale > 0
                         then concat('( ', numeric_precision, ', ', numeric_scale, ' )')
                         when numeric_precision is not null and numeric_scale = 0 and udt_name not like 'int%'
                         then concat('( ', numeric_precision, ' )')
                         when datetime_precision between 1 and 5
                         then concat('(', datetime_precision,') ')
                         else ''
                    end                                                     as data_type_length
            from {self.db_name}.information_schema.columns
            where   table_schema = '{self.schema_name}' and table_name = '{self.tbl_name}') as a"""
        ).reset_index(drop=True).to_dict('index')

    def _constraints(self) -> Dict[int, dict]:
        """
        :return:
        { <row-number>: {
            constraint_name: str : <constraint-name>,
            constraint_type: str : ['unique', 'check', 'primary key', 'foreign key'],
            foreign_table_name (optional: str : None): <foreign-table-name>,
            foreign_column_name (optional: str : None): <foreign-column-name>,
            constraint_desc: (optional: str : None): <constraint-statement>
            }
        }
        :rtype: Dict[int, dict]
        """
        return super(TableObject, self).query(
            f"""select	tc.constraint_name
            ,		lower(tc.constraint_type)                                       as constraint_type
            ,		coalesce(kcu.column_name, ccu.column_name)                      as column_name
            ,		case when tc.constraint_type = 'FOREIGN KEY'
                         then ccu.table_name
                         else null
                    end                                                             as foreign_table_name
            ,	    case when tc.constraint_type = 'FOREIGN KEY'
                         then ccu.column_name else null
                    end                                                             as foreign_column_name 
            ,		case when tc.constraint_type = 'PRIMARY KEY'
                         then format('(primary key ( %s )', ccu.column_name)
                         when tc.constraint_type = 'FOREIGN KEY'
                         then format('(foreign key ( %s ) references %s( %s ))',
                            kcu.column_name, ccu.table_name, ccu.column_name)
                         else lower(cc.check_clause)
                    end                                                             as constraint_desc
            from {self.db_name}.information_schema.table_constraints             as tc
            left join {self.db_name}.information_schema.key_column_usage         as kcu
                on tc.constraint_name = kcu.constraint_name
            left join {self.db_name}.information_schema.constraint_column_usage  as ccu
                on ccu.constraint_name = tc.constraint_name
            left join {self.db_name}.information_schema.check_constraints        as cc
                on cc.constraint_name = tc.constraint_name 
            where tc.table_schema = '{self.schema_name}' and tc.table_name = '{self.tbl_name}'
            and tc.constraint_name not like '%not_null'"""
        ).reset_index(drop=True).to_dict('index')

    @property
    def columns(self) -> Dict[str, ColumnObject]:
        return self.tbl_columns

    @property
    def constraints(self):
        return self.tbl_constraints

    @constraints.setter
    def constraints(self, _const):
        self.tbl_constraints = _const

    @constraints.deleter
    def constraints(self):
        """
        ALTER TABLE table_name
        DROP CONSTRAINT constraint_fkey;
        """
        pass

    @property
    def name(self):
        return self.tbl_name

    @property
    def fullname(self):
        return self.tbl_name_full

    @property
    def exists(self) -> bool:
        if super(TableObject, self).schema_exists:
            result_dict: dict = super(TableObject, self).query(
                f"""select exists( select from {self.db_name}.information_schema.tables
                where table_schema = '{self.schema_name}' and table_name = '{self.tbl_name}' ) as check_exists"""
            )['check_exists'].to_dict()
            return result_dict.get(0, False)
        return False

    def rename(self, table_name):
        self.statement.append(f"alter {self.OBJECT_TYPE} {self.obj_name_full} rename to {table_name};")
        if self.auto_execute:
            self.execute()
        self.obj_name = table_name
        return self

    def create(self, if_not_exists: Optional[bool] = False):
        # TODO: create table when dropped
        self.statement.append(
            f"""create {self.OBJECT_TYPE} {('if not exists' if if_not_exists else '')} {self.obj_name_full} ();"""
        )
        if self.auto_execute:
            self.execute()
        return self

    def drop(self, if_exists: Optional[bool] = False):
        self.statement.append(f"drop {self.OBJECT_TYPE} {('if exists' if if_exists else '')} {self.obj_name_full};")
        if self.auto_execute:
            self.execute()
        self.alive = False
        return self

    def select(self, *args, **kwargs):
        _columns = args if isinstance(args[0], str) else args[0]
        # TODO: validate `_columns`
        result_type = kwargs.get('result_type', None)
        return super(TableObject, self).query(
            f"""select {(",".join(_columns))} from {self.fullname}""", result_type
        )


class ViewObject(PostgresObject):
    """
    View Object
    -----------
    config
    ------
        (i)     <view-name>:
                    columns (optional): [<columns-name>, ...]
                    parameters (optional): [<parameter-name>, ...]
                    script: "<view-statement>"

    raw-example
    -----------
        (i)     CREATE [ OR REPLACE ] [ TEMP | TEMPORARY ] [ RECURSIVE ] VIEW name [ ( column_name [, ...] ) ]
                    [ WITH ( view_option_name [= view_option_value] [, ... ] ) ]
                    AS query
                    [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
        (ii)    create or replace view <view-name> as

    """
    OBJECT_TYPE = "view"

    def __init__(
            self,
            db_conn: Dict[str, Any],
            schema_name: str,
            view_name: str,
            auto_execute: Optional[bool] = False
    ):
        super().__init__(db_conn, schema_name, view_name, auto_execute)


class MaterializedViewObject(PostgresObject):
    """
    Materialized View Object
    ------------------------
    detail
    ------

    config
    ------
        (i)     <materialized-view-name>:
                    parameters (optional): [<parameter-name>, ...]
                    script: "<materialized-view-statement>"
    example
    -------
        (i)     vw_ai_opt_forecast_mch3:
                    parameters: ['']
                    script: "
                        select  coalesce(afm.scgh_mch3_code, aasm.scgh_mch3_code)	        as scgh_mch3_code
                        ,		coalesce(aasm.actual_sales_value, 0.0)				        as actual_sales_value
                        ,		coalesce(afm.forecast_value, 0.0)					        as forecast_value
                        ,		coalesce(afm.forecast_month, aasm.start_of_month)	        as start_of_month
                        from  {database_name}.{ai_schema_name}.ai_forecast_mch3             as afm
                        full join {database_name}.{ai_schema_name}.ai_actual_sales_mch3     as aasm
                        on  aasm.scgh_mch3_code   =   afm.scgh_mch3_code
                        and aasm.start_of_month   =   afm.forecast_month
                        order by 1,4
                        "
    raw-example
    -----------
        (i)     create materialized view if not exists {database_name}.{ai_schema_name}.vw_ai_opt_forecast_mch3 as
                (
                    select  coalesce(afm.scgh_mch3_code, aasm.scgh_mch3_code)	        as scgh_mch3_code
                    ,		coalesce(aasm.actual_sales_value, 0.0)				        as actual_sales_value
                    ,		coalesce(afm.forecast_value, 0.0)					        as forecast_value
                    ,		coalesce(afm.forecast_month, aasm.start_of_month)	        as start_of_month
                    from  {database_name}.{ai_schema_name}.ai_forecast_mch3             as afm
                    full join {database_name}.{ai_schema_name}.ai_actual_sales_mch3     as aasm
                    on  aasm.scgh_mch3_code   =   afm.scgh_mch3_code
                    and aasm.start_of_month   =   afm.forecast_month
                    order by 1,4
                )
    """
    OBJECT_TYPE = "materialized view"

    def refresh(self):
        self.statement.append(f"refresh {self.OBJECT_TYPE} {self.obj_name_full};")
        return self


class FunctionObject(PostgresObject):
    """
    Function Object
    ---------------
    detail
    ------
        NOTE: `select count_if_exists('ai', 'ai_date_master')`
    raw-example
    -----------
        (i)     create or replace function {database_name}.{ai_schema_name}.count_if_exists(
                    _schm text,
                    _tbl text,
                    out result integer
                ) language plpgsql as
                $func$
                    begin
                        if exists(select * from {database_name}.information_schema.tables
                            where table_schema = _schm and table_name = _tbl) then
                            execute format('select count(*) as row_num from {database_name}.%s.%s', _schm, _tbl)
                            into result;
                        else execute format('select 0') into result;
                        end if;
                    end;
                $func$;
    """
    OBJECT_TYPE = "function"


class ProcedureObject(PostgresObject):
    """
    Store Procedure Object
    ----------------------
    detail
    ------
        NOTE: `call transfer(1,2,1000);`
    raw-example
    -----------
        (i)     create or replace procedure transfer(
                    sender int,
                    receiver int,
                    amount dec
                ) language plpgsql as
                $store_proc$
                    begin
                        -- subtracting the amount from the sender's account
                        update accounts
                        set balance = balance - amount
                        where id = sender;
                        -- adding the amount to the receiver's account
                        update accounts
                        set balance = balance + amount
                        where id = receiver;
                        commit;
                    end;
                $store_proc$
    """
    OBJECT_TYPE = "procedure"
