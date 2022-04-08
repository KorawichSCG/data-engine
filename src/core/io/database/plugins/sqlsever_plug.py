import pyodbc
import pandas as pd
from typing import Dict, Any, Optional, List, Union, Tuple


class SQLServerConnection:
    """
    Microsoft SQL Server connection class
    """

    def __init__(self, db_conn: Dict[str, Any]):
        self.db_conn: Dict[str, Any] = db_conn
        self.db_conn_conf: Dict[str, Any] = {}
        self.error_stm: str = ""

    @property
    def db_name(self) -> str:
        return self.db_conn['dbname']

    @property
    def connectable(self) -> bool:
        try:
            with pyodbc.connect(connect_timeout=1, **self.db_conn):
                return True
        except pyodbc.Error as err:
            print(
                f"{type(err).__module__.removesuffix('.errors')}:{type(err).__name__}: {str(err).rstrip()}"
            )
            return False

    def execute(self, query) -> None:
        with pyodbc.connect(**self.db_conn) as conn:
            with conn.cursor() as cur:
                cur.execute(query)

    def query(
            self,
            query: str,
            result_type: Optional[str] = None,
            force_type: Optional[Any] = None
    ) -> Union[pd.DataFrame, List[Any]]:
        result_type = result_type or 'pandas'
        assert result_type in {"list", "pandas"}
        with pyodbc.connect(**self.db_conn) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                data = cur.fetchall()
                if result_type != 'pandas':
                    return data
                cols = [i[0] for i in cur.description]
                return pd.DataFrame(data, columns=cols, dtype=force_type)
