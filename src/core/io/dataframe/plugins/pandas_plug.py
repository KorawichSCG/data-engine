import os
import pandas as pd


class PandasDataFrame:
    """
    Pandas DataFrame Object for read data with difference type
    """

    def __init__(
            self,
            server_conn,
            sub_path,
            file_name,
            file_type
    ):
        self.server_conn = server_conn
        self.sub_path = sub_path
        self.file_name = file_name
        self.file_type = file_type

        # Set necessary argument for `csv` type
        self.delimiter = ','
        self.encoding = 'utf-8'
        self.header = True
        self.mangle_dupe_cols = True
        self.skipinitialspace = False
        self.skiprows = 0
        self.memory_map = True

        # Set necessary argument for `excel` type

        # Set necessary argument for `json` type

        # Set necessary argument for `html` type

        # Set necessary argument for `xml` type

        # Set necessary argument for `pickle` type

        # Set necessary argument for `parquet` type

    @property
    def full_path(self):
        """
        Local: files://localhost/path/to/table.parquet
        S3: s3://bucket/partition_dir
        """
        return os.path.join(self.server_conn, self.sub_path, f"{self.file_name}.{self.file_type}")

    @property
    def convert_pandas_type(self):
        if self.file_type == 'csv':
            return pd.read_csv

    def read(self) -> pd.DataFrame:
        return self.convert_pandas_type(self.full_path)

    def save(self):
        return None


class PandasCSVObject:
    """Pandas DataFrame for parse `csv` file"""
    OBJECT_TYPE = "csv"
    __excluded__ = {'save'}

    def __init__(
            self,
            server_conn,
            sub_path,
            file_name,
            file_type
    ):
        self.file_name: str = file_name
        # super(PandasCSVFrame, self).__init__(
        #     server_conn,
        #     sub_path,
        #     file_name,
        #     file_type
        # )
