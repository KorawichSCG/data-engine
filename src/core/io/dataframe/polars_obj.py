import os
import polars as pl
from typing import Optional
from src.core.io.dataframe.plugins.file_plug import FileObject


class PolarsColumnObject:
    """Polars Column Object"""
    MAP_DATA_TYPES = {
        'str': '',
        'i64': '',
        'f64': '',
    }
