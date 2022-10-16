# -*- coding: utf-8 -*-
"""This module implements helper SQLite database utilities used in this project."""
import os
import re
import sqlite3
from hashlib import md5
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .constants import DB_QUERIES

IS_DIGIT = re.compile(r"^\d+(?:[,.]\d*)?$")


def create_tables(database_location: str) -> None:
    """This function creates the final tables of the SQLite database
    that will store the final dataset.

    Args:
        database_location: Full path name to the SQlite DB for which the tables will be created.

    Note: the default behavior of the main `sqlite3.connect` function
        is that a DB will be created automatically if it doesn't exist.
    """
    with sqlite3.connect(database_location) as connection:
        DB_QUERIES.create_tables(connection)


def sqlite_tables_to_csv_files(database_location: str, output_dir: str) -> None:
    """This function exports all final tables of the SQLite database
    into a collection of CSVs (one CSV per table).

    Args:
        database_location: Full path name to the SQlite DB for which the tables will be exported.
        output_dir: Directory where the CSV files will be exported. It will be created
            if it doesn't exist yet.

    Note: Data in the output CSVs will be ordered by date.
    """
    Path(output_dir).mkdir(exist_ok=True)

    with sqlite3.connect(database_location) as connection:
        for curr_table in DB_QUERIES.list_all_tables(connection):
            curr_table = curr_table[0]

            table_df = pd.read_sql_query(f"SELECT * FROM {curr_table} ORDER BY DATE", connection)
            table_df.drop(columns=["id"], inplace=True)
            table_df.to_csv(os.path.join(output_dir, f"{curr_table}.csv"), index=False)


def _custom_hash_func(row: pd.core.series.Series, columns_to_ignore: Iterable[str]) -> str:
    """This function implements a deterministic hashing of a row and produces a unique hash.

    Args:
        row: Pandas DataFrame row.
        columns_to_ignore: See :func:`create_id_for_each_row`.

    Note: A column cannot have NaN or None values, the hashing wouldn't be consistent.
        Please pre-process your DataFrame so that these values are removed or replaced
        with some trivial (e.g. empty) string.
    """
    row_as_str = ""

    for col in row:
        if col in columns_to_ignore:
            continue

        col_as_str = str(col)

        if IS_DIGIT.match(col_as_str):
            row_as_str += str(round(float(col), 4))
        else:
            row_as_str += col_as_str

    return md5(row_as_str.encode("utf-8")).hexdigest()


def create_id_for_each_row(input_df: pd.DataFrame, id_column_name: str = "id", columns_to_ignore: Iterable[str] = None):
    """This function adds a new column into the input DataFrame. This column
    is meant to represent the unique identification of data in each row.

    Args:
        input_df: DataFrame that will be enriched by a new id column.
        id_column_name: Name of the unique column that will be created in this function.
        columns_to_ignore: Columns that will be ignored from the unique ID calculation.

    Returns:
        Pandas DataFrame enriched by a new column that uniquely identifies each row.

    Note: It was important to substitute None and NaN values with an empty string, because
        otherwise the hashing was not deterministic.
    """
    columns_to_ignore = columns_to_ignore if columns_to_ignore else []

    input_df[id_column_name] = (
        input_df.replace("None", np.nan)
        .fillna("")
        .apply(
            lambda x: _custom_hash_func(x, columns_to_ignore),
            axis=1,
        )
    )
    return input_df
