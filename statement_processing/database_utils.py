# -*- coding: utf-8 -*-
"""This module implements helper SQLite database utilities used in this project."""
import os
import sqlite3
from hashlib import md5
from pathlib import Path
from typing import Iterable

import pandas as pd

from .constants import DB_QUERIES


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


def create_id_for_each_row(input_df: pd.DataFrame, id_column_name: str = "id", columns_to_ignore: Iterable[str] = None):
    """This function adds a new column into the input DataFrame. This column
    is meant to represent the unique identification of data in each row.

    Args:
        input_df: DataFrame that will be enriched by a new id column.
        id_column_name: Name of the unique column that will be created in this function.
        columns_to_ignore: Columns that will be ignored from the unique ID calculation.

    Returns:
        Pandas DataFrame enriched by a new column that uniquely identifies each row.
    """
    columns_to_ignore = columns_to_ignore if columns_to_ignore else []

    # pylint: disable=bad-builtin
    input_df[id_column_name] = input_df.apply(
        lambda x: md5("".join(map(str, x if x not in columns_to_ignore else "")).encode("utf-8")).hexdigest(),
        axis=1,
    )
    # pylint: enable=bad-builtin
    return input_df
