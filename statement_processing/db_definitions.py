# -*- coding: utf-8 -*-
"""This module implements definitions of tables of the final SQLite DB."""
import os
import sqlite3

import pandas as pd

from .constants import DATA_OUTPUT_DIR, DBConstants


def create_tables() -> None:
    """This function creates the final tables of the SQLite database
    that will store the final dataset.

    Note: the default behavior of the main `sqlite3.connect` function
        is that a DB will be created automatically if it doesn't exist.
    """
    with sqlite3.connect(DBConstants.STATEMENTS_DB) as connection:
        DBConstants.QUERIES.create_tables(connection)


def db_to_excel():
    with sqlite3.connect(DBConstants.STATEMENTS_DB) as connection:
        for curr_table in DBConstants.QUERIES.list_tables(connection):
            curr_table = curr_table[0]
            table_df = pd.read_sql_query(f"SELECT * FROM {curr_table} ORDER BY DATE", connection)
            table_df.drop(columns=["id"], inplace=True)
            table_df.to_csv(os.path.join(DATA_OUTPUT_DIR, f"{curr_table}.csv"), index=False)
