# -*- coding: utf-8 -*-
"""This module implements definitions of tables of the final SQLite DB."""
import sqlite3

from .constants import DBConstants


def create_tables() -> None:
    """This function creates the final tables of the SQLite database
    that will store the final dataset.

    Note: the default behavior of the main `sqlite3.connect` function
        is that a DB will be created automatically if it doesn't exist.
    """
    with sqlite3.connect(DBConstants.STATEMENTS_DB) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DBConstants.TRANSACTIONS_TABLE} (
                id TEXT PRIMARY KEY,
                Date DATE,
                Type TEXT NOT NULL,
                Item TEXT NOT NULL,
                Units DOUBLE NOT NULL,
                Currency TEXT NOT NULL,
                PPU DOUBLE NOT NULL,
                Fees DOUBLE,
                Remarks TEXT
            )
        """
        )

        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DBConstants.DEP_AND_WITHDRAWALS_TABLE} (
                id TEXT PRIMARY KEY,
                Date DATE,
                Type TEXT NOT NULL,
                Currency TEXT NOT NULL,
                Amount DOUBLE NOT NULL
            )
        """
        )

        connection.commit()
