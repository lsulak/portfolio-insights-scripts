# -*- coding: utf-8 -*-
"""Coinbase Pro statement processing. It requires a directory of CSV reports exported from the
platform and it outputs easy-to-consume data into SQLite database.
"""
import logging
import sqlite3
from glob import glob

import pandas as pd
from currencies import MONEY_FORMATS

from .constants import DB_QUERIES
from .database_utils import create_id_for_each_row

logger = logging.getLogger(__name__)


def load_data_into_pandas(input_directory: str) -> pd.DataFrame:
    """This function loads the CSV files from the input directory into de-duplicated
    pandas DataFrame.

    Data relevant to a given user (i.e. balance or portfolio ID) are removed.

    Args:
        input_directory: Directory with the Coinbase CSV statements
            exported manually by a user.

    Returns:
        Pandas DataFrame that holds all the desired data.
    """
    all_dataframes = []

    for input_file_name in glob(f"{input_directory}/*.csv"):
        logger.info("Going to load the following file: %s", input_file_name)

        input_df = pd.read_csv(input_file_name, index_col=None, header=0)
        input_df.drop(columns=["portfolio", "balance"], inplace=True)

        input_df = create_id_for_each_row(input_df)

        all_dataframes.append(input_df)

    all_data_as_df = pd.concat(all_dataframes, axis=0, ignore_index=True).drop_duplicates()
    all_data_as_df.columns = all_data_as_df.columns.str.replace(" ", "")
    return all_data_as_df


def process_transactions(input_data: pd.DataFrame) -> pd.DataFrame:
    """This function processes transaction only data.

    Args:
        input_data: See the output of :func:`load_data_into_pandas`.

    Returns:
        Pandas DataFrame containing well-shaped transaction data.
    """
    logger.info("Going to process all the transaction data.")

    fees = input_data.query("type == 'fee'")[:]
    fees.rename(columns={"amount/balanceunit": "Currency", "amount": "Fees"}, inplace=True)
    fees.drop(columns=["type"], inplace=True)
    fees.Fees = abs(fees.Fees)

    transactions = input_data.query("type == 'match'")[:]

    crypto_details = transactions.query(f"`amount/balanceunit` not in {list(MONEY_FORMATS.keys())}")[:]
    crypto_details.rename(columns={"amount": "Units", "amount/balanceunit": "Item"}, inplace=True)

    fiat_details = transactions.query(f"`amount/balanceunit` in {list(MONEY_FORMATS.keys())}")[:]
    fiat_details.amount = abs(fiat_details.amount)
    fiat_details.rename(columns={"amount/balanceunit": "Currency"}, inplace=True)

    aggregated_df = crypto_details.merge(fiat_details, how="inner", on=["time", "type", "tradeid", "orderid"])
    aggregated_df["PPU"] = aggregated_df.amount.div(aggregated_df.Units.values)
    aggregated_df.type = aggregated_df.Units.apply(lambda x: "Buy" if x > 0 else "Sell")

    final_transactions = aggregated_df.merge(fees, how="inner", on=["time", "Currency", "tradeid", "orderid"])
    return final_transactions


def load_transactions_to_db(sqlite_conn: sqlite3.Connection, transaction_data: pd.DataFrame) -> None:
    """This function loads the transaction data into a SQLite table.

    Args:
        sqlite_conn: Already established connection to SQLite DB.
        transaction_data: See the output of the :func:`process_transactions`.
    """
    logger.info("Going to store all the transaction data into the DB")

    transaction_data.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DB_QUERIES.insert_coinbase_pro_transactions(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")


def load_deposits_and_withdrawals_to_db(sqlite_conn: sqlite3.Connection, input_data: pd.DataFrame) -> None:
    """This function identifies deposit and withdrawal events and loads them into
    a SQLite table.

    Args:
        sqlite_conn: See :func:`load_transactions_to_db`.
        input_data: See the output of :func:`load_data_into_pandas`.
    """
    logger.info("Going to process deposits and withdrawals information and store it to the DB")
    withdrawal_data = input_data.query(
        f"type in ('deposit', 'withdrawal') and `amount/balanceunit` in {list(MONEY_FORMATS.keys())}"
    )
    withdrawal_data.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DB_QUERIES.insert_coinbase_pro_deposits_and_withdrawals(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")


def process(input_directory: str, output_db_location: str) -> None:
    """This is the main function for the whole Coinbase Pro statement processing.

    Args:
        input_directory: See func:`load_data_into_pandas`.
        output_db_location: Full path name to the output SQlite DB.

    Raises:
        Exception: If the processing or loading data into DB wasn't successful.
    """
    logger.info("The processing of %s just started.", __name__)

    input_data = load_data_into_pandas(input_directory)
    transactions_data = process_transactions(input_data)

    with sqlite3.connect(output_db_location) as connection:
        try:
            load_transactions_to_db(connection, transactions_data)
            load_deposits_and_withdrawals_to_db(connection, input_data)
        except Exception as err:
            connection.execute("DROP TABLE tmp_table")
            raise Exception(
                f"The processing of {__name__} wasn't successful. Further details:\n{err}",
            ) from err

    logger.info("The processing of %s just finished.", __name__)
