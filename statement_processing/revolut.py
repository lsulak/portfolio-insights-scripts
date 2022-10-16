# -*- coding: utf-8 -*-
"""Revolut statement processing. It requires a directory of CSV reports exported from the
platform and it outputs easy-to-consume data into SQLite database.
"""
import logging
import sqlite3
from datetime import datetime, timedelta
from glob import glob

import numpy as np
import pandas as pd
import yfinance as yf

from .constants import DB_QUERIES
from .database_utils import create_id_for_each_row
from .stocks_utils import (
    produce_missing_exchange_prefixes_for_tickers,
    replace_renamed_ticker_symbols,
)

logger = logging.getLogger(__name__)


def load_data_into_pandas(input_directory: str) -> pd.DataFrame:
    """This function loads the CSV files from the input directory into memory
    in a lazy way using Generator.

    Args:
        input_directory: Directory with the Revolut CSV statements
            exported manually by a user.

    Returns:
        Pandas DataFrame containing de-duplicated input data with MD5
        hash representation of each line.
    """
    all_dataframes = []

    for input_file_name in glob(f"{input_directory}/*.csv"):
        logger.info("Going to load the following file: %s", input_file_name)

        input_df = pd.read_csv(input_file_name, index_col=None, header=0)
        input_df.drop(
            columns=[
                "FX Rate",
            ],
            inplace=True,
        )

        input_df = create_id_for_each_row(input_df)

        all_dataframes.append(input_df)

    all_data_as_df = pd.concat(all_dataframes, axis=0, ignore_index=True).drop_duplicates()
    all_data_as_df.columns = all_data_as_df.columns.str.replace(" ", "")
    return all_data_as_df


def produce_missing_dividend_details(pandas_row: pd.core.series.Series) -> pd.core.series.Series:
    """This function adds missing information about dividend-related records into a pandas row.

    Namely, dividend per share and number of currently held units are added - those two details
    are important, but not present in Revolut statements.

    Args:
        pandas_row: A single row of a Pandas DataFrame.

    Returns:
        Processed row enriched with the desired information.
    """
    ticker = yf.Ticker(pandas_row["Item"])

    start_date = datetime.strptime(pandas_row["Date"], "%Y-%m-%d") - timedelta(days=40)
    end_date = datetime.strptime(pandas_row["Date"], "%Y-%m-%d") + timedelta(days=1)

    dividend_hist_around_the_date_of_receival = ticker.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
    )["Dividends"]

    # Get the first non-zero value.
    dividend_per_share = np.trim_zeros(dividend_hist_around_the_date_of_receival)[0]

    pandas_row["PPU"] = dividend_per_share
    pandas_row["Units"] = pandas_row["TotalAmount"] / dividend_per_share

    return pandas_row


def produce_missing_stock_split_details(pandas_row: pd.core.series.Series) -> pd.core.series.Series:
    """This function adds missing information about stock split events into a pandas row.

    Namely, number of currently held units (before the split) and stock split ratio. Those two details
    are important, but not present on Revolut statements.

    Args:
        pandas_row: A single row of a Pandas DataFrame.

    Returns:
        Processed row enriched with the desired information.
    """
    ticker = yf.Ticker(pandas_row["Item"])

    start_date = datetime.strptime(pandas_row["Date"], "%Y-%m-%d") - timedelta(days=10)
    end_date = datetime.strptime(pandas_row["Date"], "%Y-%m-%d") + timedelta(days=1)

    split_hist_around_the_date_of_record = ticker.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
    )["Stock Splits"]

    # Get the first non-zero value.
    stock_split_ratio = np.trim_zeros(split_hist_around_the_date_of_record)[0]

    pandas_row["Units"] = pandas_row["TotalUnitsAfterSplit"] / stock_split_ratio
    pandas_row["StockSplitRatio"] = stock_split_ratio

    return pandas_row


def load_data_to_db(sqlite_conn: sqlite3.Connection, input_data: pd.DataFrame) -> None:
    """This is the main function that is responsible for loading the data into a SQLite table.

    Args:
        sqlite_conn: Already established connection to SQLite DB.
        input_data: See the output of :func:`load_data_into_pandas`.
    """
    logger.info("Going to process the whole statement and store it to the DB")

    input_data.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DB_QUERIES.insert_revolut_fees(sqlite_conn)

    dividends_df = pd.DataFrame(
        DB_QUERIES.get_revolut_dividends(sqlite_conn),
        columns=[
            "id",
            "Date",
            "Type",
            "Item",
            "Currency",
            "Units",
            "PPU",
            "Fees",
            "Taxes",
            "StockSplitRatio",
            "TotalAmount",
            "Remarks",
        ],
    )
    dividends_df = dividends_df.apply(produce_missing_dividend_details, axis=1)

    transactions_df = pd.DataFrame(
        DB_QUERIES.get_revolut_transactions(sqlite_conn),
        columns=[
            "id",
            "Date",
            "Type",
            "Item",
            "Currency",
            "Units",
            "PPU",
            "Fees",
            "Taxes",
            "StockSplitRatio",
            "Remarks",
        ],
    )

    stock_splits_df = pd.DataFrame(
        DB_QUERIES.get_revolut_stock_splits(sqlite_conn),
        columns=[
            "id",
            "Date",
            "Type",
            "Item",
            "Currency",
            "Units",
            "PPU",
            "Fees",
            "Taxes",
            "TotalUnitsAfterSplit",
            "StockSplitRatio",
            "Remarks",
        ],
    )
    stock_splits_df = stock_splits_df.apply(produce_missing_stock_split_details, axis=1)

    sqlite_conn.execute("DROP TABLE tmp_table")

    for curr_df in (dividends_df, transactions_df, stock_splits_df):
        curr_df = curr_df.apply(replace_renamed_ticker_symbols, axis=1)
        curr_df = curr_df.apply(produce_missing_exchange_prefixes_for_tickers, axis=1)
        curr_df.to_sql("tmp_table_transactions_ready", sqlite_conn, index=False, if_exists="replace")

        DB_QUERIES.insert_revolut_transactions(sqlite_conn)
        sqlite_conn.execute("DROP TABLE tmp_table_transactions_ready")


def process(input_directory: str, output_db_location: str) -> None:
    """This is the main function for the whole Revolut statement processing.

    Args:
        input_directory: See func:`load_data`.
        output_db_location: Full path name to the output SQlite DB.

    Raises:
        Exception: If the processing or loading data into DB wasn't successful.
    """
    logger.info("The processing of %s just started.", __name__)

    input_data = load_data_into_pandas(input_directory)

    with sqlite3.connect(output_db_location) as connection:
        try:
            load_data_to_db(connection, input_data)
        except Exception as err:
            connection.execute("DROP TABLE tmp_table")
            raise Exception(
                f"The processing of {__name__} wasn't successful. Further details:\n{err}",
            ) from err

    logger.info("The processing of %s just finished.", __name__)
