# -*- coding: utf-8 -*-
"""Coinbase Pro statement processing. It requires a directory of CSV reports exported from the
platform and it outputs easy-to-consume data into SQLite database.
"""
import logging
import sqlite3
from glob import glob
from hashlib import md5

import pandas as pd
from currencies import MONEY_FORMATS

from .constants import (  # isort:skip
    DEFAULT_COINBASE_PORTFOLIO_CURRENCY,
    DBConstants,
)

logger = logging.getLogger(__name__)


def load_data_into_pandas(input_directory: str) -> pd.DataFrame:
    """This function loads the CSV files from the input directory into de-duplicated
    pandas dataframe.

    Data relevant to a given user (i.e. balance or portfolio ID) are removed.

    Args:
        input_directory: Directory with the Coinbase CSV statements
            exported manually by a user.

    Returns:
        Pandas Dataframe that holds all the desired data.
    """
    all_dataframes = []

    for input_file_name in glob(f"{input_directory}/*.csv"):
        logger.info("Going to load the following file: %s", input_file_name)

        input_df = pd.read_csv(input_file_name, index_col=None, header=0)
        input_df.drop(columns=["portfolio", "balance"], inplace=True)

        # pylint: disable=bad-builtin
        input_df["id"] = input_df.apply(
            lambda x: md5("".join(map(str, x)).encode("utf-8")).hexdigest(),
            axis=1,
        )
        # pylint: enable=bad-builtin

        all_dataframes.append(input_df)

    all_data_as_df = pd.concat(all_dataframes, axis=0, ignore_index=True).drop_duplicates()
    all_data_as_df.columns = all_data_as_df.columns.str.replace(" ", "")
    return all_data_as_df


def process_transactions(input_data: pd.DataFrame) -> pd.DataFrame:
    """This function processes transaction only data.

    Args:
        input_data: See the output of :func:`load_data_into_pandas`.

    Returns:
        Pandas Dataframe containing well-shaped transaction data.
    """
    logger.info("Going to process all the transaction data.")

    fees = input_data.query("type == 'fee'")[:]
    fees.rename(columns={"amount/balanceunit": "Currency", "amount": "Fees"}, inplace=True)
    fees.drop(columns=["type"], inplace=True)
    fees.Fees = abs(fees.Fees)

    transactions = input_data.query("type == 'match'")[:]
    transactions.rename(columns={"amount/balanceunit": "Item"}, inplace=True)
    transactions.Currency = DEFAULT_COINBASE_PORTFOLIO_CURRENCY

    crypto_details = transactions.query(f"Item not in {list(MONEY_FORMATS.keys())}")[:]
    crypto_details.rename(columns={"amount": "Units"}, inplace=True)

    fiat_details = transactions.query(f"Item in {list(MONEY_FORMATS.keys())}")[:]
    fiat_details.rename(columns={"Item": "Currency"}, inplace=True)
    fiat_details.amount = abs(fiat_details.amount)

    aggregated_df = crypto_details.merge(
        fiat_details, how="inner", on=["time", "type", "tradeid", "orderid"]
    )
    aggregated_df["PPU"] = aggregated_df.amount.div(aggregated_df.Units.values)
    aggregated_df.type = aggregated_df.Units.apply(lambda x: "Buy" if x > 0 else "Sell")

    final_transactions = aggregated_df.merge(
        fees, how="inner", on=["time", "Currency", "tradeid", "orderid"]
    )
    return final_transactions


def load_transactions_to_db(
    sqlite_conn: sqlite3.Connection, transaction_data: pd.DataFrame
) -> None:
    """This function loads the transaction data into a SQLite table.

    Args:
        sqlite_conn: Already established connection to SQLite DB.
        transaction_data: See the output of the :func:`process_transactions`.
    """
    logger.info(
        "Going to store all the transaction data into table %s",
        DBConstants.TRANSACTIONS_TABLE,
    )

    transaction_data.to_sql(
        "tmp_table",
        sqlite_conn,
        index=False,
    )

    sqlite_conn.execute(
        f"""
        INSERT INTO {DBConstants.TRANSACTIONS_TABLE}
        SELECT id,
               DATE(time) AS Date,
               Type,
               Item,
               Units,
               Currency,
               PPU,
               Fees,
               '' AS Remarks
          FROM tmp_table

        -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
        WHERE TRUE

        -- Overlapping statements or processing of the same input file twice
        -- is all allowed. But duplicates are not allowed.
        ON CONFLICT(id) DO NOTHING
        """
    )
    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def load_deposits_and_withdrawals_to_db(
    sqlite_conn: sqlite3.Connection, input_data: pd.Dataframe, include_fiat_only: bool = True
) -> None:
    """This function identifies deposit and withdrawal events and loads them into
    a SQLite table.

    Args:
        sqlite_conn: See :func:`load_transactions_to_db`.
        input_data: See the output of :func:`load_data_into_pandas`.
        include_fiat_only: Optionally you can choose to keep deposits and
            withdrawals of Crypto currencies as well.
            By default, only deposits & withdrawals of the fiat currencies
            are kept.
    """
    logger.info(
        "Going to process deposits and withdrawals information and store it to table %s",
        DBConstants.DEP_AND_WITHDRAWALS_TABLE,
    )

    input_data.to_sql(
        "tmp_table",
        sqlite_conn,
        index=False,
    )

    if include_fiat_only:
        # pylint: disable=bad-builtin
        printable_fiat = "('" + "','".join(map(str, MONEY_FORMATS.keys())) + "')"
        fiat_filter = f" AND `amount/balanceunit` in {printable_fiat} "
    else:
        fiat_filter = ""

    sqlite_conn.execute(
        f"""
        INSERT INTO {DBConstants.DEP_AND_WITHDRAWALS_TABLE}
        SELECT id,
               DATE(time) AS Date,
               UPPER(SUBSTR(type, 1, 1)) || SUBSTR(type, 2) AS Type,
               `amount/balanceunit` AS Currency,
               CAST(amount AS DOUBLE) AS Amount
          FROM tmp_table
         WHERE type IN ('deposit', 'withdrawal') {fiat_filter}
      ORDER BY time

        -- Overlapping statements or processing of the same input file twice
        -- is all allowed. But duplicates are not allowed.
        ON CONFLICT(id) DO NOTHING
        """
    )
    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def process(input_directory: str) -> None:
    """This is the main function for the whole Coinbase statement processing.

    Args:
        input_directory: See func:`load_data_into_pandas`.

    Raises:
        Exception: If the processing or loading data into DB wasn't successful.
    """
    logger.info("The processing of %s just started.", __name__)

    input_data = load_data_into_pandas(input_directory)
    transactions_data = process_transactions(input_data)

    with sqlite3.connect(f"{DBConstants.STATEMENTS_DB}") as connection:
        try:
            load_transactions_to_db(connection, transactions_data)
            load_deposits_and_withdrawals_to_db(connection, input_data)
        except Exception as err:
            connection.execute("DROP TABLE tmp_table")
            raise Exception(
                f"The processing of {__name__} wasn't successful. Further details:\n{err}",
            ) from err

    logger.info("The processing of %s just finished.", __name__)
