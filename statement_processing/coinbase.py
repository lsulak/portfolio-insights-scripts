# -*- coding: utf-8 -*-
"""Coinbase statement processing. It requires a directory of CSV reports exported from the
platform and it outputs easy-to-consume data into SQLite database.
"""
import logging
import re
import sqlite3
from glob import glob
from hashlib import md5
from io import StringIO
from typing import Generator

import pandas as pd

from .constants import DBConstants

logger = logging.getLogger(__name__)

T_INPUT_DATA_GEN = Generator[str, None, None]


def load_data(input_directory: str) -> T_INPUT_DATA_GEN:
    """This function loads the CSV files from the input directory into memory
    in a lazy way using Generator.

    Args:
        input_directory: Directory with the Coinbase CSV statements
            exported manually by a user.

    Yields:
        Generator yielding individual lines from all input CSV reports.
    """
    header_already_seen = False

    for input_file_name in glob(f"{input_directory}/*.csv"):
        logger.info("Going to load the following file: %s", input_file_name)

        with open(input_file_name, mode="r", encoding="UTF-8") as input_file_handler:
            for line in input_file_handler:
                if line.startswith("Timestamp,"):
                    if header_already_seen:
                        continue  # we want to keep header just once

                    header_already_seen = True
                    yield line

                # Data row always starts with date column.
                elif re.match(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}", line):
                    yield line


def load_data_into_pandas(input_data: T_INPUT_DATA_GEN) -> pd.DataFrame:
    """This function loads the input data into Pandas Dataframe. It also de-duplicates the data,
    in case that the input reports contained duplicates (for example, they could be exported
    from time period).

    Args:
        input_data: See the output of the :func:`load_data`.

    Returns:
        Pandas Dataframe containing de-duplicated input data with MD5
        hash representation of each line.
    """
    input_df = pd.read_csv(StringIO("".join(input_data)))

    input_df.Fees = input_df.Fees.fillna(0)

    # pylint: disable=bad-builtin
    input_df["id"] = input_df.apply(
        lambda x: md5("".join(map(str, x)).encode("utf-8")).hexdigest(),
        axis=1,
    )
    # pylint: enable=bad-builtin

    input_df.columns = input_df.columns.str.replace(" ", "")
    return input_df.drop_duplicates()


def load_data_to_db(sqlite_conn: sqlite3.Connection, input_data: pd.DataFrame) -> None:
    """This is the main function that is responsible for loading the data into a SQLite table.

    Args:
        sqlite_conn: Already established connection to SQLite DB.
        input_data: See the output of :func:`load_data_into_pandas`.
    """
    logger.info(
        "Going to process the whole statement and store it to table %s",
        DBConstants.TRANSACTIONS_TABLE,
    )

    input_data.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    sqlite_conn.execute(
        f"""
        INSERT INTO {DBConstants.TRANSACTIONS_TABLE}
            SELECT id,
                DATE(Timestamp) AS Date,
                CASE TransactionType
                    WHEN 'Sell' THEN 'SELL'
                    ELSE 'BUY'
                END AS Type,
                Asset AS Item,
                QuantityTransacted AS Units,
                SpotPriceCurrency AS Currency,
                SpotPriceatTransaction AS PPU,
                CAST(Fees AS DOUBLE) AS Fees,
                Notes AS Remarks
            FROM tmp_table

            -- From doc, see 'Parsing Ambiguity': https://sqlite.org/lang_upsert.html
            WHERE TRUE

            -- Overlapping statements or processing of the same input file twice
            -- is all allowed. But duplicates are not allowed.
            ON CONFLICT(id) DO NOTHING
        """,
    )
    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def process(input_directory: str) -> None:
    """This is the main function for the whole Coinbase statement processing.

    Args:
        input_directory: See func:`load_data`.

    Raises:
        Exception: If the processing or loading data into DB wasn't successful.
    """
    logger.info("The processing of %s just started.", __name__)

    input_data = load_data(input_directory)
    semi_processed_data = load_data_into_pandas(input_data)

    with sqlite3.connect(f"{DBConstants.STATEMENTS_DB}") as connection:
        try:
            load_data_to_db(connection, semi_processed_data)
        except Exception as err:
            connection.execute("DROP TABLE tmp_table")
            raise Exception(
                f"The processing of {__name__} wasn't successful. Further details:\n{err}",
            ) from err

    logger.info("The processing of %s just finished.", __name__)
