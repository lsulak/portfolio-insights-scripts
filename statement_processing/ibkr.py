# -*- coding: utf-8 -*-
"""IBKR CSV statement processing. It requires a directory of CSV reports exported from the
platform and it outputs easy-to-consume data into SQLite database.

The original IBKR CSV file actually contains a lot of sections - it almost looks like
there is a couple of CSV files inside, because the individual 'sections' have different
number of columns and they might have different meaning but the same name, depending on
the section.

Furthermore, this script, by its design, is able to load lots of input reports
sequentially - time-series data they hold can even overlap. This script is bulletproof
enough to handle such cases. For example, let's have 3 input files, where file names
indicate the date ranges of transactions they hold:

* 2021-01-01_2022-01-01.csv
* 2021-12-01_2022-12-31.csv
* 2023-01-01_2023-05-05.csv

As you can see, the first and the second file overlap, so there will be some duplicated
transactions. This is fine, duplicates will be detected and removed. Thus, you don't
have to export non-overlapping reports from the IBKR.

Important: At the time of writing this documentation, I was not trading with bonds or
futures and so this script would presumably filter out such transactions; this
functionality might be implemented in the future though.
"""
import glob
import io
import logging.config
import re
import sqlite3
from collections import defaultdict
from hashlib import md5
from typing import Dict, List, Tuple

import pandas as pd

from .constants import DB_QUERIES, IBKRReportsProcessingConst
from .database_utils import create_id_for_each_row
from .stocks_utils import (
    produce_missing_exchange_prefixes_for_tickers,
    replace_renamed_ticker_symbols,
)

T_AGGREGATED_RAW_DATA = Dict[Tuple[str, str], List[str]]  # { ( <section name>, <section hash> ): <data> }
T_SEMI_PROCESSED_DATA = Dict[str, pd.DataFrame]
T_PROCESSED_DATA = Dict[str, pd.DataFrame]

logger = logging.getLogger(__name__)


def aggregate_input_files(input_directory: str, remove_totals: bool = True) -> T_AGGREGATED_RAW_DATA:
    """Process all the reports from the input directory and save their data
    into a dictionary of lists. These files are not massive and can easily
    fit into the memory.

    Args:
        input_directory: A directory with the input IBKR reports (one or more CSV files).

    Returns:
        Dictionary where each key represents a particular report type
        (e.g. Dividends, Trades, Fees, etc) and value holds a list of
        transactions associated with a given report type
        (these are individual lines from the input CSV from the IBKR).
    """
    split_files = defaultdict(list)
    skipped_sections = {}

    for input_file in glob.glob(f"{input_directory}/*.csv"):
        logger.info("Going to load the following file: %s", input_file)

        with open(input_file, "r", encoding=IBKRReportsProcessingConst.IN_STAT_FILE_ENCODING) as input_filehandler:

            for line in input_filehandler:
                line_items = line.split(IBKRReportsProcessingConst.IN_STAT_FILE_DELIMITER)

                section_name = line_items[0].strip()
                if line_items[1].strip() == "Header":
                    section_hash = md5(line.encode("utf-8")).hexdigest()

                elif remove_totals and (
                    "total" in line_items[1].strip().lower() or "total" in line_items[2].strip().lower()
                ):
                    continue

                if section_name not in IBKRReportsProcessingConst.MAP_SECTION_TO_DESIRED_COLUMNS.keys():
                    skipped_sections[section_hash] = line
                    continue

                split_files[(section_name, section_hash)].append(line)

    if skipped_sections:
        logger.debug(
            "The following sections were skipped from all the CSV reports:\n%s",
            "\n".join(skipped_sections.values()),
        )

    return split_files


def preprocess_data(input_data: T_AGGREGATED_RAW_DATA) -> T_SEMI_PROCESSED_DATA:
    """Process sections (=report types) sequentially, and store data items only
    into a dictionary of DataFrames.

    Keep only desired columns, specified in `MAP_SECTION_TO_DESIRED_COLUMNS` and remove rows that
    do not hold data (e.g. remove 'Totals' and metadata rows).

    Note: There can be actually duplicates directly even within the same IBKR statement
        report (=slightly different statements, but they could hold the same transactions
        more than once - depends on how you configure the export on IBKR's side.
        Or, a user of this script can provide IBKR CSV files with overlapping time
        periods, which could be the source of duplication.
        Duplicates are not desired because the final portfolio value wouldn't be precise.

    Args:
        input_data: See the return value of :func:`aggregate_input_files`.
        remove_totals: The input IBKR report actually contains more than just
            individual data. It contains various 'totals' records, e.g. summaries
            of Buys for particular stock. These are not needed, but we can
            optionally keep them for debugging purposes.

    Returns:
        This is very similar to :func:`aggregate_input_files` but it outputs
        pre-filtered and de-duplicated data as a dictionary that holds Pandas
        DataFrames.
    """
    all_dfs = defaultdict(pd.DataFrame)

    for (section_name, section_hash), section_content in input_data.items():
        logger.info(
            "Going to preprocess and prefilter data from the section: %s (hash: %s)",
            section_name,
            section_hash,
        )

        section_as_str = "".join(section_content)

        try:
            section_as_df = pd.read_csv(io.StringIO(section_as_str))
        except pd.errors.ParserError:
            cols_numbers = list(
                map(lambda row: len(row.split(IBKRReportsProcessingConst.IN_STAT_FILE_DELIMITER)), section_content)
            )
            logger.warning(
                f"Skipping bad lines from section '{section_name}'. Number of columns in "
                f"this section, per row: {cols_numbers} (mismatch means extra column or separator!). "
                f"Section content, please debug if we are missing some important data "
                f"(outliers were skipped):\n{section_as_str}"
            )
            section_as_df = pd.read_csv(io.StringIO(section_as_str), on_bad_lines="skip")

        prefiltered_data = section_as_df.query('Header == "Data"').filter(
            items=IBKRReportsProcessingConst.MAP_SECTION_TO_DESIRED_COLUMNS[section_name].keys()
        )

        prefiltered_data.columns = prefiltered_data.columns.str.replace(" ", "")

        # Some reports can have missing columns. Maybe a user didn't export transaction
        # fees - maybe there were no such fees - but we accept such statements here.
        missing_cols = set(IBKRReportsProcessingConst.MAP_SECTION_TO_DESIRED_COLUMNS[section_name].keys()) - set(
            prefiltered_data.columns
        )
        for missing_col in missing_cols:
            prefiltered_data[missing_col] = None

        # Remove ',' from numbers - otherwise they are considered to be strings, but we need FLOAT representation.
        for col_name, col_type in IBKRReportsProcessingConst.MAP_SECTION_TO_DESIRED_COLUMNS[section_name].items():
            if col_type == float and prefiltered_data[col_name].dtype != float:
                if prefiltered_data[col_name].dtype == int:
                    continue

                prefiltered_data[col_name] = prefiltered_data[col_name].str.replace(",", "")

        prefiltered_data = prefiltered_data.astype(
            IBKRReportsProcessingConst.MAP_SECTION_TO_DESIRED_COLUMNS[section_name]
        )
        prefiltered_data = create_id_for_each_row(prefiltered_data)

        # There might be multiple sections (=reports) with the same section name, but the data
        # could slightly differ (some extra columns or so) - this will merge all reports
        # that should represent the same information together.
        all_dfs[section_name] = pd.concat(
            [all_dfs[section_name], prefiltered_data],
            ignore_index=True,
        ).drop_duplicates()

    return all_dfs


def load_deposits_and_withdrawals_to_db(
    sqlite_conn: sqlite3.Connection, deposits_and_withdrawals: pd.DataFrame
) -> None:
    """This function identifies deposit and withdrawal events and loads them into
    a SQLite table.

    Args:
        sqlite_conn: Already established connection to SQLite DB.
        deposits_and_withdrawals: Data related to deposits and withdrawals as a Pandas DataFrame.
    """
    logger.info("Going to process deposits and withdrawals information and store it to the DB")

    if deposits_and_withdrawals.empty:
        return

    deposits_and_withdrawals.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DB_QUERIES.insert_ibkr_deposits_and_withdrawals(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")


def load_forex_transactions_to_db(sqlite_conn: sqlite3.Connection, forex_transactions: pd.DataFrame) -> None:
    """This function identifies forex transactions and loads them into a SQLite table.

    Args:
        sqlite_conn: See :func:`load_deposits_and_withdrawals_to_db`.
        forex_transactions: Data related to forex transactions as a Pandas DataFrame.
    """
    logger.info("Going to process forex transactions and store it to the DB")

    if forex_transactions.empty:
        return

    forex_transactions.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DB_QUERIES.insert_ibkr_forex(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")


def load_special_fees_to_db(sqlite_conn: sqlite3.Connection, special_fees: T_SEMI_PROCESSED_DATA) -> None:
    """This function identifies special fees-related data and loads them into a SQLite table.

    Args:
        sqlite_conn: See :func:`load_deposits_and_withdrawals_to_db`.
        special_fees: Data related to special fees as a Pandas DataFrame.
    """
    logger.info("Going to process special fees transactions information and store it to the DB")

    if special_fees.empty:
        return

    special_fees.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DB_QUERIES.insert_ibkr_special_fees(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")


def load_stock_transactions_to_db(
    sqlite_conn: sqlite3.Connection, stock_transactions: pd.DataFrame, transaction_fees: pd.DataFrame
) -> None:
    """This function inserts stock transactions as well as their fees into a SQLite table.

    Args:
        sqlite_conn: See :func:`load_deposits_and_withdrawals_to_db`.
        stock_transactions: Data related to stock transactions as a Pandas DataFrame.
        transaction_fees: Data related to transaction fees as a Pandas DataFrame.
    """
    logger.info("Going to process stock transactions information and store it to the DB")

    if stock_transactions.empty:
        return

    stock_transactions = stock_transactions.apply(
        lambda x: replace_renamed_ticker_symbols(x, ticker_column="Symbol"), axis=1
    )
    stock_transactions = stock_transactions.apply(
        lambda x: produce_missing_exchange_prefixes_for_tickers(x, ticker_column="Symbol"), axis=1
    )

    if transaction_fees.empty:
        stock_transactions.to_sql("tmp_table_stocks", sqlite_conn, index=False, if_exists="replace")
        DB_QUERIES.insert_ibkr_transactions_without_special_fees(sqlite_conn)
        sqlite_conn.execute("DROP TABLE tmp_table_stocks")
        return

    transaction_fees = transaction_fees.apply(
        lambda x: replace_renamed_ticker_symbols(x, ticker_column="Symbol"), axis=1
    )
    transaction_fees = transaction_fees.apply(
        lambda x: produce_missing_exchange_prefixes_for_tickers(x, ticker_column="Symbol"), axis=1
    )

    stock_transactions.to_sql("tmp_table_stocks", sqlite_conn, index=False, if_exists="replace")
    transaction_fees.to_sql("tmp_table_tran_fees", sqlite_conn, index=False, if_exists="replace")

    DB_QUERIES.insert_ibkr_transactions_with_special_fees(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table_stocks")
    sqlite_conn.execute("DROP TABLE tmp_table_tran_fees")


def load_dividends_to_db(
    sqlite_conn: sqlite3.Connection, input_dividends: pd.DataFrame, input_taxes: pd.DataFrame
) -> None:
    """This function inserts dividend related data into a SQLite table.

    Args:
        sqlite_conn: See :func:`load_deposits_and_withdrawals_to_db`.
        input_dividends: Data related to received dividends as a Pandas DataFrame.
        input_taxes: Data related to dividend taxes as a Pandas DataFrame.

    Raises:
        Exception: if there are unexpected duplicates in the dataset or situations
            which we haven't anticipated, such as different kinds of fees.
    """

    def get_dividend_item(item):
        try:
            return re.match(IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, item.strip()).group(1)
        except:
            logger.error(f"Item {item} seems to have unsupported data, Dividend Item couldn't be obtained")

    def get_dividend_ppu(item):
        try:
            return float(re.match(IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, item.strip()).group(2))
        except:
            logger.error(f"Item {item} seems to have unsupported data, Dividend PPU couldn't be obtained")
            return 0.0

    logger.info("Going to process dividend information and store it to the DB")

    if input_dividends.empty:
        return

    input_dividends["Item"] = input_dividends["Description"].apply(get_dividend_item)
    input_dividends["PPU"] = input_dividends["Description"].apply(get_dividend_ppu)

    input_dividends.to_sql("tmp_table_dividends", sqlite_conn, index=False, if_exists="replace")

    deduped_dividends = pd.DataFrame(
        DB_QUERIES.select_deduped_dividends_received(sqlite_conn),
        columns=["id", "Currency", "Date", "Item", "PPU", "Amount"],
    )
    no_negative_dividends_received_validation = deduped_dividends.query("Amount < 0")

    if not no_negative_dividends_received_validation.empty:
        raise Exception(
            f"There are multiple dividend records that have negative received amount - but the dividend is "
            f"a receivable item, it should be always positive! IBKR had this bug in the past in "
            f"their CSV reports. Perhaps you'll need to manually correct the CSV statements. "
            f"Please investigate:\n{no_negative_dividends_received_validation}"
        )

    deduped_dividends = deduped_dividends.apply(replace_renamed_ticker_symbols, axis=1)
    deduped_dividends = deduped_dividends.apply(produce_missing_exchange_prefixes_for_tickers, axis=1)

    deduped_dividends.to_sql("tmp_table_deduped_dividends", sqlite_conn, index=False, if_exists="replace")

    if not input_taxes.empty:
        input_taxes["Item"] = input_taxes["Description"].apply(get_dividend_item)
        input_taxes["PPU"] = input_taxes["Description"].apply(get_dividend_ppu)

        input_taxes = input_taxes.query(
            "PPU > 0.0"
        )  # Ignore dividends on margin, at least for now; they have PPU = 0.0

        input_taxes.to_sql("tmp_table_div_taxes", sqlite_conn, index=False, if_exists="replace")

        deduped_taxes = pd.DataFrame(
            DB_QUERIES.select_deduped_dividend_taxes(sqlite_conn),
            columns=["id", "Currency", "Date", "Item", "PPU", "Amount"],
        )
        no_positive_dividend_taxes_validation = deduped_taxes.query("Amount > 0")

        if not no_positive_dividend_taxes_validation.empty:
            raise Exception(
                f"There are multiple dividend tax records that have positive tax amount - but the tax is "
                f"not a receivable item, it should be always negative! IBKR had this bug in the past in "
                f"their CSV reports. Perhaps you'll need to manually correct the CSV statements. "
                f"Please investigate:\n{no_positive_dividend_taxes_validation}"
            )

        deduped_taxes = deduped_taxes.apply(replace_renamed_ticker_symbols, axis=1)
        deduped_taxes = deduped_taxes.apply(produce_missing_exchange_prefixes_for_tickers, axis=1)

        deduped_taxes.to_sql("tmp_table_deduped_taxes", sqlite_conn, index=False, if_exists="replace")

        DB_QUERIES.insert_dividend_records_with_taxes(sqlite_conn)

        sqlite_conn.execute("DROP TABLE tmp_table_div_taxes")
        sqlite_conn.execute("DROP TABLE tmp_table_deduped_taxes")

    else:
        DB_QUERIES.insert_dividend_records_without_taxes(sqlite_conn)

    duplicit_dividend_records_validation = DB_QUERIES.validate_duplicit_dividend_records(sqlite_conn)
    if duplicit_dividend_records_validation:
        raise Exception(
            f"There are duplicit dividend records for the same ticker and "
            f"the same day, this shouldn't happen. "
            f"Please investigate:\n{duplicit_dividend_records_validation}"
        )

    sqlite_conn.execute("DROP TABLE tmp_table_dividends")
    sqlite_conn.execute("DROP TABLE tmp_table_deduped_dividends")


def drop_all_tmp_tables(sqlite_conn: sqlite3.Connection) -> None:
    """This function drops all temporary tables that are present in the DB.

    Args:
        sqlite_conn: See :func:`load_deposits_and_withdrawals_to_db`.
    """
    logger.info("Going to drop all temporary tables.")

    tables_to_drop = DB_QUERIES.list_all_tmp_tables(sqlite_conn)
    for table_to_drop in tables_to_drop:
        sqlite_conn.execute(f"DROP TABLE {table_to_drop[0]}")


def process(input_directory: str, output_db_location: str) -> None:
    """This is the main function for the whole IBKR statement processing.

    Args:
        input_directory: See func:`aggregate_input_files`.
        output_db_location: Full path name to the output SQlite DB.
    """
    logger.info("The processing of %s just started.", __name__)

    input_data_by_section = aggregate_input_files(input_directory)
    semi_processed_data = preprocess_data(input_data_by_section)

    with sqlite3.connect(output_db_location) as connection:
        try:
            load_deposits_and_withdrawals_to_db(connection, semi_processed_data["Deposits & Withdrawals"])
            load_forex_transactions_to_db(connection, semi_processed_data["Trades"].query("AssetCategory == 'Forex'"))
            load_special_fees_to_db(connection, semi_processed_data["Other Fees"])
            load_stock_transactions_to_db(
                connection,
                semi_processed_data["Trades"].query("AssetCategory == 'Stocks'"),
                semi_processed_data["Transaction Fees"],
            )
            load_dividends_to_db(connection, semi_processed_data["Dividends"], semi_processed_data["Withholding Tax"])

        except Exception as err:
            drop_all_tmp_tables(connection)
            raise Exception(
                f"The processing of {__name__} wasn't successful. Further details:\n{err}",
            ) from err

    logger.info("The processing of %s just finished.", __name__)
