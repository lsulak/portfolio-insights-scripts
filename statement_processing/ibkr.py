# -*- coding: utf-8 -*-
"""This script loads and processes CSV reports, that were manually exported from the
Interactive Brokers platform, and produces three CSV files
(time-series data, ordered by date):

* `deposits_and_withdrawals.csv`: information about deposit/withdrawals
* `forex.csv`: currency exchange transactions
* `securities.csv`: transaction data related to investment securities

The goal of this script is to have a convenient way of loading all transactions into
a CSV files that will be later manually imported into my personal finance sheet on Google
Docs. I needed to have a consolidated view on the overall investment portfolio that goes
beyond a single broker by loading and reshaping the statement(s) into a time-series data
where each line represents a single buy/sell/dividends/fees transaction.

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

from .constants import DBConstants, IBKRReportsProcessingConst

T_AGGREGATED_RAW_DATA = Dict[Tuple[str, str], List[str]]  # { ( <section name>, <section hash> ): <data> }
T_SEMI_PROCESSED_DATA = Dict[str, pd.DataFrame]
T_PROCESSED_DATA = Dict[str, pd.DataFrame]

logger = logging.getLogger(__name__)

IS_DIGIT = re.compile(r"^\d+(?:[,.]\d*)?$")


def custom_hash_func(row):
    row_as_str = ""
    for col in row:
        col_as_str = str(col)

        if IS_DIGIT.match(col_as_str):
            row_as_str += str(round(float(col), 4))
        else:
            row_as_str += col_as_str

    return md5(row_as_str.encode("utf-8")).hexdigest()


def aggregate_input_files(input_directory: str) -> T_AGGREGATED_RAW_DATA:
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


def preprocess_data(input_data: T_AGGREGATED_RAW_DATA, remove_totals: bool = True) -> T_SEMI_PROCESSED_DATA:
    """Process sections (=report types) sequentially, and store data items only
    into a dictionary of dataframes.

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
        Dataframes.
    """
    all_dfs = defaultdict(pd.DataFrame)

    for (section_name, section_hash), section_content in input_data.items():
        logger.info(
            "Going to preprocess and prefilter data from the section: %s (hash: %s)",
            section_name,
            section_hash,
        )

        section_as_df = pd.read_csv(io.StringIO("".join(section_content)))

        prefiltered_data = section_as_df.query('Header == "Data"').filter(
            items=IBKRReportsProcessingConst.MAP_SECTION_TO_DESIRED_COLUMNS[section_name].keys()
        )

        if remove_totals:
            # Concatenate the row so that it's just a single string and search
            # for particular regex pattern; filter out the row on match.
            idx_of_totals = prefiltered_data.iloc[:, 0].str.contains("^Total.*$", na=False)
            prefiltered_data = prefiltered_data[~idx_of_totals]

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
                prefiltered_data[col_name] = prefiltered_data[col_name].str.replace(",", "")

        prefiltered_data = prefiltered_data.astype(
            IBKRReportsProcessingConst.MAP_SECTION_TO_DESIRED_COLUMNS[section_name]
        )

        prefiltered_data["id"] = prefiltered_data.apply(lambda x: custom_hash_func(x), axis=1)

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
        deposits_and_withdrawals: Data related to deposits and withdrawals as a Pandas Dataframe.
    """
    logger.info("Going to process deposits and withdrawals information and store it to the DB")

    if deposits_and_withdrawals.empty:
        return

    deposits_and_withdrawals.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DBConstants.QUERIES.insert_ibkr_deposits_and_withdrawals(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def load_forex_transactions_to_db(sqlite_conn: sqlite3.Connection, forex_transactions: pd.DataFrame) -> None:
    """This function identifies forex transactions and loads them into a SQLite table.

    Args:
        sqlite_conn: See :func:`load_deposits_and_withdrawals_to_db`.
        forex_transactions: Data related to forex transactions as a Pandas Dataframe.
    """
    logger.info("Going to process forex transactions and store it to the DB")

    if forex_transactions.empty:
        return

    forex_transactions.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DBConstants.QUERIES.insert_ibkr_forex(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def load_special_fees_to_db(sqlite_conn: sqlite3.Connection, special_fees: T_SEMI_PROCESSED_DATA) -> None:
    """This function identifies special fees-related data and loads them into a SQLite table.

    Args:
        sqlite_conn: See :func:`load_deposits_and_withdrawals_to_db`.
        special_fees: Data related to special fees data as a Pandas Dataframe.
    """
    logger.info("Going to process special fees transactions information and store it to the DB")

    special_fees.to_sql("tmp_table", sqlite_conn, index=False, if_exists="replace")

    DBConstants.QUERIES.insert_ibkr_special_fees(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def load_stock_transactions_to_db(
    sqlite_conn: sqlite3.Connection, stock_transactions: pd.DataFrame, transaction_fees: pd.DataFrame
) -> None:
    """TODO"""
    logger.info("Going to process stock transactions information and store it to the DB")

    stock_transactions.to_sql("tmp_table_stocks", sqlite_conn, index=False, if_exists="replace")
    transaction_fees.to_sql("tmp_table_tran_fees", sqlite_conn, index=False, if_exists="replace")

    DBConstants.QUERIES.insert_ibkr_transactions(sqlite_conn)

    sqlite_conn.execute("DROP TABLE tmp_table_stocks")
    sqlite_conn.execute("DROP TABLE tmp_table_tran_fees")

    sqlite_conn.commit()


def load_dividends_to_db(
    sqlite_conn: sqlite3.Connection, input_dividends: pd.DataFrame, input_taxes: pd.DataFrame
) -> None:
    """This function reshapes and enhances the data from the IBKR reports based on
    particular business logic we need.

    Args:
        input_data: see the output of the function :func:`put_data_into_df`.

    Returns:
        The output type is the same as the input parameter, but there were some row
        modifications of the data (column additions & removals, changing data types,
        grouping, filtering out irrelevant records, and more).

    Raises:
        Exception: if there are unexpected duplicates in the dataset or situations
            which we haven't anticipated, such as different kinds of fees.
    """
    logger.info("Going to process dividend information and store it to the DB")

    input_dividends["Item"] = input_dividends["Description"].apply(
        lambda x: re.match(IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, x.strip()).group(1)
    )
    input_dividends["PPU"] = input_dividends["Description"].apply(
        lambda x: float(re.match(IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, x.strip()).group(2))
    )
    # sqlite_conn.execute("DROP TABLE tmp_table_dividends")
    # sqlite_conn.execute("DROP TABLE tmp_table_div_taxes")
    # sqlite_conn.execute("DROP TABLE tmp_records_to_insert")

    input_dividends.to_sql("tmp_table_dividends", sqlite_conn, index=False)

    input_taxes["PPU"] = input_taxes["Description"].apply(
        lambda x: float(re.match(r".+?Cash Dividend .+ ([0-9.]+) per", x.strip()).group(1))
    )
    input_taxes["Item"] = input_taxes["Description"].apply(lambda x: re.match(r".+?\(", x.strip()).group()[:-1])
    input_taxes.to_sql("tmp_table_div_taxes", sqlite_conn, index=False, if_exists="replace")

    records_to_insert = pd.read_sql_query(
        f"""
        WITH dedup_dividends AS (
                -- Sometimes there can be a false positives
                -- (i.e. dividends -$10, then +$10 and then -$5, so the final would be $5).
                SELECT MIN(id) AS id, -- doesn't matter
                       Currency,
                       Date,
                       Item,
                       PPU,
                       SUM(Amount) Amount
                  FROM tmp_table_dividends
              GROUP BY Currency, Date, Item, PPU

            ), dedup_div_taxes AS (
                -- Sometimes there can be a false positives
                -- (i.e. tax -$10, then +$10 and then -$5, so the final would be $5).
                SELECT MIN(id) AS id, -- doesn't matter
                       Currency,
                       Date,
                       Item,
                       PPU,
                       SUM(Amount) Amount
                  FROM tmp_table_div_taxes
                 WHERE NOT (Item = 'STOR' AND strftime('%Y', Date) = '2021' AND Amount < 0)
              GROUP BY Currency, Date, Item, PPU
            )

             SELECT DISTINCT
                    div.id,
                    div.Date,
                    'DIVIDENDS' AS Type,
                    div.Item,
                    ROUND(div.Amount / div.PPU, 4) AS Units,
                    div.Currency,
                    ROUND(div.PPU, 4) AS PPU,
                    0 AS Fees,
                    ROUND(COALESCE(tax.Amount, 0), 4) as Taxes,
                    1.0 AS StockSplitRatio,
                    '' AS Remarks

               FROM dedup_dividends AS div

          LEFT JOIN dedup_div_taxes AS tax
                 ON div.Currency = tax.Currency
                AND div.Date = tax.Date
                AND div.PPU = tax.PPU
                AND div.Item = tax.Item

           ORDER BY div.date
        """,
        sqlite_conn,
    )
    records_to_insert.to_sql("tmp_records_to_insert", sqlite_conn, index=False, if_exists="replace")

    omg = pd.read_sql_query(
        f"""
        SELECT * FROM tmp_records_to_insert
    """,
        sqlite_conn,
    )
    print(omg)

    multiple_dividend_records_validation = pd.read_sql_query(
        f"""
        SELECT Date, Type, Item, PPU, COUNT(*)
          FROM tmp_records_to_insert
      GROUP BY Date, Item, PPU
        HAVING COUNT(*) > 1
    """,
        sqlite_conn,
    )

    if not multiple_dividend_records_validation.empty:
        raise Exception(
            f"There are multiple dividend records for the same ticker and "
            f"the same day, this shouldn't happen. "
            f"Please investigate:\n{multiple_dividend_records_validation}"
        )

    sqlite_conn.execute(
        f"""
        INSERT INTO transactions

        SELECT * FROM tmp_records_to_insert WHERE true

            -- Overlapping statements or processing of the same input file
            -- twice is all allowed. But duplicates are not allowed.
            ON CONFLICT(id) DO NOTHING
    """
    )

    sqlite_conn.execute("DROP TABLE tmp_table_dividends")
    sqlite_conn.execute("DROP TABLE tmp_table_div_taxes")
    sqlite_conn.execute("DROP TABLE tmp_records_to_insert")

    sqlite_conn.commit()


def process(input_directory: str) -> None:
    """This is the main function for the whole IBKR statement processing.

    Args:
        input_directory: See func:`aggregate_input_files`.
    """
    logger.info("The processing of %s just started.", __name__)

    input_data_by_section = aggregate_input_files(input_directory)
    semi_processed_data = preprocess_data(input_data_by_section)

    with sqlite3.connect(f"{DBConstants.STATEMENTS_DB}") as connection:
        try:
            load_deposits_and_withdrawals_to_db(connection, semi_processed_data["Deposits & Withdrawals"])
            load_forex_transactions_to_db(connection, semi_processed_data["Trades"].query("AssetCategory == 'Forex'"))
            load_special_fees_to_db(connection, semi_processed_data["Fees"])
        except Exception as err:
            connection.execute("DROP TABLE tmp_table")
            raise Exception(
                f"The processing of {__name__} wasn't successful. Further details:\n{err}",
            ) from err

        load_stock_transactions_to_db(
            connection,
            semi_processed_data["Trades"].query("AssetCategory == 'Stocks'"),
            semi_processed_data["Transaction Fees"],
        )
        load_dividends_to_db(connection, semi_processed_data["Dividends"], semi_processed_data["Withholding Tax"])

    logger.info("The processing of %s just finished.", __name__)
