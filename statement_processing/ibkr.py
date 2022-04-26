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
import pstats
import re
import sqlite3
from collections import defaultdict
from hashlib import md5
from typing import Dict, List, Tuple

import pandas as pd

from .constants import DATA_DIR, DBConstants, IBKRReportsProcessingConst

T_AGGREGATED_RAW_DATA = Dict[Tuple[str, str], List[str]]
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

        with open(
            input_file, "r", encoding=IBKRReportsProcessingConst.IN_STAT_FILE_ENCODING
        ) as input_filehandler:

            for line in input_filehandler:
                line_items = line.split(",")

                section_name = line_items[0].strip()
                if line_items[1].strip() == "Header":
                    section_hash = md5(line.encode("utf-8")).hexdigest()

                if section_name not in IBKRReportsProcessingConst.COLUMNS_TO_GET.keys():
                    skipped_sections[section_hash] = line
                    continue

                split_files[(section_name, section_hash)].append(line)

    if skipped_sections:
        logger.info(
            "The following sections were skipped from all the CSV reports:\n%s",
            "\n".join(skipped_sections.values()),
        )

    return split_files


def preprocess_data(
    input_data: T_AGGREGATED_RAW_DATA, remove_totals: bool = True
) -> T_SEMI_PROCESSED_DATA:
    """Process sections (=report types) sequentially, and store data items only
    into a dictionary of dataframes.

    Keep only desired columns, specified in `COLUMNS_TO_GET` and remove rows that
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
            items=IBKRReportsProcessingConst.COLUMNS_TO_GET[section_name].keys()
        )

        if remove_totals:
            # Concatenate the row so that it's just a single string and search
            # for particular regex pattern; filter out the row on match.
            idx_of_totals = prefiltered_data.iloc[:, 0].str.contains("^Total.*$", na=False)
            prefiltered_data = prefiltered_data[~idx_of_totals]

        prefiltered_data.columns = prefiltered_data.columns.str.replace(" ", "")

        # Some reports can have missing columns. Maybe a user didn't export transaction
        # fees - maybe there were no such fees - so we accept such statements here.
        missing_cols = set(IBKRReportsProcessingConst.COLUMNS_TO_GET[section_name].keys()) - set(
            prefiltered_data.columns
        )
        for missing_col in missing_cols:
            prefiltered_data[missing_col] = None

        for col_name, col_type in IBKRReportsProcessingConst.COLUMNS_TO_GET[section_name].items():
            if col_type == float and prefiltered_data[col_name].dtype != float:
                prefiltered_data[col_name] = prefiltered_data[col_name].str.replace(",", "")

        prefiltered_data = prefiltered_data.astype(
            IBKRReportsProcessingConst.COLUMNS_TO_GET[section_name]
        )

        prefiltered_data["id"] = prefiltered_data.apply(
            lambda x: custom_hash_func(x),
            axis=1,
        )

        all_dfs[section_name] = pd.concat(
            [all_dfs[section_name], prefiltered_data],
            ignore_index=True,
        ).drop_duplicates()

    return all_dfs


def load_deposits_and_withdrawals_to_db(
    sqlite_conn: sqlite3.Connection, input_data: pd.DataFrame
) -> None:

    logger.info(
        "Going to process deposits and withdrawals information and store it to table %s",
        DBConstants.DEP_AND_WITHDRAWALS_TABLE,
    )

    if input_data.empty:
        return

    input_data.to_sql(
        "tmp_table",
        sqlite_conn,
        index=False,
    )

    sqlite_conn.execute(
        f"""
        INSERT INTO {DBConstants.DEP_AND_WITHDRAWALS_TABLE}
        SELECT id,
            SettleDate AS Date,
            CASE
                WHEN Amount > 0 THEN 'Deposit'
                ELSE 'Withdrawal'
            END AS Type,
            Currency,
            ROUND(Amount, 2) AS Amount
        FROM tmp_table
    ORDER BY Date

            -- Overlapping statements or processing of the same input file
            -- twice is all allowed. But duplicates are not allowed.
            ON CONFLICT(id) DO NOTHING
        """
    )
    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def load_forex_transactions_to_db(
    sqlite_conn: sqlite3.Connection, input_data: pd.DataFrame
) -> None:
    logger.info(
        "Going to process Forex information and store it to table %s",
        DBConstants.FOREX_TABLE,
    )

    forex_transactions = input_data.query("AssetCategory == 'Forex'")
    if forex_transactions.empty:
        return

    forex_transactions.to_sql(
        "tmp_table",
        sqlite_conn,
        index=False,
    )

    sqlite_conn.execute(
        f"""
        INSERT INTO {DBConstants.FOREX_TABLE}
        SELECT id,
                DATE(REPLACE(`Date/Time`, ',', '')) AS Date,
                SUBSTR(Symbol, 0, 3) AS CurrencySold,
                Currency AS CurrencyBought,
                Symbol AS CurrencyPairCode,
                ROUND(Quantity, 2) AS CurrencySoldUnits,
                ROUND(`T.Price`, 2) AS PPU,
                ROUND(ComminUSD, 2) AS Fees
        FROM tmp_table
    ORDER BY Date

            -- Overlapping statements or processing of the same input file
            -- twice is all allowed. But duplicates are not allowed.
            ON CONFLICT(id) DO NOTHING
        """
    )
    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def load_stock_transactions_to_db(
    sqlite_conn: sqlite3.Connection, input_transactions: pd.DataFrame, input_fees: pd.DataFrame
) -> None:
    logger.info(
        "Going to process stock transactions information and store it to table %s",
        DBConstants.TRANSACTIONS_TABLE,
    )

    stock_transactions = input_transactions.query("AssetCategory == 'Stocks'")
    stock_transactions.to_sql(
        "tmp_table_stocks",
        sqlite_conn,
        index=False,
    )

    input_fees.to_sql(
        "tmp_table_tran_fees",
        sqlite_conn,
        index=False,
    )

    sqlite_conn.execute(
        f"""
        WITH records_to_insert AS (

             SELECT DISTINCT
                    stocks.id,
                    DATE(REPLACE(stocks.`Date/Time`, ',', '')) AS Date,
                    CASE
                        WHEN stocks.Quantity > 0 THEN 'BUY'
                        ELSE 'SELL'
                    END AS Type,
                    stocks.Symbol AS Item,
                    ROUND(stocks.Quantity, 2) AS Units,
                    stocks.Currency,
                    ROUND(stocks.`T.Price`, 2) AS PPU,
                    ROUND(stocks.`Comm/Fee`, 2) AS Fees,
                    ROUND(COALESCE(t_fees.Amount, 0), 2) as Taxes,
                    1.0 AS StockSplitRatio,
                    COALESCE(t_fees.Description, '') AS Remarks

               FROM tmp_table_stocks AS stocks

          LEFT JOIN tmp_table_tran_fees AS t_fees
                 ON stocks.Symbol = t_fees.Symbol
                AND stocks.`Date/Time` = t_fees.`Date/Time`
                AND stocks.Quantity = t_fees.Quantity
                AND stocks.`T.Price` = t_fees.TradePrice

           ORDER BY stocks.`Date/Time`
        )

        INSERT INTO {DBConstants.TRANSACTIONS_TABLE}

        SELECT * FROM records_to_insert WHERE true

            -- Overlapping statements or processing of the same input file
            -- twice is all allowed. But duplicates are not allowed.
            ON CONFLICT(id) DO NOTHING
        """
    )

    sqlite_conn.execute("DROP TABLE tmp_table_stocks")
    sqlite_conn.execute("DROP TABLE tmp_table_tran_fees")

    sqlite_conn.commit()


def load_special_fees_to_db(
    sqlite_conn: sqlite3.Connection, input_data: T_SEMI_PROCESSED_DATA
) -> None:
    logger.info(
        "Going to process special fees transactions information and store it to table %s",
        DBConstants.TRANSACTIONS_TABLE,
    )

    input_data.to_sql(
        "tmp_table",
        sqlite_conn,
        index=False,
    )

    sqlite_conn.execute(
        f"""
        INSERT INTO {DBConstants.TRANSACTIONS_TABLE}

            SELECT DISTINCT
                   id,
                   Date,
                   'FEES' AS Type,
                   'IBKR Fee' AS Item,
                   0.0 AS Units,
                   Currency,
                   0.0 PPU,
                   ROUND(Amount, 2) AS Fees,
                   '' AS Taxes,
                   1.0 AS StockSplitRatio,
                   '' AS Remarks

                FROM tmp_table

            ORDER BY Date

            -- Overlapping statements or processing of the same input file
            -- twice is all allowed. But duplicates are not allowed.
            ON CONFLICT(id) DO NOTHING
        """
    )

    sqlite_conn.execute("DROP TABLE tmp_table")
    sqlite_conn.commit()


def load_dividends_to_db(
    sqlite_conn: sqlite3.Connection, input_dividends: pd.DataFrame, input_taxes: pd.DataFrame
) -> None:
    # TODO! Groupnut oba datasety a vymazat duplikaty!
    logger.info(
        "Going to process dividend information and store it to table %s",
        DBConstants.TRANSACTIONS_TABLE,
    )

    input_dividends["Item"] = input_dividends["Description"].apply(
        lambda x: re.match(IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, x.strip()).group(
            1
        )
    )
    input_dividends["PPU"] = input_dividends["Description"].apply(
        lambda x: float(
            re.match(IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, x.strip()).group(2)
        )
    )
    # sqlite_conn.execute("DROP TABLE tmp_table_dividends")
    # sqlite_conn.execute("DROP TABLE tmp_table_div_taxes")
    input_dividends.to_sql(
        "tmp_table_dividends",
        sqlite_conn,
        index=False,
    )

    input_taxes["PPU"] = input_taxes["Description"].apply(
        lambda x: float(re.match(r".+?Cash Dividend .+ ([0-9.]+) per", x.strip()).group(1))
    )
    input_taxes["Item"] = input_taxes["Description"].apply(
        lambda x: re.match(r".+?\(", x.strip()).group()[:-1]
    )

    input_taxes.to_sql(
        "tmp_table_div_taxes",
        sqlite_conn,
        index=False,
    )

    print(input_dividends)
    print(input_taxes)
    # return
    sqlite_conn.execute(
        f"""
        WITH records_to_insert AS (
             SELECT DISTINCT
                    div.id,
                    div.Date,
                    'DIVIDENDS' AS Type,
                    div.Item,
                    ROUND(div.Amount / div.PPU, 2) AS Units,
                    div.Currency,
                    ROUND(div.PPU, 2) AS PPU,
                    0 AS Fees,
                    ROUND(COALESCE(tax.Amount, 0), 2) as Taxes,
                    1.0 AS StockSplitRatio,
                    '' AS Remarks

               FROM tmp_table_dividends AS div

          LEFT JOIN tmp_table_div_taxes AS tax
                 ON div.Currency = tax.Currency
                AND div.Date = tax.Date
                AND div.PPU = tax.PPU
                AND div.Item = tax.Item

           ORDER BY div.date
        )

        INSERT INTO {DBConstants.TRANSACTIONS_TABLE}

        SELECT * FROM records_to_insert WHERE true

            -- Overlapping statements or processing of the same input file
            -- twice is all allowed. But duplicates are not allowed.
            ON CONFLICT(id) DO NOTHING
        """
    )

    sqlite_conn.execute("DROP TABLE tmp_table_dividends")
    sqlite_conn.execute("DROP TABLE tmp_table_div_taxes")

    sqlite_conn.commit()


def enrich_data(input_data: T_SEMI_PROCESSED_DATA) -> T_PROCESSED_DATA:
    """This function reshapes and enhances the data from the IBKR reports based on
    particular business logic we need.

    Args:
        input_data: see the output of the function :func:`put_data_into_df`.

    Returns:
        The output type is the same as the input parameter, but there were some row
        modifications of the data (column additoins & removals, changing data types,
        groupping, filtering out irrelevant records, and more).

    Raises:
        Exception: if there are unexpected duplicates in the dataset or situations
            which we haven't anticipated, such as different kinds of fees.
    """
    logger.info("Going to enrich data from all the sections of all the reports.")

    all_dfs = {}

    for section_name, curr_df in input_data.items():

        if section_name == "Withholding Tax":
            curr_df["Amount"] = pd.to_numeric(curr_df["Amount"])
            curr_df["PPU"] = curr_df["Description"].apply(
                lambda x: float(re.match(r".+?Cash Dividend .+ ([0-9.]+) per", x.strip()).group(1))
            )
            curr_df["Item"] = curr_df["Description"].apply(
                lambda x: re.match(r".+?\(", x.strip()).group()[:-1]
            )
            curr_df.drop(columns=["Description"], inplace=True)

            # Sometimes there can be a false positives
            # (i.e. tax -$10, then +$10 and then -$5, so the final would be $5).
            columns_to_group_by = list(curr_df.columns)
            columns_to_group_by.remove("Amount")

            aggregated_df = curr_df.groupby(by=list(columns_to_group_by)).sum().reset_index()
            records_to_remove = aggregated_df.query(
                "Date == '2021-06-08' and Item == 'CDR' and PPU > 5"
            ).index

            aggregated_df = (
                aggregated_df.drop(records_to_remove).reset_index().drop(columns=["index"])
            )

            multiple_dividend_records_validation = aggregated_df.groupby(
                by=["Date", "Item"]
            ).filter(lambda x: len(x) > 1)
            if not multiple_dividend_records_validation.empty:
                raise Exception(
                    f"There are multiple dividend records for the same ticker and "
                    f"the same day, this shouldn't happen. "
                    f"Please investigate:\n{multiple_dividend_records_validation}"
                )

            aggregated_df["Type"] = "DIVIDENDS"
            aggregated_df["Fees"] = float(0)
            curr_df["Date"] = pd.to_datetime(curr_df["Date"]).dt.date

            aggregated_df.rename(columns={"Amount": "Taxes"}, inplace=True)

            all_dfs[section_name] = aggregated_df

        elif section_name == "Dividends":
            curr_df["Date"] = pd.to_datetime(curr_df["Date"]).dt.date

            curr_df["Item"] = curr_df["Description"].apply(
                lambda x: re.match(
                    IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, x.strip()
                ).group(1)
            )
            curr_df["PPU"] = curr_df["Description"].apply(
                lambda x: float(
                    re.match(
                        IBKRReportsProcessingConst.REGEX_PARSE_DIVIDEND_DESC, x.strip()
                    ).group(2)
                )
            )

            curr_df["Quantity"] = pd.to_numeric(curr_df["Amount"])
            curr_df["Units"] = curr_df.Quantity.div(curr_df.PPU.values)
            curr_df.drop(columns=["Description", "Amount"], inplace=True)

            # Sometimes there can be a false positives
            # (i.e. dividends -$10, then +$10 and then -$5, so the final would be $5).
            columns_to_group_by = list(curr_df.columns)
            columns_to_group_by.remove("Quantity")
            curr_df = curr_df.groupby(by=list(columns_to_group_by)).sum().reset_index()

            curr_df["Type"] = "DIVIDENDS"
            curr_df["Fees"] = float(0)
            all_dfs[section_name] = curr_df

    return all_dfs


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
            load_deposits_and_withdrawals_to_db(
                connection, semi_processed_data["Deposits & Withdrawals"]
            )
            load_forex_transactions_to_db(connection, semi_processed_data["Trades"])
            load_special_fees_to_db(connection, semi_processed_data["Fees"])
        except Exception as err:
            connection.execute("DROP TABLE tmp_table")
            raise Exception(
                f"The processing of {__name__} wasn't successful. Further details:\n{err}",
            ) from err

        load_stock_transactions_to_db(
            connection, semi_processed_data["Trades"], semi_processed_data["Transaction Fees"]
        )
        load_dividends_to_db(
            connection, semi_processed_data["Dividends"], semi_processed_data["Withholding Tax"]
        )

    logger.info("The processing of %s just finished.", __name__)
