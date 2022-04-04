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
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .constants import DATA_DIR, IBKRReportsProcessingConst

T_AGGREGATED_RAW_DATA = Dict[str, List[str]]
T_SEMI_PROCESSED_DATA = Dict[str, pd.DataFrame]
T_PROCESSED_DATA = Dict[str, pd.DataFrame]

logger = logging.getLogger(__name__)


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
    skipped_sections = set()

    for input_file in glob.glob(f"{input_directory}/*.csv"):
        logger.info("Going to load the following file: %s", input_file)

        with open(
            input_file, "r", encoding=IBKRReportsProcessingConst.IN_STAT_FILE_ENCODING
        ) as input_filehandler:

            for line in input_filehandler:
                line_items = line.split(",")
                section_name = line_items[0].strip()

                if section_name not in IBKRReportsProcessingConst.COLUMNS_TO_GET.keys():
                    skipped_sections.add(section_name)
                    continue

                split_files[section_name].append(line)

    if skipped_sections:
        logger.info(
            "The following sections were skipped from all the CSV reports: %s", skipped_sections
        )

    return split_files


def prefilter_data(
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
        input_data: see the return value of :func:`aggregate_input_files`.
        remove_totals: the input IBKR report actually contains more than just
            individual data. It contains various 'totals' records, e.g. summaries
            of Buys for particular stock. These are not needed, but we can
            optionally keep them for debugging purposes.

    Returns:
        This is very similar to :func:`aggregate_input_files` but it outputs
        pre-filtered and de-duplicated data as a dictionary that holds Pandas
        Dataframes.
    """
    all_dfs = {}

    for section_name, section_content in input_data.items():
        logger.info("Going to prefilter data from the section: %s", section_name)

        section_as_df = pd.read_csv(
            io.StringIO("".join(section_content)), on_bad_lines="skip"
        )  # TODO there are bad lines?
        prefiltered_data = section_as_df.query('Header == "Data"').filter(
            items=IBKRReportsProcessingConst.COLUMNS_TO_GET[section_name]
        )

        if remove_totals:
            # Concatenate the row so that it's just a single string and search
            # for particular regex pattern; filter out the row on match.
            idx_of_totals = prefiltered_data.iloc[:, 0].str.contains("^Total.*$", na=False)
            prefiltered_data = prefiltered_data[~idx_of_totals]

        all_dfs[section_name] = prefiltered_data.drop_duplicates()

    return all_dfs


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
        curr_df.columns = curr_df.columns.str.replace(" ", "")

        if section_name == "Deposits & Withdrawals":
            curr_df.rename(columns={"SettleDate": "Date"}, inplace=True)

            all_dfs[section_name] = curr_df
            continue

        curr_df["StockSplitRatio"] = float(1)  # This is a default value.

        if section_name == "Trades":
            curr_df.rename(columns={"Date/Time": "Date"}, inplace=True)
            curr_df["Date"] = pd.to_datetime(curr_df["Date"]).dt.date

            curr_df["Type"] = (
                curr_df["Quantity"]
                .str.replace(",", "")
                .astype(float)
                .apply(lambda x: "BUY" if x > 0 else "SELL")
            )

            curr_df.rename(
                columns={
                    "T.Price": "PPU",
                    "Comm/Fee": "Fees",
                    "Quantity": "Units",
                    "Symbol": "Item",
                },
                inplace=True,
            )
            curr_df["PPU"] = curr_df["PPU"].astype(float)

            forex_transactions = curr_df.query("AssetCategory == 'Forex'")
            stock_transactions = curr_df.query("AssetCategory == 'Stocks'")

            all_dfs["Forex"] = forex_transactions.drop(columns=["AssetCategory"])
            all_dfs["Stocks"] = stock_transactions.drop(columns=["AssetCategory"])

        elif section_name == "Transaction Fees":
            curr_df.rename(columns={"Date/Time": "Date"}, inplace=True)
            curr_df["Date"] = pd.to_datetime(curr_df["Date"]).dt.date

            curr_df.rename(
                columns={"Quantity": "Units", "TradePrice": "PPU", "Description": "Remarks"},
                inplace=True,
            )
            curr_df["PPU"] = curr_df["PPU"].astype(float)

            fees_as_tax_validation = curr_df.query("Remarks != 'UK Stamp Tax'")
            if not fees_as_tax_validation.empty:
                raise Exception(
                    f"The {section_name} records contain fees that are not related to "
                    f"the UK Stamp Tax. It's possible that the script needs to be extended. "
                    f"Please investigate:\n{fees_as_tax_validation}"
                )

            curr_df.rename(columns={"Amount": "Taxes", "Symbol": "Item"}, inplace=True)

            all_dfs[section_name] = curr_df.drop(columns=["AssetCategory"])

        elif section_name == "Withholding Tax":
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

        elif section_name == "Fees":
            curr_df["Date"] = pd.to_datetime(curr_df["Date"]).dt.date

            curr_df["Fees"] = curr_df["Amount"].astype(float)
            curr_df.rename(columns={"Description": "Remarks"}, inplace=True)

            curr_df.drop(columns=["Subtitle", "Amount"], inplace=True)

            curr_df["Item"] = str("IBKR Fee")
            curr_df["Type"] = str("FEES")
            curr_df["Units"] = float(0)
            curr_df["PPU"] = float(0)

            # Sometimes there are 'false positives' - we received a fee payment and
            # then it was reverted. So we only care about the real, materialized, fees.
            columns_to_group_by = list(curr_df.columns)
            columns_to_group_by.remove("Fees")
            aggregated_df = curr_df.groupby(by=list(columns_to_group_by)).sum()
            aggregated_df = aggregated_df.query("Fees != 0").reset_index()

            all_dfs[section_name] = aggregated_df

    return all_dfs


def construct_securities_transactions(securities_transactions: T_PROCESSED_DATA) -> pd.DataFrame:
    """This function joins/merges the following individual reports into
    financial transactions related to securities (bonds, stocks, etc):
    * Dividends
    * (optional) Withholding Tax: based on our observation,
        these records are related to dividends
    * Stocks
    * (optional) Transaction Fees: based on our observation,
        these records are related to stocks

    Args:
        securities_transactions: see the output of function :func:`customize_data`

    Returns:
        A single dataframe that contains all transactions. It is reshaped so that
        a user can see a nice, consolidated view, ordered by the date of transaction.

    Raises:
        Exception: if there are duplicates in the final dataset.
    """
    logger.info("Going to construct the securities transactions.")

    if "Withholding Tax" not in securities_transactions:
        final_dividends_df = securities_transactions["Dividends"]
    else:
        final_dividends_df = securities_transactions["Dividends"].merge(
            securities_transactions["Withholding Tax"],
            how="left",
            on=["Currency", "Date", "StockSplitRatio", "PPU", "Item", "Type", "Fees"],
        )
    final_dividends_df["Remarks"] = str()

    if "Transaction Fees" not in securities_transactions:
        final_transactions_df = securities_transactions["Stocks"]
    else:
        final_transactions_df = securities_transactions["Stocks"].merge(
            securities_transactions["Transaction Fees"],
            how="left",
            on=["Currency", "Date", "Item", "Units", "StockSplitRatio", "PPU"],
        )

    final_dataset = (
        final_dividends_df.append(final_transactions_df)
        .append(securities_transactions["Fees"])
        .sort_values(by="Date")
    )

    duplicated_records = final_dataset.duplicated(
        ["Item", "Date", "Type", "Units"],
        keep=False,
    )
    duplicates_validation = final_dataset[duplicated_records]
    if not duplicates_validation.empty:
        raise Exception(
            f"There are duplicates in the final dataset with transactions. "
            f"Please investigate:\n{duplicates_validation}"
        )

    return final_dataset


def process(input_directory: str) -> None:
    """This is the main function for the whole IBKR statement processing.

    Args:
        input_directory: See func:`aggregate_input_files`.
    """
    logger.info("The processing of %s just started.", __name__)

    input_data_by_section = aggregate_input_files(input_directory)
    semi_processed_data = prefilter_data(input_data_by_section)
    processed_data = enrich_data(semi_processed_data)

    forex_to_exp = processed_data.pop("Forex")
    dep_and_withdrawals_to_exp = processed_data.pop("Deposits & Withdrawals")
    securities_to_exp = construct_securities_transactions(processed_data)

    Path(DATA_DIR).mkdir(exist_ok=True)

    for df_to_export, filename in (
        (forex_to_exp, IBKRReportsProcessingConst.FOREX_CSV),
        (dep_and_withdrawals_to_exp, IBKRReportsProcessingConst.DEP_AND_WITHDRAWALS_CSV),
        (securities_to_exp, IBKRReportsProcessingConst.SECURITIES_TRANSACTIONS_CSV),
    ):
        df_to_export.to_csv(filename, index=False)

    logger.info("The processing of %s just finished.", __name__)
