# -*- coding: utf-8 -*-
"""This module implements some additional utilities related to stocks - such as, adding a missing exchange symbols, or
capturing ticker symbol rename operation and so on.
"""
import logging
from functools import cache

import pandas as pd
import requests

from .constants import (
    MAP_STOCK_TO_EXCHANGE,
    RENAMED_STOCKS,
    URL_NASDAQ_TICKETS,
    URL_NYSE_TICKETS,
)

logger = logging.getLogger(__name__)


@cache
def get_exchanges():
    """This function downloads, caches, and returns the ticker symbols for a few exchanges.

    This is important because Revolut statements only contain ticker symbols
    without respective exchange symbols.

    Returns:
        A dictionary in format: {<exchange>: <list_of_tickers>}
    """
    logger.info("Going to download list of all tickets from a few exchanges.")
    exchanges = {}
    with requests.get(URL_NASDAQ_TICKETS, stream=True) as r:
        r.raise_for_status()
        exchanges["NASDAQ"] = r.text.split("\n")

    with requests.get(URL_NYSE_TICKETS, stream=True) as r:
        r.raise_for_status()
        exchanges["NYSE"] = r.text.split("\n")

    return exchanges


def produce_missing_exchange_prefixes_for_tickers(
    pandas_row: pd.core.series.Series, ticker_column: str = "Item"
) -> pd.core.series.Series:
    """This function adds missing information about exchange symbols into a pandas row.

    Namely, dividend per share and number of currently held units are added - those two details
    are important, but not present on Revolut statements.

    Args:
        pandas_row: A single row of a Pandas DataFrame.
        ticker_column: A Pandas DataFrame column where the ticker symbol is stored.

    Returns:
        Processed row enriched with the desired information.
    """
    final_exchange = ""
    exchanges = get_exchanges()

    if pandas_row[ticker_column] in MAP_STOCK_TO_EXCHANGE.keys():
        pandas_row[ticker_column] = f"{MAP_STOCK_TO_EXCHANGE[pandas_row[ticker_column]]}:{pandas_row[ticker_column]}"
        return pandas_row

    for exchange_name, ticker_list in exchanges.items():
        # BRK.B is represented as BRK for example, in the exchange resources that will be checked.
        # So we are only interested in whatever is before the dot.
        curr_ticker = pandas_row[ticker_column].split(".")[0]  # note: this will work even if there is no dot

        if curr_ticker in ticker_list:
            final_exchange = f"{exchange_name}:"

    pandas_row[ticker_column] = f"{final_exchange}{pandas_row[ticker_column]}"
    return pandas_row


def replace_renamed_ticker_symbols(
    pandas_row: pd.core.series.Series, ticker_column: str = "Item"
) -> pd.core.series.Series:
    """This function replaces ticker symbols that were renamed on an exchange in the past. Unfortunately this
    event is not present in Revolut statement, but it's important to have the most up to date ticker symbol for
    each companies.

    Args:
        pandas_row: A single row of a Pandas DataFrame.
        ticker_column: A Pandas DataFrame column where the ticker symbol is stored.

    Returns:
        Processed row enriched with the desired information.
    """
    ticker = pandas_row[ticker_column]
    if ticker in RENAMED_STOCKS:
        pandas_row[ticker_column] = RENAMED_STOCKS[ticker]
    return pandas_row
