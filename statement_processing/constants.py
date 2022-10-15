# -*- coding: utf-8 -*-
"""This file contains all Python constants used in this project."""
import os
import re

import aiosql

_CURR_DIR = os.path.abspath(os.path.dirname(__file__))

_QUERIES_DIR = os.path.join(_CURR_DIR, "queries")
DB_QUERIES = aiosql.from_path(_QUERIES_DIR, "sqlite3")

URL_NASDAQ_TICKETS = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt"
URL_NYSE_TICKETS = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_tickers.txt"

RENAMED_STOCKS = {
    "FB": "META",
    "MYL": "VTRS",
}

MAP_STOCK_TO_EXCHANGE = {
    "WORK": "NYSE",
}

LOGGING_CONF = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {"format": "%(asctime)s %(levelname)s [%(processName)s|%(name)s] - %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "simple",
            "class": "logging.StreamHandler",
        },
        "file_handler": {
            "level": "INFO",
            "filename": os.path.join(_CURR_DIR, os.path.pardir, "logs", "financial_statement_processor.log"),
            "class": "logging.FileHandler",
            "formatter": "simple",
        },
    },
    "loggers": {
        "": {"handlers": ["default", "file_handler"], "level": "INFO", "propagate": True},
    },
}


class IBKRReportsProcessingConst:  # pylint: disable=too-few-public-methods
    """This class holds all constants needed for IBKR statement processing."""

    IN_STAT_FILE_ENCODING = "utf-8-sig"
    IN_STAT_FILE_DELIMITER = ","

    REGEX_PARSE_DIVIDEND_DESC = re.compile(r"[a-zA-Z ]*?([A-Z0-9]+)\(.+?\) Cash Dividend .+? ([0-9.]+?) per")

    # This is only for the first pre-processing and pre-filtering data, not for outputting.
    MAP_SECTION_TO_DESIRED_COLUMNS = {
        "Trades": {
            "Asset Category": str,
            "Currency": str,
            "Symbol": str,
            "Date/Time": str,
            "Quantity": float,
            "T. Price": float,
            "Comm/Fee": float,
            "Comm in USD": float,
        },
        "Transaction Fees": {
            "Asset Category": str,
            "Currency": str,
            "Date/Time": str,
            "Symbol": str,
            "Description": str,
            "Quantity": float,
            "Trade Price": float,
            "Amount": float,
        },
        "Deposits & Withdrawals": {
            "Currency": str,
            "Settle Date": str,
            "Description": str,
            "Amount": float,
        },
        "Fees": {
            "Subtitle": str,
            "Currency": str,
            "Date": str,
            "Description": str,
            "Amount": float,
        },
        "Dividends": {
            "Currency": str,
            "Date": str,
            "Description": str,
            "Amount": float,
        },
        "Withholding Tax": {
            "Currency": str,
            "Date": str,
            "Description": str,
            "Amount": float,
        },
    }
