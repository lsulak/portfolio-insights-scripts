# -*- coding: utf-8 -*-
"""This file contains all Python constants used in this project."""
import os
import re

_PROJ_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
DATA_DIR = os.path.join(_PROJ_DIR, "data")


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
            "filename": os.path.join(_PROJ_DIR, "logs", "financial_statement_processor.log"),
            "class": "logging.FileHandler",
            "formatter": "simple",
        },
    },
    "loggers": {
        "": {"handlers": ["default", "file_handler"], "level": "INFO", "propagate": True},
    },
}


DEFAULT_COINBASE_PORTFOLIO_CURRENCY = "GBP"


class DBConstants:  # pylint: disable=too-few-public-methods
    """This class holds all constants related to the final database (tables, db location, etc)
    that this project builds.
    """

    STATEMENTS_DB = os.path.join(DATA_DIR, "statements_sqlite.sql")

    TRANSACTIONS_TABLE = "transactions"
    DEP_AND_WITHDRAWALS_TABLE = "deposits_and_withdrawals"


class IBKRReportsProcessingConst:  # pylint: disable=too-few-public-methods
    """This class holds all constants needed for IBKR statement processing."""

    FOREX_CSV = os.path.join(DATA_DIR, "forex.csv")
    DEP_AND_WITHDRAWALS_CSV = os.path.join(DATA_DIR, "deposits_and_withdrawals.csv")
    SECURITIES_TRANSACTIONS_CSV = os.path.join(DATA_DIR, "securities_transactions.csv")

    IN_STAT_FILE_ENCODING = "utf-8-sig"

    REGEX_PARSE_DIVIDEND_DESC = re.compile(
        r"[a-zA-Z ]*?([A-Z0-9]+)\(.+?\) Cash Dividend .+? ([0-9.]+?) per"
    )

    COLUMNS_TO_GET = {
        "Trades": (
            "Asset Category",
            "Currency",
            "Symbol",
            "Date/Time",
            "Quantity",
            "T. Price",
            "Comm/Fee",
        ),
        "Transaction Fees": (
            "Asset Category",
            "Currency",
            "Date/Time",
            "Symbol",
            "Description",
            "Quantity",
            "Trade Price",
            "Amount",
        ),
        "Deposits & Withdrawals": ("Currency", "Settle Date", "Amount"),
        "Fees": ("Subtitle", "Currency", "Date", "Description", "Amount"),
        "Dividends": ("Currency", "Date", "Description", "Amount"),
        "Withholding Tax": ("Currency", "Date", "Description", "Amount"),
    }
