# -*- coding: utf-8 -*-
"""This file contains all Python constants used in this project."""
import os

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
