#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""This is the main script, i.e. the main entry point that runs individual
statement processing. It consumes activity statements exported prior to
its run from particular brokerage account and loads the well-shaped data
into a SQLite DB.

Note: Please see more detailed info about the usage supplied by the `--help` CLI parameter.
"""
import argparse
import importlib
import logging.config
import os
from glob import glob

import statement_processing
from statement_processing.constants import LOGGING_CONF
from statement_processing.db_definitions import create_tables


def get_args():
    """CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="This script loads and processes financial statements from various "
        "brokers or platforms, and stores the data into SQLite database into an enriched, "
        "well-shaped form, ready for further use. The input statements are required to be "
        "a collection of CSV files placed into a directory."
    )
    parser.add_argument(
        "-d",
        "--directory",
        required=True,
        help="Directory where the input reports are stored, "
        "as a collection of 1 or more CSV files.",
    )
    parser.add_argument(
        "-r",
        "--report_type",
        required=True,
        help="Report type to parse. Please put reports into the "
        "directory you specified in the parameter -d",
        choices=statement_processing.__dict__["__all__"],
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = get_args()

    logging.config.dictConfig(LOGGING_CONF)

    dir_not_ready = (
        not os.path.exists(cli_args.directory)
        or not os.path.isdir(cli_args.directory)  # noqa: W503
        or not glob(f"{cli_args.directory}/*.csv")  # noqa: W503
    )
    if dir_not_ready:
        raise Exception(
            f"Directory {cli_args.directory} either doesn't exist or it "
            f"doesn't contain any CSV files with desired reports."
        )

    imported_processing_module = importlib.import_module(
        f"{statement_processing.__name__}.{cli_args.report_type}"
    )

    create_tables()
    processing_function = getattr(imported_processing_module, "process")
    processing_function(cli_args.directory)
