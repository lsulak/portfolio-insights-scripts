#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""This is the main script, i.e. the main entry point that runs individual
statement processing. It consumes activity statements exported prior to
its run from particular brokerage account and loads the well-shaped data
into a SQLite DB.

The goal of this script is to have a convenient way of loading all transactions into
my personal finance sheet on Google Docs.
I needed to have a consolidated view on the overall investment portfolio that goes
beyond a single broker by loading and reshaping the statement(s) into a time-series data
where each line represents a single buy/sell/dividends/fees transaction.

Note: Please see more detailed info about the usage supplied by the `--help` CLI parameter.
"""
import argparse
import importlib
import logging.config
import os
from glob import glob
from pathlib import Path

import statement_processing
from statement_processing.constants import LOGGING_CONF
from statement_processing.database_utils import create_tables


def get_args():
    """CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="This script loads and processes financial statements from various "
        "brokers or platforms, and stores the data into SQLite database into an enriched, "
        "well-shaped form, ready for further use. The input statements are required to be "
        "a collection of CSV files placed into a directory."
    )
    parser.add_argument(
        "-i",
        "--input_directory",
        required=True,
        help="Directory where the input reports are stored, as a collection of 1 or more CSV files.",
    )
    parser.add_argument(
        "-o",
        "--output_database",
        required=True,
        help="Output database where the final data will be stored.",
    )
    parser.add_argument(
        "-r",
        "--report_type",
        required=True,
        help="Report type to parse. Please put reports into the directory you specified in the parameter -i",
        choices=statement_processing.__dict__["__all__"],
    )
    parser.add_argument(
        "--overwrite",
        default=False,
        action="store_true",
        help="Whether or not to overwrite the output database if it exists.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = get_args()

    logging.config.dictConfig(LOGGING_CONF)

    dir_not_ready = (
        not os.path.exists(cli_args.input_directory)
        or not os.path.isdir(cli_args.input_directory)  # noqa: W503
        or not glob(f"{cli_args.input_directory}/*.csv")  # noqa: W503
    )
    if dir_not_ready:
        raise Exception(
            f"Directory {cli_args.input_directory} either doesn't exist or it "
            f"doesn't contain any CSV files with desired reports."
        )

    imported_processing_module = importlib.import_module(f"{statement_processing.__name__}.{cli_args.report_type}")

    if cli_args.overwrite and os.path.exists(cli_args.output_database):
        os.remove(cli_args.output_database)

    Path(os.path.dirname(cli_args.output_database)).mkdir(exist_ok=True)

    create_tables(cli_args.output_database)

    processing_function = getattr(imported_processing_module, "process")
    processing_function(cli_args.input_directory, cli_args.output_database)
