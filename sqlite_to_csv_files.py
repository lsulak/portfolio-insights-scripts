#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""This is a helper script that just exports all tables in the input SQLite DB into a list of CSV
files for easier end-user consumption.
"""
import argparse
import logging.config
import os
from glob import glob

from statement_processing.constants import LOGGING_CONF
from statement_processing.database_utils import sqlite_tables_to_csv_files


def get_args():
    """CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="This is a helper script that just exports all tables in the input SQLite "
        "DB into a list of CSV files for easier end-user consumption."
    )
    parser.add_argument(
        "-i",
        "--input_database",
        required=True,
        help="Input database that will be exported into the CSV files.",
    )
    parser.add_argument(
        "--overwrite",
        default=False,
        action="store_true",
        help="Whether or not to overwrite all existing CSV output data by removing all the output files.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = get_args()

    logging.config.dictConfig(LOGGING_CONF)

    output_dir = os.path.dirname(cli_args.input_database)
    for file_to_remove in glob(f"{output_dir}/*.csv"):
        print(file_to_remove)

    sqlite_tables_to_csv_files(cli_args.input_database, output_dir)
