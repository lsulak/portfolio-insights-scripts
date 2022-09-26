# -*- coding: utf-8 -*-
"""This file contains all Python constants used in this project."""

from hashlib import md5


def create_id_for_each_row(input_df, id_column_name="id", columns_to_ignore=None):
    """This function adds a new column into the input dataframe. This column
    is meant to represent the unique identification of data in each row.
    """
    columns_to_ignore = columns_to_ignore if columns_to_ignore else []

    # pylint: disable=bad-builtin
    input_df[id_column_name] = input_df.apply(
        lambda x: md5("".join(map(str, x if x not in columns_to_ignore else "")).encode("utf-8")).hexdigest(),
        axis=1,
    )
    # pylint: enable=bad-builtin
    return input_df
