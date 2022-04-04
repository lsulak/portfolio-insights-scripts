# -*- coding: utf-8 -*-
"""This package contains modules responsible for processing, re-shaping, and
loading financial statement data (which are usually activity statements, containing
for example buy/sell transactions, dividends, and so on) from various brokerage
platforms into a SQLite DB.
"""

# List of modules, i.e. platforms which activity statement processing are supported.
__all__ = ["coinbase_pro", "coinbase"]
