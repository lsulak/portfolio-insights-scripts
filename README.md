# Portfolio Insights Scripts

This repository stores scripts that I use for my personal finance needs - they are
part of a bigger project that I built on Google Sheets in 2021 (see below).

## Run Locally

If you want to run the script locally, including installation
of all dependencies, please follow these steps:

1. Create and activate virtual env and install dependencies: `./setup.sh`
2. Run `python3 statement_processing_runner.py -d <directory_with_input_statements> -r <report_type>`

## Personal Finance Google Sheet

The motivation here is to have a unified view on an individual's personal finances using
a tool that is fully open-sourced, free, and doesn't establish a direct connection to
your brokerage systems.
This functionality is especially useful if you use multiple brokers/platforms and your
portfolio is diversified across many asset categories such as stocks, crypto, gold, cash reserves,
etc. Furthermore, it supports many practical features, but more on that later.

### How It Started?

In 2020, I come across a book titled *The Richest Man in Babylon (by George S. Clason)* and it blew my mind.
Later that year, I told myself that I have to start investing and take personal finances more seriously.
Next year I bought a lots of books and started educating myself during evenings and weekends. You can
find some of my notes from this journey [here](https://lsulak-deployed-notes.s3-eu-west-1.amazonaws.com/investing.pdf).

Anyway, as I actually started with investing on the capital markets and my portfolio was more and more
diversified across many asset classes, brokerages and platforms, I wanted to have a centralized way
for viewing the overall portfolio. I was looking for something open-sourced, free & community driven,
with certain features I wanted. I couldn't find a product that would fully satisfy me so I created a
solution myself, tailored to my own needs.

It's just a **Google Sheets document** (for now, maybe I'll rewrite it to something more robust), publicly available
[here](https://docs.google.com/spreadsheets/d/1j-MRajHPs00mB3snLUEiYbVaY4v9_jb3mU_A1mn83sM).
Feel free to use it.

It's primarily driven by transactions (i.e. Buy, Sell, Dividends, Fees, Stock Split). I was manually entering
all individual transactions, in the past, but I wanted to automate this - ideally, without a direct API access
to the brokerage system. I wanted this to be completely isolated from my accounts.

### How It Works?

The tool has a couple of sheets. Some of them are presented below in the screens.
Half of the sheets represent user inputs, and the other half contain individual calculations
and pivot tables that are updated automatically based on the input data or various portals / finance APIs.

A user provides a set of inputs about his age, net worth, expected cash flow
from the monthly salary, expected and desired expenses, expected inflation
rate, and a few others, and the tool will project his future net worth and year when
he will be financially independent.

Other features include portfolio tracking through a lot of graphs and tables, portfolio
comparison against benchmarks (e.g. ETFs representing an index, such as S&P500 or QQQ),
dividend tracker, and much more. All world currencies are supported.

See a few screenshots of the whole project below. If you found this interesting,
don't hesitate to reach out here: <u><a href="mailto:laco.sulak@gmail.com">laco.sulak@gmail.com</a></u>.

![An example of net worth assumptions, tracking, and future projection.](screens/net_worth_future_projection.png)

![An example of securities & crypto tracking.](screens/securities_and_crypto.png)

![Everything is driven by transaction log.](screens/transactions.png)
