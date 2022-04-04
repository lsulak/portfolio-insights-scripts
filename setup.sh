#!/usr/bin/env bash
#
# This is a short script that automates the creation of python venv,
# and install all the dependencies you should need for the development
# (you can also install ruby and mdl, since it's part of the CI).
#

make venv-create

source .venv/bin/activate
make install
