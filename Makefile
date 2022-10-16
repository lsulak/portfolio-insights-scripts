# Makefile for Python development & CI
# You need: black, flake8, flake8-isort, pylint, mdl (installable via gem).

BASEDIR = $(shell pwd)

test: lint unit-tests

venv-create:
	[ -d $(BASEDIR)/.venv ] || python3 -m venv $(BASEDIR)/.venv

lint: black-ci flake8 pylint-shorter readme-lint

install:
	@echo 'Going to install Python requirements'
	pip install --upgrade pip
	pip install -r requirements.txt

black:
	black --line-length 99 --exclude="pelican-plugins|themes|.venv" .

black-ci:
	echo -e "\n# Diff for each file:"; \
	black --line-length 99 --exclude=".venv" --diff .; \
	echo -e "\n# Status:"; \
	black --line-length 99 --exclude=".venv" --check .

flake8:
	flake8 --extend-exclude .venv,build

PYLINT_FILES = `find . \
		-path './docs' -prune -o \
		-path './.venv' -prune -o \
		-path './build' -prune -o \
		-name '*.py' -print`;

pylint:
	python3 -m pylint $(PYLINT_FILES)

pylint-shorter:
	python3 -m pylint --disable=bad-continuation --enable=useless-suppression $(PYLINT_FILES)

readme-lint:
	mdl README.md

unit-tests:
	echo "Unit tests are not implemented for this project yet"
	# python3 -m pytest -rxXs --cov

clean-all: clean
	find . -name '*.pyc' -delete
	find . -name '*.pyo' -delete
	find . -name '.pytest_cache' -type d | xargs rm -rf
	find . -name '__pycache__' -type d | xargs rm -rf
	find . -name '.coverage' -delete
