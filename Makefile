MYPY_OPTIONS = --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs

.PHONY: install
install:
	pip install pre-commit
	pre-commit install
	poetry install

.PHONY: data-gen
data-gen:
	poetry run python test_data/test_data_gen.py

.PHONY: run
run:
	poetry run python -m pii_anonymizer.standalone

.PHONY: spark-run
spark-run:
	poetry run python -m pii_anonymizer.spark

.PHONY: build
build:
	poetry build

.PHONY: test
test:
	poetry run sh bin/run_tests.sh

.PHONY: requirements
requirements:
	poetry export -f requirements.txt --output requirements.txt --dev
