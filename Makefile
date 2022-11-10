MYPY_OPTIONS = --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs

.PHONY: install
install:
	pip install pre-commit
	pre-commit install
	poetry install

.PHONY: run
run:
	poetry run python src/pii_anonymizer/standalone/dpf_main.py --config config.json

.PHONY: build
build:
	poetry build

.PHONY: test
test:
	poetry run sh bin/run_tests.sh

.PHONY: requirements
requirements:
	poetry export -f requirements.txt --output requirements.txt --dev
