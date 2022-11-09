MYPY_OPTIONS = --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs

.PHONY: install
install:
	pip install -r requirements-dev.txt

.PHONY: build
build:
	python setup.py bdist_wheel
	
.PHONY: test
test:
	sh bin/run_tests.sh
