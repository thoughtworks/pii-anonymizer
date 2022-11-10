#!/usr/bin/env bash
set -e
project_path=$(dirname $0)/..

export PYTHONPATH=$project_path

coverage run --source='./src/pii_anonymizer' --omit='*/tests/*' -m unittest discover ./src/pii_anonymizer
coverage report -m
