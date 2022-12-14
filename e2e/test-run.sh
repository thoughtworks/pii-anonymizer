#!/usr/bin/env bash

poetry run python -m pii_anonymizer.standalone --config e2e/standalone.json
poetry run python -m pii_anonymizer.spark --config e2e/spark.json

poetry run python e2e/assert.py
