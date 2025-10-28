SHELL := /bin/bash
PY := python

.PHONY: setup ingest_dc transform_dc forecast_dc

setup:
	@mkdir -p data

ingest_dc: setup
	@echo "Ingest placeholder ran"

transform_dc:
	@echo "Transform placeholder ran"

forecast_dc:
	@echo "Forecast placeholder ran"
