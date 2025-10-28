SHELL := /bin/bash
PY := python

.PHONY: setup ingest_dc transform_dc forecast_dc

setup:
	@mkdir -p data

ingest_dc: setup
	$(PY) -c "print('Ingest placeholder ran')"

transform_dc:
	$(PY) - << 'PY'
print('Transform placeholder ran')
PY

forecast_dc:
	$(PY) - << 'PY'
print('Forecast placeholder ran')
PY
