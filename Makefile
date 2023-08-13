.PHONY: lint
lint:
	flake8 ./learndb --ignore E501,E203,W503

.PHONY: tests
tests:
	python -m pytest tests/*.py

.PHONY: repl
repl:
	python run_learndb.py repl
