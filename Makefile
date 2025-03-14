PYTHON := python
PIP := pip
PACKAGES := midb

.PHONY: all clean install install-e test build release
.DEFAULT_GOAL := help

help:
	@echo "Welcome to the MIDB Makefile"
	@echo "Available commands:"
	@echo "  help       - Show this help message"
	@echo "  all        - Clean, build, and install the package"
	@echo "  install    - Install the package"
	@echo "  install-e  - Install the package in development mode"
	@echo "  test       - Run the tests"
	@echo "  clean      - Clean the build and dist directories"
	@echo "  build      - Build the Cython extensions"
	@echo "  release    - Run on release"

all: clean build install

install:
	$(PIP) install .

install-e:
	$(PIP) install -e .

test:
	$(PYTHON) -m unittest discover -s tests

clean:
	@rm -rf build/
	@rm -rf src/
	@rm -rf dist/
	@rm -rf *.egg-info/
	@find . -type f -name "*.so" -delete
	@find . -type f -name "*.c" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} +

build:
	$(PYTHON) setup.py build_ext --inplace

release:
	make build install test 