.PHONY: help clean install dev test lint format build publish

help:
	@echo ""
	@echo "Available targets:"
	@echo "  make install   Clean and install package"
	@echo "  make dev       Clean install with dev dependencies"
	@echo "  make test      Run test suite"
	@echo "  make lint      Run ruff linter"
	@echo "  make format    Format code with ruff"
	@echo "  make build     Build distribution artifacts"
	@echo "  make publish   Build and upload to PyPI via twine"
	@echo "  make clean     Remove build artifacts and caches"
	@echo ""

clean:
	rm -rf build dist src/*.egg-info
	find . -name "__pycache__" -type d -exec rm -rf {} +

# Clean + editable install
install: clean
	pip install -e .

# Clean + editable install with dev dependencies
dev: clean
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .

format:
	ruff format .

build: clean
	python -m build

publish: build
	twine upload dist/*