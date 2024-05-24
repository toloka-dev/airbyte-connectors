# ------- Color print -------
R=$(shell tput setaf 1)
G=$(shell tput setaf 2)
Y=$(shell tput setaf 3)
B=$(shell tput setaf 4)
M=$(shell tput setaf 5)
C=$(shell tput setaf 6)
W=$(shell tput sgr0)
# ---------------------------


setup-dev:
	@echo "${M}Setting up dev environment${W}"
	poetry install --no-root --with=dev
	pre-commit install


ipython:
	poetry run ipython


update-deps:
	@echo "${M}Updating poetry.lock${W}"
	poetry lock


test:
	@echo "${M}Testing with pytest${W}"
	poetry run pytest -v tests
	@echo "${M}Testing style${W}"
	poetry run ruff check .
	poetry run mypy destination_databricks_py

format:
	@echo "${M}Formatting code${W}"
	poetry run ruff check --fix .
	poetry run ruff format .


build:
	@echo "${M}Building connector${W}"
	airbyte-ci --disable-update-check connectors --name=destination-databricks-py build

clean:
	@echo "${M}Cleaning...${W}"
	rm -rf build dist *.egg-info junit .pytest_cache .coverage coverage.xml