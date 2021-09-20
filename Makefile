VENV = venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip

# changes to setup.py will result in a rebuild
$(VENV)/bin/activate: setup.py
	python3 -m venv $(VENV)
	$(PIP) install -e .
	$(PIP) install --upgrade pip

clean:
	rm -rf __pycache__
	rm -rf $(VENV)

install: $(VENV)/bin/activate

build:
	docker-compose build

test: $(VENV)/bin/activate
	$(PIP) install tox
	$(PYTHON) -m tox

dist:
	python setup.py sdist

run:
	docker-compose up

stop:
	docker-compose down
