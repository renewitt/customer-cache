install:
	python3 -m venv venv
	. venv/bin/activate
	python3 -m pip install -e .

build:
	python setup.py sdist

data:
	docker build -t generate-pi-data data/
