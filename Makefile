install:
	python3 -m venv venv
	. venv/bin/activate
	python3 -m pip install -e .

build:
	docker-compose build

dist:
	python setup.py sdist

run:
	docker-compose up

stop:
	docker-compose down
