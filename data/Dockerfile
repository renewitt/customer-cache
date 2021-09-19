FROM python:3
WORKDIR /usr/src/data
COPY . .
RUN pip install -r requirements.txt
CMD ["generate_data.py"]
ENTRYPOINT ["python3"]