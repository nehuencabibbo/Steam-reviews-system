FROM python:3.9.7-slim

RUN pip install pika

COPY /filter_columns /filter_columns
COPY /common /common
COPY /utils /utils

WORKDIR /filter_columns

CMD ["python3", "-u", "main.py"]