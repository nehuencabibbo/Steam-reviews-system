FROM python:3.9.7-slim

RUN pip install pika

COPY /drop_nulls /drop_nulls
COPY /middleware /middleware
COPY /utils /utils

WORKDIR /drop_nulls

CMD ["python3", "-u", "main.py"]