FROM python:3.9.7-slim

RUN pip install pika

COPY /drop_nulls /drop_nulls
COPY /utils /utils
COPY /common /common

WORKDIR /drop_nulls

CMD ["python3", "-u", "main.py"]