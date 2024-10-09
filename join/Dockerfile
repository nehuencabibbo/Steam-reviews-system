FROM python:3.9.7-slim

RUN pip install pika

COPY /join /join
COPY /middleware /middleware
COPY /common /common
COPY /utils /utils

CMD ["python3", "-m", "join.main"]