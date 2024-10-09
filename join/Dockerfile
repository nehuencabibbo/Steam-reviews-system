FROM python:3.9.7-slim

RUN pip install pika

COPY /join /join
COPY /common /common

CMD ["python3", "-m", "join.main"]