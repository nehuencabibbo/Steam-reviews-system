FROM python:3.9.7-slim

RUN pip install pika

COPY /top_k /top_k
COPY /common /common
COPY /utils /utils

CMD ["python3", "-m", "top_k.main"]