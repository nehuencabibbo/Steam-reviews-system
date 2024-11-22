FROM python:3.9.7-slim

RUN pip install pika
RUN pip install langid

COPY /filter_by_language /filter_by_language
COPY /utils /utils
COPY /common /common

WORKDIR /filter_by_language

CMD ["python3", "-u", "main.py"]