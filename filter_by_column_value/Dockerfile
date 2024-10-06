FROM python:3.9.7-slim

RUN pip install pika
RUN pip install langid 

COPY /filter_by_column_value /filter_by_column_value
COPY /middleware /middleware
COPY /utils /utils
COPY /common /common

WORKDIR /filter_by_column_value

CMD ["python3", "-u", "main.py"]