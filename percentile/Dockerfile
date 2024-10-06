FROM python:3.9.7-slim
RUN python3 -m pip install pika
COPY percentile /
COPY common common
CMD [ "python3", "main.py" ]