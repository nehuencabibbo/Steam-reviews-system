FROM python:3.9.7-slim
RUN python3 -m pip install pika
COPY counter_by_app_id /
COPY common common
COPY utils utils
CMD [ "python3", "main.py" ]