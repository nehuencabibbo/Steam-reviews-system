FROM python:3.9.7-slim
RUN python3 -m pip install pika
COPY counter_by_platform /
COPY common common
CMD [ "python3", "main.py" ]