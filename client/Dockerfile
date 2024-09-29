FROM python:3.9.7-slim
RUN python3 -m pip install pika
COPY client /
CMD [ "python3", "main.py" ]