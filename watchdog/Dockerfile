FROM python:3.9.7
RUN apt-get update && apt-get install -y docker.io
COPY common common
COPY watchdog /
CMD [ "python3", "main.py" ]
