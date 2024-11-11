FROM python:3.9.7-alpine
RUN apk update
RUN apk add docker
COPY common common
COPY watchdog /
CMD [ "python3", "main.py" ]
