FROM python:3.7-alpine
RUN apk update
RUN apk add docker
COPY /common /common
COPY watchdog /
CMD [ "python3", "-u", "main.py" ]