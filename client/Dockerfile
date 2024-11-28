FROM python:3.9.7-slim
RUN python3 -m pip install pika && python3 -m pip install pyzmq
COPY client /
COPY common common
COPY /utils /utils
CMD [ "python3", "main.py" ]