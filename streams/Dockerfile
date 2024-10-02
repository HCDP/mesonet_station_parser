
FROM python:3.10

RUN pip install --no-cache-dir pandas tapipy

ADD processor.py /home/tapis/streams_processor.py

WORKDIR /home/tapis

RUN mkdir /home/tapis/logs
