
FROM python:3.10

RUN pip install --no-cache-dir pandas tapipy

ADD streams_processor.py /home/tapis/

WORKDIR /home/tapis

RUN mkdir /home/tapis/logs
