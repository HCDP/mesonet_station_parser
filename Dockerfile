# image: mesonet_streams

from python:3.10

RUN pip install --no-cache-dir pandas tapipy

RUN useradd tapis

ADD /scripts /home/tapis/

RUN chown -R tapis:tapis /home/tapis

USER tapis

WORKDIR /home/tapis

RUN mkdir /home/tapis/data
RUN mkdir /home/tapis/logs
