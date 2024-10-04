
FROM python:3.10

RUN pip3 install requirements.txt

RUN mkdir -p /app
ADD processor.py /app

WORKDIR /app

RUN mkdir /logs

CMD [ "python3", "-u", "/app/processor.py", "-v" ]