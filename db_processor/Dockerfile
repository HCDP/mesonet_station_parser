
FROM python:3.12.6

RUN mkdir -p /app
ADD ./* /app

RUN pip3 install -r /app/requirements.txt

WORKDIR /app

RUN mkdir /app/logs

CMD [ "python3", "-u", "/app/processor.py", "-v" ]