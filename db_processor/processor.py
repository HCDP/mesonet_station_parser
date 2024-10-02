from datetime import date, timedelta, datetime
import argparse
import logging
from logging import FileHandler
from os.path import isfile, join, exists
from os import listdir, environ
import time
import concurrent.futures
import json
from urllib.request import urlopen
import csv
import typing
from typing import TypeAlias, Any
import traceback
import requests
from io import StringIO
from pytz import timezone

tz_map = {
    "hawaii": "Pacific/Honolulu",
    "american_samoa": "Pacific/Pago_Pago"
}

def get_last_timestamp(station_id, cur):
    query = f"""
        SELECT timestamp
        FROM measurements
        WHERE station_id = {station_id}
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    cur.execute(query)
    res = cur.fetchall()
    last_report = None
    if len(res) > 0:
        timestamp = res[0][0]
        #db entries should be tz aware
        #add one second to last entry to move past entry
        last_report = datetime.fromisoformat(timestamp) + timedelta(seconds = 1)
    else:
        last_report = datetime.combine(datetime.now(localtz), datetime.min.time())
        last_report = localtz.localize(last_report)
    return last_report


def get_station_data(location, cur):
    if location is not None:
        query = """
            SELECT station_id, location
            FROM metadata
            WHERE location = %s
        """
        cur.execute(query, (location,))
    else:
        query = """
            SELECT station_id, location
            FROM metadata;
        """
        cur.execute(query)
    station_data = cur.fetchall()
    return station_data


def setup_logging(verbose: bool) -> None:
    global info_logger
    global err_logger
    # Logger for errors
    level = logging.ERROR
    file_handler = FileHandler('./logs/out.err')
    err_logger = logging.getLogger('Logger1')
    err_logger.setLevel(level)
    formatter = logging.Formatter('[%(asctime)s] %(message)s [%(pathname)s:%(lineno)d]')
    file_handler.setFormatter(formatter)
    err_logger.addHandler(file_handler)

    # Logger for execution info
    level = logging.INFO
    info_logger = logging.getLogger('info_logger')
    info_logger.setLevel(level)
    file_handler2 = FileHandler('./logs/out.log')
    formatter = logging.Formatter('[%(asctime)s] %(message)s [%(pathname)s:%(lineno)d]')
    file_handler2.setFormatter(formatter)
    info_logger.addHandler(file_handler2)

    # Print to stdout if -v (verbose) option is passed
    if verbose: 
        stdout_handler = logging.StreamHandler()
        err_logger.addHandler(stdout_handler)
        info_logger.addHandler(stdout_handler)

def handle_error(error: Exception, prepend_msg: str = "error:", exit_code: int = -1) -> None:
    msg = traceback.format_exc()
    err_logger.error(f"{prepend_msg} {msg}")
    if exit_code is not None:
        exit(exit_code)


def parse_timestamp(timestamp: str) -> str:
    measurement_time = timestamp.split(" ")
    dt = None
    #handle 24:00:00 formatting for midnight
    if int(measurement_time[1].split(":")[0]) > 23:
        converted_timestamp = measurement_time[0] + " 23:59:59"
        dt = datetime.strptime(converted_timestamp, '%Y-%m-%d %H:%M:%S')
        dt += timedelta(seconds = 1)
    else:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    dt = localtz.localize(dt)
    return dt   


def get_station_files_from_api(station_id: str, location: str, date: str):
    files = []
    url = f"https://api.hcdp.ikewai.org/raw/list?date={date}&station_id={station_id}&location={location}"
    token = environ["HCDP_TOKEN"]
    headers = {
        "Authorization": f"Bearer {token}"
    }
    res = requests.get(url, headers = headers)
    if(res.status_code == 200):
        files = res.json()
    else:
        err_logger.error(f"An error occurred while listing data files for station: {station_id}, date: {date}, code: {res.status_code}")
    return files


def daterange(start_date: datetime, end_date: datetime):
    date = start_date.date()
    end_date = end_date.date()
    while date <= end_date:
        yield date
        date += timedelta(days = 1)


def get_station_files(station_id: str, location: str, start_date: datetime, end_date: datetime):
    files = []
    for date in daterange(start_date, end_date):
        dstr = date.strftime("%Y-%m-%d")
        files += get_station_files_from_api(station_id, location, dstr)
    return files


def get_measurements_from_file(station_id, file, start_date, end_date, cur):
    measurements = []
    with urlopen(file) as f:
        decoded = f.read().decode()
        text = StringIO(decoded)
        reader = csv.reader(text)
        version = next(reader)[5]
        #second line has variable names
        #strip out timestamp and id columns
        variables = next(reader)[2:]
        #third line has units, should be handled by variable metadata
        next(reader)
        #move past last header line
        next(reader)
        #get measurements
        for row in reader:
            timestamp = parse_timestamp(row[0])
            dt = parse_timestamp(row[0])
            if dt >= start_date and dt <= end_date:
                timestamp = dt.isoformat()
                row = row[2:]
                for i in range(len(row)):
                    variable = variables[i]
                    value = row[i]
                    
                    # !! TEMP PASS TO FLAG LOGIC !!
                    flag = 0
                    
                    measurements.append([station_id, timestamp, variable, version, value, flag])
    return measurements


#note start and end date need to be passed as datetime objects
def handle_station(station_id: str, location: str, site_and_instrument_handler, start_date = None, end_date = None):
    global file_count
    global success
    
    #localize timestamps to location
    #works as location validation for query insertion since will throw an error if invalid
    localtz = timezone(tz_map[location])
    if start_date is not None:
        start_date = datetime.fromisoformat(start_date)
        start_date = localtz.localize(start_date)
    else:
        start_date = get_last_timestamp(station_id, cur)
        
    if end_date is not None:
        end_date = datetime.fromisoformat(end_date)
        end_date = localtz.localize(end_date)
    else:
        end_date = datetime.now(localtz)
        
    station_files = []
    
    with psycopg2.connect(
        host = environ.get["DB_HOST"], 
        port = environ.get("DB_PORT") or "5432", 
        dbname = environ["DB_NAME"], 
        user = environ["DB_USERNAME"], 
        password = environ["DB_PASSWORD"]
    ) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            try:
                station_files = get_station_files(station_id, location, start_date, end_date)
            except Exception as e:
                handle_error(e, f"Unable to retreive files for station {station_id}", None)
            file_count += len(station_files)
            for file in station_files:
                try:
                    rows = get_measurements_from_file(station_id, file, start_date, end_date, cur)
                    
                    #sanitized (mogrified) row data for query
                    values = ",".join(cur.mogrify("%s,%s,%s,%s,%s,%s", row).decode('utf-8') for row in data)
                    cur.execute(f"""
                        INSERT INTO {location}_measurements
                        VALUES {values}
                        ON CONFLICT (station_id, timestamp, variable)
                        DO UPDATE SET
                            version = EXCLUDED.version,
                            value = EXCLUDED.value,
                            flag = EXCLUDED.flag
                    """)

                    success += 1
                    info_logger.info(f"Completed processing file {file}")

                except Exception as e:
                    handle_error(e, f"An error occurred while processing file {file} for station {station_id}", None)
    info_logger.info(f"Completed station {station_id}")




if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(prog = "streams_processor.py", description = "Ingest mesonet flat files into the Mesonet database")

    parser.add_argument("-v", "--verbose", action="store_true", help="turn on verbose mode")
    parser.add_argument("-th","--threads", type=int, help="Number of threads to use to process the mesonet files in parallel")
    parser.add_argument("-sd","--start_date", help="Optional. An ISO 8601 timestamp indicating the starting time of measurements to ingest. Defaults to the last recorded time for each station.")
    parser.add_argument("-ed","--end_date", help="Optional. An ISO 8601 timestamp indicating the end time of measurements to ingest. Defaults to the last recorded time for each station.")
    parser.add_argument("-l","--location", default="hawaii", help="Mesonet location")

    args = parser.parse_args()

    setup_logging(args.verbose)
    
    num_workers = args.threads
    location = args.location
    start_date = args.start_date
    end_date = args.end_date

    
    if last_record_file is not None:
        try:
            with open(last_record_file) as f:
                last_record_timestamps = json.load(f)
        except Exception as e:
            handle_error(e, prepend_msg = "Error reading last record file:")

    file_count = 0
    success = 0

    start_time = time.time()

    station_data = []
    with psycopg2.connect(
        host = environ.get["DB_HOST"], 
        port = environ.get("DB_PORT") or "5432", 
        dbname = environ["DB_NAME"], 
        user = environ["DB_USERNAME"], 
        password = environ["DB_PASSWORD"]
    ) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            station_data = get_station_data(location, cur)

    with concurrent.futures.ThreadPoolExecutor(max_workers = num_workers) as executor:
        try:
            station_handlers = []

            for station_id, location in station_data:
                station_handler = executor.submit(handle_station, station_id, location, start_date, end_date)
                station_handlers.append(station_handler)
            concurrent.futures.wait(station_handlers)
        except Exception as e:
            err_logger.error(traceback.format_exc())

    end_time = time.time()
    exec_time = end_time - start_time
    #cut to two decimal places
    exec_time = round(exec_time, 2)

    info_logger.info(f"Files parsing complete: success: {success}, failed: {file_count - success}, time: {exec_time} seconds")
