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
from pytz import timezone, utc
import psycopg2


def get_last_timestamp(station_id, location, localtz, cur):
    query = f"""
        SELECT timestamp
        FROM {location}_measurements
        WHERE station_id = '{station_id}'
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    cur.execute(query)
    res = cur.fetchall()
    last_report = None
    if len(res) > 0:
        timestamp = res[0][0]
        timestamp = timestamp.replace(tzinfo=utc).astimezone(localtz)
        #db entries should be tz aware
        #add one second to last entry to move past entry
        last_report = timestamp + timedelta(seconds = 1)
    else:
        last_report = datetime.combine(datetime.now(localtz), datetime.min.time())
        last_report = localtz.localize(last_report)
    
    return last_report


def get_stations(location, cur):
    if location is not None:
        query = """
            SELECT station_metadata.station_id, station_metadata.location, timezone_map.timezone
            FROM station_metadata
            JOIN timezone_map ON station_metadata.location = timezone_map.location
            WHERE station_metadata.location = %s;
        """
        cur.execute(query, (location,))
    else:
        query = """
            SELECT station_metadata.station_id, station_metadata.location, timezone_map.timezone
            FROM station_metadata
            JOIN timezone_map ON station_metadata.location = timezone_map.location;
        """
        cur.execute(query)
    stations = cur.fetchall()
    return stations


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

def handle_error(error: Exception, prepend_msg: str = "error:", rethrow: bool = False) -> None:
    msg = traceback.format_exc()
    err_logger.error(f"{prepend_msg} {msg}")
    if rethrow:
        raise error


def parse_timestamp(timestamp: str, localtz) -> str:
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


def daterange(start_date: datetime, end_date: datetime):
    date = start_date.date()
    end_date = end_date.date()
    while date <= end_date:
        yield date
        date += timedelta(days = 1)


def get_station_files(station_id: str, location: str, start_date: datetime, end_date: datetime):
    files = []
    url = f"https://api.hcdp.ikewai.org/raw/list?startDate={start_date.strftime('%Y-%m-%d')}&endDate={end_date.strftime('%Y-%m-%d')}&station_id={station_id}&location={location}"
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


def get_measurements_from_file(station_id, file, start_date, end_date, localtz):
    timestamps = set()
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
            dt = parse_timestamp(row[0], localtz)
            if dt >= start_date and dt <= end_date:
                #convert timestamp back to utc for db storage
                timestamp = dt.astimezone(utc).isoformat()
                row = row[2:]
                #ensure not a duplicate timestamp, some files have dupes
                if timestamp not in timestamps:
                    timestamps.add(timestamp)
                    for i in range(len(row)):
                        variable = variables[i]
                        value = row[i]
                        #don't record missing values
                        if value != "NAN":
                        
                            # !! TEMP PASS TO FLAG LOGIC !!
                            flag = 0
                            
                            measurements.append([station_id, timestamp, variable, version, value, flag])
    return measurements


#note start and end date need to be passed as datetime objects
def handle_station(station_id: str, location: str, localtz: str, start_date = None, end_date = None):
    global file_count
    global success
    with psycopg2.connect(
        host = environ["DB_HOST"], 
        port = environ.get("DB_PORT") or "5432", 
        dbname = environ["DB_NAME"], 
        user = environ["DB_USERNAME"], 
        password = environ["DB_PASSWORD"]
    ) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            #localize timestamps to location
            if start_date is not None:
                start_date = datetime.fromisoformat(start_date)
                start_date = localtz.localize(start_date)
            else:
                start_date = get_last_timestamp(station_id, location, localtz, cur)
                
            if end_date is not None:
                end_date = datetime.fromisoformat(end_date)
                end_date = localtz.localize(end_date)
            else:
                end_date = datetime.now(localtz)
                
            station_files = []
    
            try:
                station_files = get_station_files(station_id, location, start_date, end_date)
            except Exception as e:
                handle_error(e, f"Unable to retreive files for station {station_id}")
            file_count += len(station_files)
            for file in station_files:
                try:
                    rows = get_measurements_from_file(station_id, file, start_date, end_date, localtz)
                    #skip if no measurements to add
                    if len(rows) > 0:
                        #sanitized (mogrified) row data for query
                        values = ",".join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", row).decode('utf-8') for row in rows)                            
                        cur.execute(f"""
                            INSERT INTO {location}_measurements
                            VALUES {values}
                            ON CONFLICT (station_id, timestamp, variable)
                            DO UPDATE SET
                                version = EXCLUDED.version,
                                value = EXCLUDED.value,
                                flag = EXCLUDED.flag;
                        """)

                    success += 1
                    info_logger.info(f"Completed processing file {file}")

                except Exception as e:
                    handle_error(e, f"An error occurred while processing file {file} for station {station_id}")
    info_logger.info(f"Completed station {station_id}")




if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(prog = "streams_processor.py", description = "Ingest mesonet flat files into the Mesonet database")

    parser.add_argument("-v", "--verbose", action=  "store_true", help = "turn on verbose mode")
    parser.add_argument("-t","--threads", type = int, help = "Number of threads to use to process the mesonet files in parallel")
    parser.add_argument("-sd","--start_date", help = "Optional. An ISO 8601 timestamp indicating the starting time of measurements to ingest. Defaults to the last recorded time for each station.")
    parser.add_argument("-ed","--end_date", help = "Optional. An ISO 8601 timestamp indicating the end time of measurements to ingest. Defaults to the current time.")
    parser.add_argument("-l","--location", help = "Optional. The mesonet location to work process.")

    args = parser.parse_args()

    setup_logging(args.verbose)
    
    num_workers = args.threads
    location = args.location
    start_date = args.start_date
    end_date = args.end_date

    file_count = 0
    success = 0

    start_time = time.time()

    stations = []
    with psycopg2.connect(
        host = environ["DB_HOST"], 
        port = environ.get("DB_PORT") or "5432", 
        dbname = environ["DB_NAME"], 
        user = environ["DB_USERNAME"], 
        password = environ["DB_PASSWORD"]
    ) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            stations = get_stations(location, cur)
    with concurrent.futures.ThreadPoolExecutor(max_workers = num_workers) as executor:
        try:
            station_handlers = []
            for station_id, location, tz_str in stations:
                station_handler = executor.submit(handle_station, station_id, location, timezone(tz_str), start_date, end_date)
                station_handlers.append(station_handler)
            concurrent.futures.wait(station_handlers, 3600)
        except Exception as e:
            err_logger.error(traceback.format_exc())

    end_time = time.time()
    exec_time = end_time - start_time
    #cut to two decimal places
    exec_time = round(exec_time, 2)

    info_logger.info(f"Files parsing complete: success: {success}, failed: {file_count - success}, time: {exec_time} seconds")
