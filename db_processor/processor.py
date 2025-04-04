from datetime import date, timedelta, datetime
import argparse
import logging
from logging import FileHandler
from os import environ, cpu_count
import time
import concurrent.futures
from urllib.request import urlopen
import csv
import traceback
import requests
from io import StringIO
from pytz import timezone, utc

token = environ["HCDP_TOKEN"]
hcdp_api = "https://api.hcdp.ikewai.org"
headers = {
    "Authorization": f"Bearer {token}"
}
locations = ["hawaii", "american_samoa"]

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


def get_station_timezone(station_id):
    ep = f"{hcdp_api}/mesonet/db/stations?station_ids={station_id}"
    res = requests.get(ep, headers = headers)
    res.raise_for_status()
    tz = res.json()[0]["timezone"]
    station_timezone = timezone(tz)
    return station_timezone


def get_measurements_from_file(station_id, file, start_date, end_date):
    timestamps = set()
    measurements = []
    with urlopen(file, timeout = 5) as f:
        decoded = f.read().decode()
        text = StringIO(decoded)
        reader = csv.reader(text)
        first_header = next(reader)
        station_id = first_header[1].split("_")[0]
        version = first_header[5]
        station_timezone = get_station_timezone(station_id)
        #second line has variable names
        #strip out timestamp and id columns
        variables = next(reader)[2:]
        #third line has units, should be handled by variable metadata
        next(reader)
        #move past last header line
        next(reader)
        #get measurements
        for row in reader:
            dt = parse_timestamp(row[0], station_timezone)
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


    

def insert_rows(rows, location):
    ep = f"{hcdp_api}/mesonet/db/measurements/insert"
    body = {
        "overwrite": True,
        "location": location,
        "data": rows
    }
    
    res = requests.put(ep, json = body, headers = headers)
    res.raise_for_status()
    modified = res.json()["modified"]
    info_logger.info(f"Successfully wrote {modified} values.")


def handle_file(file: str, start_date: datetime, end_date: datetime):
    rows = handle_retry(get_measurements_from_file, (file, start_date, end_date))
    #skip if no measurements to add
    if len(rows) > 0:
        insert_rows(rows, location)
    info_logger.info(f"Completed processing file {file}")


#create generic retry for passed function
def handle_retry(f, args, failure_handler = None, failure_args = (), retry = 0):
    time.sleep(retry**3)
    try:
        return f(*args)
    except Exception as e:
        if retry < 3:
            err_logger.error(f"{f.__name__} attempt {retry} failed with error: {e}. Retrying...")
            if failure_handler is not None:
                failure_handler(*failure_args)
            return handle_retry(f, args, failure_handler, failure_args, retry + 1)
        else:
            raise e


def clean_file(file):
    ep = f"{hcdp_api}/mesonet/dirtyFiles/remove"
    requests.delete(ep, headers = headers)

def get_files_in_range(location: str, start_date: datetime, end_date: datetime):
    files = []
    url = f"https://api.hcdp.ikewai.org/raw/list?startDate={start_date.strftime('%Y-%m-%d')}&endDate={end_date.strftime('%Y-%m-%d')}&location={location}"
    res = requests.get(url, headers = headers, timeout = 5)
    res.raise_for_status()
    files = res.json()
    return files

def get_dirty_files():
    ep = f"{hcdp_api}/mesonet/dirtyFiles/list"
    res = requests.get(ep, headers = headers)
    res.raise_for_status()
    files = res.json()
    return files

def process_dirty():
    ep = f"{hcdp_api}/mesonet/dirtyFiles/process"
    res = requests.get(ep, headers = headers)
    res.raise_for_status()

if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(prog = "streams_processor.py", description = "Ingest mesonet flat files into the Mesonet database")

    parser.add_argument("-v", "--verbose", action = "store_true", help = "turn on verbose mode")
    parser.add_argument("-t","--threads", type = int, help = "Optional. Number of threads to use to process the mesonet files in parallel. Defaults to number of CPUs on the machine.")
    parser.add_argument("-sd","--start_date", help = "Optional. An ISO 8601 timestamp indicating the starting time of measurements to ingest. If no start date is provided, a list of data files will be pulled from previously unprocessed loggernet upload manifests.")
    parser.add_argument("-ed","--end_date", help = "Optional. An ISO 8601 timestamp indicating the end time of measurements to ingest. Defaults to the current time. This value will be ignored if no start date is provided.")
    parser.add_argument("-l","--location", help = "Optional. The mesonet location to work process.")

    args = parser.parse_args()

    setup_logging(args.verbose)
    
    num_workers = args.threads or cpu_count()
    location = args.location
    start_date = args.start_date
    end_date = args.end_date

    start_time = time.time()
    process_dirty()
    
    files = []
    if start_date is None:
        files = get_dirty_files()
    else:
        for location in locations:
            files += get_files_in_range(location, start_date, end_date)
          
    file_count = len(files)
    successes = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers = num_workers) as executor:
        try:
            file_handlers = {}
            for file in files:
                file_handler = executor.submit(handle_file, file, start_date, end_date)
                file_handlers[file] = file_handler
            concurrent.futures.wait(file_handlers.values(), 3600)
        except Exception as e:
            err_logger.error(traceback.format_exc())
    for file in file_handlers:
        future = file_handlers[file]
        try:
            future.result()
            successes += 1
        except Exception as e:
            err_logger.error(f"Processing {file} failed. Error: ")
            err_logger.error(traceback.format_exc())

    end_time = time.time()
    exec_time = end_time - start_time
    #cut to two decimal places
    exec_time = round(exec_time, 2)

    info_logger.info(f"Files parsing complete: successes: {successes}, failures: {file_count - successes}, time: {exec_time} seconds")
