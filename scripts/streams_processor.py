from datetime import date, timedelta, datetime
import argparse
from tapipy.tapis import Tapis, TapisResult
from tapipy.errors import BadRequestError
import logging
from logging import FileHandler
from os.path import isfile, join, exists
from os import listdir
import time
import concurrent.futures
import threading
import json
from urllib.request import urlopen
import csv
import typing
from typing import TypeAlias, Any
import traceback
import requests
from io import StringIO


def dispatch_req(req_funct, retries, *args, pass_exceptions = (), **kwargs):
    res = None
    try:
        res = req_funct(*args, **kwargs)
    except pass_exceptions:
        raise
    except Exception:
        if retries > 0:
            res = dispatch_req(req_funct, retries - 1, *args, **kwargs)
        else:
            raise
    return res
    

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

def create_project(owner: str, pi: str) -> None:
    try:
        dispatch_req(tapis_client.streams.get_project, retries, pass_exceptions = (BadRequestError), project_id = project_id)
    #throws BadRequestError if project does not exist
    except BadRequestError:
        dispatch_req(tapis_client.streams.create_project, retries, project_name = project_id, owner = owner, pi = pi)

def cache_existing_vars(station_id: str) -> None:
    existing_vars = set()
    list_res = []
    try:
        list_res = dispatch_req(tapis_client.streams.list_variables, retries, pass_exceptions = (BadRequestError), project_id = project_id, site_id = station_id, inst_id = f"{station_id}{inst_ext}")
    #list methods return an error if there are no items...
    except BadRequestError:
        pass
    for var in list_res:
        existing_vars.add(var.var_id)
    existing_var_map[station_id] = existing_vars


def create_sites_and_instruments() -> None:
    try:
        #copy the metadata map so don't affect original
        unknown_metadata_map = metadata_map.copy()
        list_res = []
        try:
            list_res = dispatch_req(tapis_client.streams.list_sites, retries, pass_exceptions = (BadRequestError), project_id = project_id)
        #list methods return an error if there are no items...
        except BadRequestError:
            pass
        for site in list_res:
            #site for the station already exists, delete from metadata map
            if site.site_id in unknown_metadata_map:
                del unknown_metadata_map[site.site_id]
                #cache existing vars once
                cache_existing_vars(site.site_id)

        site_request_body = []
        for station_id in unknown_metadata_map:
            station_metadata = unknown_metadata_map[station_id]
            station_name = station_metadata["name"]
            site_request_body.append({
                "site_name": station_name,
                "site_id": station_id,
                "latitude": float(station_metadata["lat"]), 
                "longitude": float(station_metadata["lng"]),
                "elevation": float(station_metadata["elevation"]),
                "description": f"Station {station_id}, {station_name}"
            })
            #no existing vars since this is new
            existing_var_map[station_id] = set()
        info_logger.info(f"Creating {len(site_request_body)} sites and instruments.")
        if len(site_request_body) > 0:
            dispatch_req(tapis_client.streams.create_site, retries, project_id = project_id, request_body = site_request_body)
            #instruments are redundant, just create one per site
            #add an extension to the id to make the name globally unique...
            for site_data in site_request_body:
                station_id = site_data["site_id"]
                station_name = site_data["site_name"]
                instrument_request = {
                    "inst_name": station_name,
                    "inst_id": f"{station_id}{inst_ext}",
                    "inst_description": f"Station {station_id}, {station_name}"
                }
                dispatch_req(tapis_client.streams.create_instrument, retries, project_id = project_id, site_id = station_id, request_body = [instrument_request])
        info_logger.info(f"Completed creating sites and instruments")
    except Exception as e:
        handle_error(e)

def create_variables(station_id: str, variables: list[str], display_names: list[str], units: list[str]) -> None:
    #get the set of vars that already exist in the API
    existing_vars = existing_var_map[station_id]
    request_body = []
    for i in range(len(variables)):
        var_id = variables[i]
        #don't recreate vars that already exist
        if var_id not in existing_vars:
            request_body.append({
                "var_id": var_id,
                "var_name": display_names[i],
                "unit": units[i]
            })
            #add to set of existing vars
            existing_vars.add(var_id)
    info_logger.info(f"Creating {len(request_body)} variables for station {station_id}.")
    if len(request_body) > 0:
        dispatch_req(tapis_client.streams.create_variable, retries, project_id = project_id, site_id = station_id, inst_id = f"{station_id}{inst_ext}", request_body = request_body)


def create_measurements(station_id: str, measurements: list[typing.Dict[str, str]]) -> None:
    info_logger.info(f"Creating {len(measurements)} blocks of measurements for station {station_id}.")
    dispatch_req(tapis_client.streams.create_measurement, retries, inst_id = f"{station_id}{inst_ext}", vars = measurements)


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
    return dt


def get_station_files_from_api(station_id: str, date: str):
    files = []
    url = f" https://api.hcdp.ikewai.org/raw/list?date={date}&station_id={station_id}&location={location}"
    headers = {
        "Authorization": f"Bearer {hcdp_token}"
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


def get_station_files(station_id: str, start_date: datetime, end_date: datetime):
    files = []
    for date in daterange(start_date, end_date):
        dstr = date.strftime("%Y-%m-%d")
        files += get_station_files_from_api(station_id, dstr)
    return files


def get_start_time(station_id: str):
    last_report = None
    if last_record_timestamps is None or last_record_timestamps.get(station_id) is None:
        last_report = datetime.combine(datetime.now(), datetime.min.time())
    else:
        last_report = datetime.fromisoformat(last_record_timestamps[station_id]) + timedelta(seconds = 1)
    return last_report


def get_data_from_file(station_id, file, start_date, end_date):
    data = None
    with urlopen(file) as f:
        decoded = f.read().decode()
        text = StringIO(decoded)
        reader = csv.reader(text)
        #get the version the file uses
        version = next(reader)[5]
        #get the alias map for the file based on the station id and file type
        file_alias_map = alias_map.get(version)
        if file_alias_map is None:
            info_logger.info(f"Warning: No alias map found for logger version {version} found in file {file}. Variable names will be used directly.")
            file_alias_map = {}
        #second line has variable names
        #strip out timestamp and id columns
        variables = next(reader)[2:]
        #translate to standard names
        variables = [file_alias_map.get(var) or var for var in variables]
        #translate variable names to display names
        display_names = list(map(lambda variable: display_map.get(variable) or variable, variables))
        #third line has units
        units = next(reader)[2:]
        #move past last header line
        next(reader)
        measurements = []
        
        dt = None
        #get measurements
        for row in reader:
            dt_measurements = {}
            timestamp = parse_timestamp(row[0])
            dt = parse_timestamp(row[0])
            if dt >= start_date and dt <= end_date:
                timestamp = f"{dt.isoformat()}-10:00"
                dt_measurements["datetime"] = timestamp
                row = row[2:]
                for i in range(len(row)):
                    dt_measurements[variables[i]] = row[i]
                measurements.append(dt_measurements)
        data = {
            "var_ids": variables,
            "display_names": display_names,
            "units": units,
            "measurements": measurements,
            "last_timestamp": dt
        }
    return data


#note start and end date need to be passed as datetime objects
def handle_station(station_id: str, site_and_instrument_handler, start_date = None, end_date = None):
    global file_count
    global success
    if start_date is None:
        start_date = get_start_time(station_id)
    if end_date is None:
        end_date = datetime.now()
    station_files = []
    try:
        station_files = get_station_files(station_id, start_date, end_date)
    except Exception as e:
        handle_error(e, f"Unable to retreive files for station {station_id}", None)
    file_count += len(station_files)
    for file in station_files:
        try:
            data = get_data_from_file(station_id, file, start_date, end_date)
            var_ids = data["var_ids"]
            units = data["units"]
            display_names = data["display_names"]
            measurements = data["measurements"]
            last_timestamp = data["last_timestamp"]
            #wait for sites and instruments to be created
            site_and_instrument_handler.result()
            create_variables(station_id, var_ids, display_names, units)
            create_measurements(station_id, measurements)
            #record last timestamp
            if last_record_timestamps is not None:
                last_record_timestamps[station_id] = last_timestamp.isoformat()
            success += 1
            info_logger.info(f"Completed processing file {file}")
        except Exception as e:
            handle_error(e, f"An error occurred while processing file {file} for station {station_id}", None)
    info_logger.info(f"Completed station {station_id}")



if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(prog = "streams_processor.py", description = "Ingest mesonet flat files into the Tapis Streams API")

    parser.add_argument("-v", "--verbose", action="store_true", help="turn on verbose mode")
    parser.add_argument("-pid","--project_id", help="provide the Tapis Streams Project ID to parse the data into")
    parser.add_argument("-t","--tenant", help="Tapis tenant to use like dev")
    parser.add_argument("-tu","--tapis_url", help="Tapis base URL to use like https://dev.develop.tapis.io")
    parser.add_argument("-th","--threads", type=int, help="Number of threads to use to process the mesonet files in parallel")
    parser.add_argument("-u","--username", help="Tapis username that was read/write access to the project and data")
    parser.add_argument("-p","--password", help="The Tapis password for the username provided")
    parser.add_argument("-sd","--start_date", help="Optional. An ISO 8601 timestamp indicating the starting time of measurements to ingest. Defaults to the last recorded time for each station.")
    parser.add_argument("-ed","--end_date", help="Optional. An ISO 8601 timestamp indicating the end time of measurements to ingest. Defaults to the last recorded time for each station.")
    parser.add_argument("-ht","--hcdp_token", help="Token for accessing the HCDP API.")
    parser.add_argument("-ie","--inst_ext", help="Extension for making instrument IDs globally unique.")
    parser.add_argument("-r","--retries", type=int, help="Number of times to retry failed requests")
    parser.add_argument("-lrf","--last_record_file", help="File with last recorded times for stations")
    parser.add_argument("-l","--location", default="hawaii", help="Mesonet location")

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Set Tapis Tenant and Base URL
    tenant = args.tenant
    base_url = args.tapis_url
    inst_ext = args.inst_ext
    username = args.username
    password = args.password
    hcdp_token = args.hcdp_token
    retries = args.retries
    project_id = args.project_id
    num_workers = args.threads
    location = args.location
    start_date = args.start_date
    if start_date is not None:
        start_date = datetime.fromisoformat(start_date)
    end_date = args.end_date
    if end_date is not None:
        end_date = datetime.fromisoformat(end_date)
    last_record_file = args.last_record_file
    
    last_record_timestamps = None
    if last_record_file is not None:
        try:
            with open(last_record_file) as f:
                last_record_timestamps = json.load(f)
        except Exception as e:
            handle_error(e, prepend_msg = "Error reading last record file:")

    file_count = 0
    success = 0
    tapis_client = None
    existing_var_map = {}   

    start_time = time.time()
    
    try:
        # #Create python Tapis client for user
        tapis_client = Tapis(base_url = base_url, username = username, password = password, account_type = "user", tenant_id = tenant)
        # Generate an Access Token that will be used for all API calls
        tapis_client.get_tokens()

    except Exception as e:
        handle_error(e, prepend_msg = "Error: Tapis client could not be created -")


    create_project(username, password)

    ###############################
    # get json files
    ###############################
    alias_map = None
    metadata_map = None
    display_map = None
    alias_doc = "https://raw.githubusercontent.com/HCDP/loggernet_station_data/main/json_data/versions.json"
    metadata_doc = "https://raw.githubusercontent.com/HCDP/loggernet_station_data/main/json_data/metadata.json"
    display_doc = "https://raw.githubusercontent.com/HCDP/loggernet_station_data/main/json_data/display.json"
    try:
        with urlopen(alias_doc) as f:
            alias_map = json.load(f)
    except Exception as e:
        handle_error(e, prepend_msg = "Error retrieving alias json doc:")
    try:
        with urlopen(metadata_doc) as f:
            metadata_map = json.load(f)
    except Exception as e:
        handle_error(e, prepend_msg = "Error retrieving metadata json doc:")
    try:
        with urlopen(display_doc) as f:
            display_map = json.load(f)
    except Exception as e:
        handle_error(e, prepend_msg = "Error retrieving display json doc:")
    

    with concurrent.futures.ThreadPoolExecutor(max_workers = num_workers) as executor:
        try:
            station_handlers = []
            site_and_instrument_handler = executor.submit(create_sites_and_instruments)

            for station_id in metadata_map:
                station_handler = executor.submit(handle_station, station_id, site_and_instrument_handler, start_date, end_date)
                station_handlers.append(station_handler)
            concurrent.futures.wait(station_handlers)
        except Exception as e:
            err_logger.error(traceback.format_exc())

    if last_record_file is not None:
        with open(last_record_file, "w") as f:
            last_record_timestamps = json.dump(last_record_timestamps, f)

    end_time = time.time()
    exec_time = end_time - start_time
    #cut to two decimal places
    exec_time = round(exec_time, 2)

    info_logger.info(f"Files parsing complete: success: {success}, failed: {file_count - success}, time: {exec_time} seconds")
