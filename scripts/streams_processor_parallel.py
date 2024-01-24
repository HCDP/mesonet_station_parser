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

VariableCache: TypeAlias = list[str]
InstrumentCache: TypeAlias = typing.Dict[str, VariableCache]
SiteCache: TypeAlias = typing.Dict[str, InstrumentCache]
ProjectCache: TypeAlias = typing.Dict[str, SiteCache]

SiteData: TypeAlias = typing.Dict[str, tuple[str, str]]

def get_msg(error: Exception) -> str:
    return traceback.format_exc()


def handle_error(error: Exception, prepend_msg: str = "error:", exit_code: int = -1) -> None:
    msg = get_msg(error)
    print(msg)
    logger.error(f"{prepend_msg} {msg}")
    if exit_code is not None:
        exit(exit_code)


def get_location_info(station_id: str) -> tuple[int, int, int]:
    station_metadata = metadata_map.get(station_id)

    location_info = (20, -158, 0) # somewhere left of Hawaii island in the ocean

    # Check if station exist in csv
    if station_metadata is not None:
        latitude = float(station_metadata["lat"])
        longitude = float(station_metadata["lng"])
        elevation = float(station_metadata["elevation"])
        location_info = (latitude, longitude, elevation)
    else:
        logger.info("Station's meta data for %s not found", station_id)
    return location_info


def check_create_project(project_id: str, owner: str, pi: str, cache) -> None:
    if not project_id in cache:
        create_project(project_id, owner, pi)
        cache[project_id] = {}


def create_project(project_id: str, owner: str, pi: str) -> None:
    try:
        permitted_client.streams.get_project(project_id = project_id)
    except BadRequestError:
        permitted_client.streams.create_project(project_name = project_id, owner = owner, pi = pi)


def check_create_sites(project_id: str, site_data: SiteData, cache: SiteCache) -> None:
    unknown_site_data = {}
    for site in site_data:
        if not site in cache:
            unknown_site_data[site] = site_data[site]
    if len(unknown_site_data) > 0:
        create_sites(project_id, unknown_site_data, cache)


def create_sites(project_id: str, unknown_site_data: SiteData, cache: typing.Dict[str, typing.Dict[str, str]]) -> None:
    result = []
    try:
        result = permitted_client.streams.list_sites(project_id = project_id)
    #list methods return an error if there are no items...
    except BadRequestError:
        pass
    for item in result:
        #site already exists, delete from unknown set and add to cache
        if item.site_id in unknown_site_data:
            del unknown_site_data[item.site_id]
            cache[item.site_id] = {}
    if len(unknown_site_data) > 0:
        request_body = []
        for site in unknown_site_data:
            (station_id, site_name) = unknown_site_data[site]
            (latitude, longitude, elevation) = get_location_info(station_id = station_id)
            request_body.append({
                "site_name": site,
                "site_id": site,
                "latitude": latitude, 
                "longitude": longitude,
                "elevation": elevation,
                "description": site_name
            })
        permitted_client.streams.create_site(project_id = project_id, request_body = request_body)
        #add newly created sites to cache
        for site in unknown_site_data:
            cache[site] = {}


def check_create_instruments(project_id: str, site_id: str, site_name: str, instruments: list[str], cache: InstrumentCache) -> None:
    unknown_instruments = set()
    for instrument in instruments:
        if not instrument in cache:
            unknown_instruments.add(instrument)
    if len(unknown_instruments) > 0:
        create_instruments(project_id, site_id, site_name, unknown_instruments, cache)


def create_instruments(project_id: str, site_id: str, site_name: str, unknown_instruments: typing.Set, cache: InstrumentCache) -> None:
    #list_instruments does not throw an error if empty, instead returns useless uniterable TapisResult object
    result = permitted_client.streams.list_instruments(project_id = project_id, site_id = site_id)
    #convert to empty array if returned TapisResult object
    if isinstance(result, TapisResult):
        result = []

    for item in result:
        if item.inst_id in unknown_instruments:
            unknown_instruments.remove(item.inst_id)
            cache[item.inst_id] = []
    if len(unknown_instruments) > 0:
        request_body = [{
            "inst_name": instrument,
            "inst_id": instrument,
            "inst_description": f"{instrument} for {site_id}, {site_name}"
        } for instrument in unknown_instruments]
        permitted_client.streams.create_instrument(project_id = project_id, site_id = site_id, request_body = request_body)
        for instrument in unknown_instruments:
            cache[instrument] = []


def check_create_variables(project_id: str, site_id: str, inst_id: str, variables: list[str], units: list[str], cache: VariableCache) -> None:
    #create unit map by zipping variables and units
    variable_unit_map = {variables[i]: units[i] for i in range(len(variables))}
    cache_set = set(cache)
    unknown_variable_unit_map = {}
    for variable in variable_unit_map:
        if not variable in cache_set:
            unknown_variable_unit_map[variable] = variable_unit_map[variable]
    if len(unknown_variable_unit_map) > 0:
        create_variables(project_id, site_id, inst_id, unknown_variable_unit_map, cache)


def create_variables(project_id: str, site_id: str, inst_id: str, unknown_variable_unit_map: typing.Dict[str, str], cache: VariableCache) -> None:
    result = []
    try:
        result = permitted_client.streams.list_variables(project_id = project_id, site_id = site_id, inst_id = inst_id)
    except BadRequestError:
        pass
    for item in result:
        if item.var_id in unknown_variable_unit_map:
            del unknown_variable_unit_map[item.var_id]
            cache.append(item.var_id)
    if len(unknown_variable_unit_map) > 0:
        request_body = [{
            "var_id": variable,
            "var_name": variable,
            "unit": unknown_variable_unit_map[variable]
        } for variable in unknown_variable_unit_map]
        permitted_client.streams.create_variable(project_id = project_id, site_id = site_id, inst_id = inst_id, request_body = request_body)
    for variable in unknown_variable_unit_map:
        cache.append(variable)


def create_measurements(inst_id: str, measurements: list[typing.Dict[str, str]]) -> None:
    permitted_client.streams.create_measurement(inst_id = inst_id, vars = measurements)


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

    iso_string = dt.isoformat()+"-10:00"
    return iso_string


def process_station_file(project_id: str, site_id: str, inst_id: str, fpath: str, alias_map: typing.Dict[str, str], cache: VariableCache) -> None:
    global count

    print("process file")

    measurements = []
    with open(fpath, encoding="utf8", errors="backslashreplace", newline='') as f:
        print("open")
        reader = csv.reader(f)
        #skip first line
        next(reader)
        #second line has variable names
        #strip out timestamp and id columns
        variables = next(reader)[2:]
        print(variables)
        #translate to standard names
        variables = [alias_map.get(var) or var for var in variables]
        #third line has units
        units = next(reader)[2:]
        #create variables
        check_create_variables(project_id, site_id, inst_id, variables, units, cache)
        #move past last header line
        next(reader)
        
        #get measurements
        for row in reader:
            dt_measurements = {}
            timestamp = parse_timestamp(row[0])

            dt_measurements["datetime"] = timestamp
            row = row[2:]
            for i in range(len(row)):
                print(i, len(row), len(variables))
                dt_measurements[variables[i]] = row[i]
            variables.append(dt_measurements)
    #create the measurements
    create_measurements(inst_id, measurements)

    print("Complete!")
    
    


def get_alias_map(file_type: str, station_id: str) -> typing.Dict[str, str]:
    #switch to simplified form when alias map stripped out (may have to process exceptions)
    ftype_alias_map = alias_map.get(file_type) or {}
    universal_alias_map = ftype_alias_map.get("universal") or {}
    id_alias_map = ftype_alias_map.get(station_id) or {}
    #for master variable list in file need to find variable names not in this map and use reflexive
    file_alias_map = {**universal_alias_map, **id_alias_map}
    return file_alias_map


# Function that will be executed by the threads
def process_station_files(station_file_group: typing.Dict[str, Any]) -> bool:
    print("process_station_files start")
    station_id = station_file_group["station_id"]
    station_name = station_file_group["station_name"]
    site_id = station_file_group["site_id"]
    station_file_data = station_file_group["files"]
    site_cache = project_cache[site_id]

    logger2.info(f"Processing files for station {station_id}, {station_name}")

    try:
        instruments = [ f"{site_id}_{file_data[0]}" for file_data in station_file_data ]
        #create instruments
        check_create_instruments(project_id, site_id, station_name, instruments, site_cache)
        for i in range(len(station_file_data)):
            (file_type, fname, fpath) = station_file_data[i]
            inst_id = instruments[i]
            alias_map = get_alias_map(file_type, station_id)
            instrument_cache = site_cache[inst_id]
            try:
                process_station_file(project_id, site_id, inst_id, fpath, alias_map, instrument_cache)
            except Exception as e:
                e_msg = get_msg(e)
                print(e_msg)
                logger.error(f"An error occurred while processing the file {fpath} for station {station_id}, {station_name}: {e_msg}")
                return False
    except Exception as e:
        e_msg = get_msg(e)
        print(e_msg)
        logger.error(f"An error occurred while processing the files for station {station_id}, {station_name}: {e_msg}")
        return False
    #replace total with global var
    # Update the count value in a thread-safe manner
    # with count_lock:
    #     count += 1
    #     logger2.info("Progress: %d/%d", count, len(listdir(data_dir)))
    return True
        

    
def process_file_name(file_name: str) -> tuple[str]:
    data = None
    split = file_name.split("_")
    if len(split) == 2:
        data = (split[0], None, split[1].split(".")[0])
    elif len(split) == 3:
        data = (split[0], split[1], split[2].split(".")[0])
    return data


# Define a function that handles the parallel processing of all files
def process_files_in_parallel(data_dir: str, dir_content: list[str], num_workers: int, iteration: str):
    file_groups = {}
    site_data = {}
    print("process files")
    for file in dir_content:
        path = join(data_dir, file)
        print(path)
        if isfile(path) and file.endswith(".dat"):
            fname_data = process_file_name(file)
            if fname_data is not None:
                (station_id, station_name, file_type) = fname_data
                site_id = f"{station_id}_{iteration}"
                group = file_groups.get(station_id)
                if group is None:
                    group = {
                        "station_id": station_id,
                        "station_name": station_name,
                        "site_id": site_id,
                        "files": []
                    }
                    file_groups[station_id] = group
                site_data[site_id] = (station_id, station_name)
                group["files"].append((file_type, file, path))
    try:
        print("before check_create")
        check_create_sites(project_id, site_data, project_cache)
    except Exception as e:
        print("????")
        print(e)
        handle_error(e, prepend_msg = "Could not create sites:")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers = num_workers) as executor:
        print("dispatch")
        try:
            # Submit the processing of each file to the ThreadPoolExecutor
            results = list(executor.map(process_station_files, list(file_groups.values())))
        except Exception as e:
            print(get_msg(e))

    return results

def setup_logging(verbose: bool) -> None:
    global logger
    global logger2
    # Logger for errors
    level = logging.ERROR
    file_handler = FileHandler('./logs/out.err')
    logger = logging.getLogger('Logger1')
    logger.setLevel(level)
    formatter = logging.Formatter('[%(asctime)s] %(message)s [%(pathname)s:%(lineno)d]')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Logger for execution info
    level = logging.INFO
    logger2 = logging.getLogger('Logger2')
    logger2.setLevel(level)
    file_handler2 = FileHandler('./logs/out.log')
    formatter = logging.Formatter('\r[%(asctime)s] %(message)s [%(pathname)s:%(lineno)d]')
    file_handler2.setFormatter(formatter)
    logger2.addHandler(file_handler2)

    # Print to stdout if -v (verbose) option is passed
    if (verbose):
        stdout_handler = logging.StreamHandler()
        logger.addHandler(stdout_handler)
        logger2.addHandler(stdout_handler)


if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(
        prog="streams_processor.py",
        description=""
    )

    # TODO: might remove for final prod
    parser.add_argument("-i", "--iteration", help="set the iteration number for project version")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="turn on verbose mode")
    parser.add_argument("-d","--data_dir", help="provide the path to the directory with the mesonet stations files")
    parser.add_argument("-pid","--project_id", help="provide the Tapis Streams Project ID to parse the data into")
    parser.add_argument("-t","--tenant", help="Tapis tenant to use like dev")
    parser.add_argument("-tu","--tapis_url", help="Tapis base URL to use like https://dev.develop.tapis.io")
    parser.add_argument("-th","--threads", type=int, help="Number of threads to use to process the mesonet files in parallel")
    parser.add_argument("-u","--username", help="Tapis username that was read/write access to the project and data")
    parser.add_argument("-p","--password", help="The Tapis password for the username provided")
    parser.add_argument("-c","--cache_file", help="The Tapis password for the username provided")

    args = parser.parse_args()

    # Set Tapis Tenant and Base URL
    tenant = args.tenant #"dev"
    base_url = args.tapis_url #'https://' + tenant + '.develop.tapis.io'

    setup_logging(args.verbose)

    permitted_username = args.username #"testuser2"
    permitted_user_password = args.password #"testuser2"

    # iteration of test
    # TODO: can be removed for final revision
    iteration = args.iteration

    start_time = time.time()

    try:
        # #Create python Tapis client for user
        permitted_client = Tapis(base_url = base_url,
                                 username = permitted_username,
                                 password = permitted_user_password,
                                 account_type = 'user',
                                 tenant_id = tenant
                                 )

        # Generate an Access Token that will be used for all API calls
        permitted_client.get_tokens()

    except Exception as e:
        handle_error(e, prepend_msg = "Error: Tapis client could not be created -")

    project_id = args.project_id + '_' + iteration

    cache = {}
    if args.cache_file is not None:
        if exists(args.cache_file):
            with open(args.cache_file) as f:
                cache = json.load(f)
        else:
            logger2.info("Could not find cache file. A new one will be created.")
    

    #allow multi url/tenant cache support
    url_cache = cache.get(args.tapis_url)
    if url_cache is None:
        url_cache = {}
        cache[args.tapis_url] = url_cache
    tenant_cache = url_cache.get(args.tenant)
    if tenant_cache is None:
        tenant_cache = {}
        url_cache[args.tenant] = tenant_cache
    project_cache = tenant_cache.get(project_id)
    if project_cache is None:
        project_cache = {}
        tenant_cache[project_id] = project_cache

    check_create_project(project_id, args.username, args.username, project_cache)

    # Count how many files were parsed into streams-api vs total num of files
    count = 0

    ###############################
    # get json files
    ###############################
    alias_map = None
    metadata_map = None
    alias_doc = "https://raw.githubusercontent.com/HCDP/loggernet_station_data/main/json_data/aliases.json"
    metadata_doc = "https://raw.githubusercontent.com/HCDP/loggernet_station_data/main/json_data/metadata.json"
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

    # Initialize the lock
    count_lock = threading.Lock()

    # Define the number of parallel workers
    num_workers = args.threads
    
    dir_content = listdir(args.data_dir)
    num_files = len(dir_content)

    logger2.info(f"Progress: 0/{len(dir_content)}")
    # Call the function to process files in parallel
    results = process_files_in_parallel(args.data_dir, dir_content, num_workers, iteration)

    #write out cache file if specified
    if args.cache_file is not None:
        with open(args.cache_file, "w") as f:
            json.dump(cache, f)

    end_time = time.time()
    exec_time = end_time - start_time

    logger2.info("Files parsing complete: success: %d, failed: %d, time: %.2f seconds", count, len(dir_content) - count, exec_time)
