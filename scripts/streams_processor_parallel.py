from datetime import date, timedelta, datetime
import argparse
from tapipy.tapis import Tapis
from tapipy.errors import InvalidInputError
import logging
from logging import FileHandler
from os.path import basename, isfile, join, exists
from os import listdir
import pandas as pd
import time
import concurrent.futures
import threading
import json
from urllib import urlopen

def get_msg(error: Exception):
    msg = e
    if hasattr(e, 'message'):
        msg = e.message
    return msg

def handle_error(error: Exception, prepend_msg: str = "error:", exit_code: int = -1):
    msg = get_msg(error)
    logger.error(f"{prepend_msg} {msg}")
    if exit_code is not None:
        exit(exit_code)

def get_location_info(station_id: str) -> tuple:
    station_metadata = metadata_doc.get(station_id)

    location_info = (20, -158, 0) # somewhere left of Hawaii island in the ocean

    # Check if station exist in csv
    if station_metadata is not None:
        logger.info("Station's metadata for %s found", station_id)
        latitude = float(station_metadata["lat"])
        longitude = float(station_metadata["lng"])
        elevation = float(station_metadata["elevation"])

        logger.info("%d %d %d", latitude, longitude, elevation)
        location_info = (latitude, longitude, elevation)
    else:
        logger.info("Station's meta data for %s not found", station_id)
    return location_info

def create_project(project_id: str, username: str, pi: str):
    exists = True
    try:
            permitted_client.streams.get_project(project_id = project_id)
            project_exist = True
        except Exception as e:
            pass

        if not project_exist:
            try:
                logger.error("trying to create PROJECT")
                permitted_client.streams.create_project(
                    project_name=project_id, owner=args.username, pi=args.username)
            except Exception as e:
                handle_error(e)

def create_instrument(project_id: str, site_id: str, site_name: str, inst_id: str):
    err_msg = None
    try:
        permitted_client.streams.get_instrument(project_id = project_id, side_id = site_id, inst_id = inst_id)
    except tapipy.errors.InvalidInputError:
        try:
            permitted_client.streams.create_instrument(project_id=project_id,
                                                            site_id=site_id,
                                                            request_body=[{
                                                                "inst_name": inst_id,
                                                                "inst_id": inst_id,
                                                                "inst_description": "MetData for " + site_id + "_" + site_name
                                                                }])
        except Exception as e:
            err_msg = get_msg(e)
    except Exception as e:
        err_msg = get_msg(e)
    return err_msg

def create_site(project_id: str, site_id: str, site_name: str, station_id: str) -> bool:
    err_msg = None
    try:
        permitted_client.streams.get_site(project_id = project_id, side_id = site_id)
    except tapipy.errors.InvalidInputError:
        (latitude, longitude, elevation) = get_location_info(station_id = station_id)
        try:
            permitted_client.streams.create_site(project_id=project_id,
                                                        request_body=[{
                                                            "site_name": site_id,
                                                            "site_id": site_id,
                                                            "latitude": latitude, 
                                                            "longitude": longitude,
                                                            "elevation": elevation,
                                                            "description": site_name}])
        except Exception as e:
            err_msg = get_msg(e)
    except Exception as e:
        err_msg = get_msg(e)
    return err_msg



def create_variable(project_id: str, site_id: str, inst_id: str, unit_map: dict):
    try:
        request_body = []

        for var in unit_map:
            request_body.append({
                "var_id": var,
                "var_name": var,
                "units": unit_map[var]
            })

        # Create variables in bulk
        result = permitted_client.streams.create_variable(project_id=project_id,
                                                 site_id=site_id,
                                                 inst_id=inst_id,
                                                 request_body=request_body)
        
        return None
    except Exception as e:
        msg = e
        if hasattr(e, 'message'):
            msg = e.message
        return msg 


# Function that will be executed by the threads
def process_file(data_path):

    fname = basename(data_path)
    global count
    global project_cache
    # ... (processing logic for the file)
    # Tapis Structure:
    #   Project (MesoNet) -> Site (InstID+Name) -> Instrument (MetData/SoilData, MinMax, RFMin, SysInfo) -> Variables -> Measurements
    # site_id:
    #   <STATION ID>
    # inst_id:
    #   <STATION ID>_ + "MetData", "SysInfo" (WILL IMPLEMENT LATER), "MinMax", "RFMin"

    # File Name Convention: <STATION ID>_<STATION NAME>_<FILETYPE>.DAT
    fname_splitted = fname.split("_")
    
    #get last part of fname and strip out file extension
    file_type = fname_splitted[len(fname_splitted) - 1].split(".")[0]
    
    station_id = fname_splitted[0]
    site_id = station_id + "_" + iteration  # STATION ID
    
    station_name = fname_splitted[1] # Station Name
    instrument_id = site_id + "_" + file_type

    #switch to simplified form when alias map stripped out (may have to process exceptions)
    ftype_alias_map = alias_map.get(file_type) or {}
    universal_alias_map = ftype_alias_map.get("universal") or {}
    id_alias_map = ftype_alias_map.get(station_id) or {}

    #for master variable list in file need to find variable names not in this map and use reflexive
    file_alias_map = {**universal_alias_map, **id_alias_map}

    # Check if site exists, else create site and instruments
    logger2.info(station_id)
    logger2.info(station_name)

    site_cache = None
    with site_create_lock:
        #check site cache for site
        global_site_cache = tenant_cache["global_site_cache"]
        #if site is not in project_cache then try to create
        if not site_id in global_site_cache:
            result = create_site(site_id, station_name, station_id)
            # if result is not None and "already use in project namepsace" not in result and "already exists" not in result:
            #     logger.error("Unable to create site/instruments:")
            #     logger.error("file: %s", fname)
            #     logger.error("error: %s", result)
            #     return False
            #add the site to the global cache and project cache
            global_site_cache.append(site_id)
        site_cache = project_cache.get(site_id)
        if site_cache is None:
            site_cache = {}
            project_cache[site_id] = site_cache
    
    instrument_cache = None
    with instrument_create_lock:
        instrument_cache = site_cache.get(instrument_id)
        if instrument_cache is None:
            result = create_instrument()

            instrument_cache = []
            site_cache[instrument_id] = instrument_cache
    



    with open(data_path, "r", encoding="utf8", errors="backslashreplace") as file:
        inst_data_file = file.readlines()

        #REPLACE WITH CSV READER
        # Grabbing the list of variables from the file
        list_vars = inst_data_file[1].strip().replace("\"", "").split(",")
        #translate variable names
        list_vars = [file_alias_map.get(var) or var for var in list_vars]

        #strip timestamp and record columns and ensure uniqueness
        file_vars = set(list_vars[2:])


        
        try:
            with var_create_lock:

                var_cache = set(instrument_cache)
                cache_diff = file_vars - var_cache
                #if there are variables not in the cache, get the tapis vars and create missing ones
                if len(cache_diff) > 0:
                    # replace with cache file
                    result = permitted_client.streams.list_variables(project_id=project_id, site_id=site_id, inst_id=instrument_id)
                    tapis_vars = {i.var_id for i in result}

                    #get file vars that are not in tapis
                    unknown_vars = file_vars - tapis_vars
        
                    if len(unknown_vars) > 0:
                        
                        list_units = inst_data_file[2].strip().replace("\"", "").split(",")
                        unit_map = {}
                        #zip vars and units and filter
                        for i in range(len(list_vars)):
                            if list_vars[i] in unknown_vars:
                                unit_map[list_vars[i]] = list_units[i]
                        result = create_variable(project_id, site_id, instrument_id, unit_map)
                        if result is not None:
                            logger.error("Unable to create variable:")
                            logger.error("file: %s", fname)
                            logger.error("error: %s", result)
                            return False

                    var_cache += file_vars
                    site_cache[instrument_id] = list(var_cache)

        except Exception as e:
            msg = e
            if hasattr(e, 'message'):
               msg = e.message
            logger.error("Unable to list variables:")
            logger.error("file: %s", fname)
            logger.error("error: %s", msg)
            return False

        
        # Parsing the measurements for each variable
        variables = []
        for i in range(4, len(inst_data_file)):
            measurements = inst_data_file[i].strip().replace(
                "\"", "").split(",")
            measurement = {}
            measurement_time = measurements[0].split(" ")

            #handle 24:00:00 formatting for midnight
            if int(measurement_time[1].split(":")[0]) > 23:
                time_string = measurement_time[0] + " 23:59:59"
                time_string = datetime.strptime(
                    time_string, '%Y-%m-%d %H:%M:%S')
                time_string += timedelta(seconds=1)
            else:
                time_string = datetime.strptime(
                    measurements[0], '%Y-%m-%d %H:%M:%S')

            measurement['datetime'] = time_string.isoformat()+"-10:00"


            for j in range(2, len(measurements)):
                measurement[list_vars[j]] = measurements[j]
            variables.append(measurement)
        # Creating the Tapis measurements
        try:
            result = permitted_client.streams.create_measurement(
                inst_id=instrument_id, vars=variables, )
        except Exception as e:
                msg = e
                if hasattr(e, 'message'):
                    msg = e.message
                logger.error("Unable to create measurement:")
                logger.error("file: %s", fname)
                logger.error("error: %s", e)
                return False
        
        # Update the count value in a thread-safe manner
        with count_lock:
            count += 1
            logger2.info("Progress: %d/%d", count, len(listdir(data_dir)))
        
        return True

# Define a function that handles the parallel processing of all files
def process_files_in_parallel(data_dir, num_workers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Get a list of all the file paths in the data directory
        file_paths = [join(data_dir, fname) for fname in listdir(data_dir) if isfile(join(data_dir, fname)) and fname.endswith(".dat")]

        # Submit the processing of each file to the ThreadPoolExecutor
        results = list(executor.map(process_file, file_paths))

    return results

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
    if (args.verbose):
        stdout_handler = logging.StreamHandler()
        logger.addHandler(stdout_handler)
        logger2.addHandler(stdout_handler)

    permitted_username = args.username #"testuser2"
    permitted_user_password = args.password #"testuser2"

    # iteration of test
    # TODO: can be removed for final revision
    iteration = args.iteration

    start_time = time.time()

    try:
        # #Create python Tapis client for user
        permitted_client = Tapis(base_url=base_url,
                                 username=permitted_username,
                                 password=permitted_user_password,
                                 account_type='user',
                                 tenant_id=tenant
                                 )

        # Generate an Access Token that will be used for all API calls
        permitted_client.get_tokens()

    except Exception as e:
        handle_error(e, prepend_msg = "Error: Tapis Client not created -")

    # TODO: remove iteration and _test_
    project_id = args.project_id + '_' + iteration


    cache = {}
    if args.cache_file is not None:
        try:
            with open(args.cache_file) as f:
                cache = json.load(f)
        except:
            logger.error("Could not open cache file. A new cache file will be created.")
    

    #allow multi tenant support
    url_cache = cache.get(args.tapis_url)
    if url_cache is None:
        url_cache = {}
        cache[args.tapis_url] = url_cache
    tenant_cache = url_cache.get(args.tenant)
    if tenant_cache is None:
        tenant_cache = {
            "project_cache": {},
            #instruments are global not project based
            "global_site_cache": []
        }
        url_cache[args.tenant] = tenant_cache
    project_cache = tenant_cache["project_cache"].get(project_id)

    #if project not in cache check if it exists and if it doesn't attempt to create it
    if project_cache is None:
        project_exist = False
        # Checks if project exists (can be removed once code is finalize)
        try:
            permitted_client.streams.get_project(project_id=project_id)
            project_exist = True
        except Exception as e:
            pass

        if not project_exist:
            try:
                logger.error("trying to create PROJECT")
                permitted_client.streams.create_project(
                    project_name=project_id, owner=args.username, pi=args.username)
            except Exception as e:
                handle_error(e)
        #add to the cache
        project_cache = {}
        tenant_cache[project_id] = project_cache

    
    

    data_dir = args.data_dir

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
    var_create_lock = threading.Lock()
    site_create_lock = threading.Lock()
    instrument_create_lock = threading.Lock()

    # Define the number of parallel workers
    num_workers = args.threads #6
    logger2.info("Progress: %d/%d", count, len(listdir(data_dir)))
    # Call the function to process files in parallel
    results = process_files_in_parallel(data_dir, num_workers)

    #write out cache file if specified
    if args.cache_file is not None:
        with open(args.cache_file, "w") as f:
            cache = json.dump(cache, f)

    end_time = time.time()
    exec_time = end_time - start_time

    logger2.info("Files parsing complete: success: %d, failed: %d, time: %.2f seconds", count, len(listdir(data_dir)) - count, exec_time)
