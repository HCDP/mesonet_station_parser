from datetime import date, timedelta, datetime
import argparse
from tapipy.tapis import Tapis
import logging
from logging import FileHandler
from os.path import basename, isfile, join, exists
from os import listdir
import pandas as pd
import time
import concurrent.futures
import threading
import json

def get_location_info(station_id: str) -> tuple:
    master_url = "https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/hi_mesonet_sta_status.csv"
    dtype_dict = {'sta_ID': str}
    
    df = pd.read_csv(master_url, dtype=dtype_dict)
    
    # Check if station exist in csv
    if len(df[df['sta_ID'] == station_id]) == 1:
        logger.info("Station's meta data for %s found", station_id)
        latitude = float(df[df['sta_ID'] == station_id].iloc[0]["LAT"])
        longitude = float(df[df['sta_ID'] == station_id].iloc[0]["LON"])
        elevation = float(df[df['sta_ID'] == station_id].iloc[0]["ELEV"])

        logger.info("%d %d %d", latitude, longitude, elevation)
        return (latitude, longitude, elevation)
    
    logger.info("Station's meta data for %s not found", station_id)
    return (20, -158, 0) # somewhere left of Hawaii island in the ocean

def create_site(fname: str, project_id: str, site_id: str, site_name: str) -> bool:
    logger.info(f"Trying to create site {site_id}")
    try:
        station_id = site_id.split("_")[0] # can be removed for final prod
        (latitude, longitude, elevation) = get_location_info(station_id=station_id)

        permitted_client.streams.create_site(project_id=project_id,
                                                    request_body=[{
                                                        "site_name": site_id,
                                                        "site_id": site_id,
                                                        "latitude": latitude, 
                                                        "longitude": longitude,
                                                        "elevation": elevation,
                                                        "description": site_name,
                                                        "metadata": {
                                                            "station_name": site_name
                                                        }}])
    except Exception as e:
        msg = e
        if hasattr(e, 'message'):
            msg = e.message
        return msg

    try:
        # creating instrument in bulk (MetData, MinMax, SysInfo, RFMin)
        permitted_client.streams.create_instrument(project_id=project_id,
                                                          site_id=site_id,
                                                          request_body=[
                                                              {
                                                               "inst_name": site_id + "_" + "MetData",
                                                               "inst_id": site_id + "_" + "MetData",
                                                               "inst_description": "MetData for " + site_id + "_" + site_name
                                                              },
                                                              {
                                                               "inst_name": site_id + "_" + "MinMax",
                                                               "inst_id": site_id + "_" + "MinMax",
                                                               "inst_description": "MinMax data for " + site_id + "_" + site_name
                                                              },
                                                              {
                                                               "inst_name": site_id + "_" + "SysInfo",
                                                               "inst_id": site_id + "_" + "SysInfo",
                                                               "inst_description": "SysInfo for " + site_id + "_" + site_name
                                                              },
                                                              {
                                                               "inst_name": site_id + "_" + "RFMin",
                                                               "inst_id": site_id + "_" + "RFMin",
                                                               "inst_description": "RFMin data for " + site_id + "_" + site_name
                                                              }
                                                              ])
    except Exception as e:
        msg = e
        if hasattr(e, 'message'):
            msg = e.message
        return msg 

    with open("site.cache", "a") as file:
        file.write(f"{station_id}\n")

    return None


def standardized_vars(station_id: str, list_vars: list) -> list:
    conversion_dir = "./standard_var"
    standard = []
    logger2.info(station_id[1:])  
    if station_id in ["0119","0141", "0143", "0145", "0151", "0152", "0153", "0154", "0201", "0281", "0282", "0283", "0286", "0287", "0288", "0501", "0502", "0521", "0601", "0602"]:
        sf = pd.read_csv(conversion_dir + "/0" + station_id[1:] + ".csv")
        lf = pd.read_csv(conversion_dir+"/Universal.csv")
        df = pd.concat([sf, lf], ignore_index=True)
    # elif station_id in ["0502", "0601"]:
    #     df = pd.read_csv(conversion_dir + "/" + station_id[1:] + ".csv")
    #     if sorted(df['alias_nagitme'].tolist()) != sorted(list_vars[2:]):
    #         df = pd.read_csv(conversion_dir + "/Universal.csv")
    else:
        df = pd.read_csv(conversion_dir + "/Universal.csv")

    standard = df['standard_name'].tolist()
    raw = df['alias_name'].tolist()

    return {raw[i]: standard[i] for i in range(len(standard))}

def create_variable(fname: str, project_id: str, site_id: str, inst_id: str, list_vars: list, list_units: list) -> bool:
    try:
        request_body = []

        station_id = site_id.split("_")[0]

        if inst_id.split("_")[-1] == "MetData":
            standard_var = standardized_vars(station_id, list_vars)
        else:
            standard_var = {list_vars[i]: list_vars[i] for i in range(len(list_vars))}

        for i in range(2, len(list_vars)):
            request_body.append({
                "var_id": standard_var[list_vars[i]],
                "var_name": standard_var[list_vars[i]],
                "unit": list_units[i]
            })

        # Create variables in bulk
        result = permitted_client.streams.create_variable(project_id=project_id,
                                                 site_id=site_id,
                                                 inst_id=inst_id,
                                                 request_body=request_body)
        
        # file_type = inst_id.split("_")[-1]
        # with open("var.cache"+ iteration + file_type, "r") as var_file:
        #     cached_vars = json.load(var_file)

        #     if station_id in cached_vars.keys():
        #         cached_vars[station_id].extend(list_vars[2:])
        #     else:
        #         cached_vars[station_id] = list_vars[2:]
        
        # with open("var.cache"+ iteration + file_type,"w") as var_file:
        #     logger.error("writing cache: %s - %s", cached_vars[station_id], inst_id)
        #     json.dump(cached_vars, var_file)
        
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
    # ... (processing logic for the file)
    # Tapis Structure:
    #   Project (MesoNet) -> Site (InstID+Name) -> Instrument (MetData/SoilData, MinMax, RFMin, SysInfo) -> Variables -> Measurements
    # site_id:
    #   <STATION ID>
    # inst_id:
    #   <STATION ID>_ + "MetData", "SysInfo" (WILL IMPLEMENT LATER), "MinMax", "RFMin"

    # File Name Convention: <STATION ID>_<STATION NAME>_<FILETYPE>.DAT
    fname_splitted = fname.split("_")

    # Checks what type of file it is (MetData, SoilData, SysInfo, MinMax, RFMin)
    file_type = ""
    if "metdata" in fname.lower() or "soildata" in fname.lower():
        logger.info("File Category: MetData/SoilData")
        file_type = "MetData"
    elif "sysinfo" in fname.lower():
        logger.info("File Category: SysInfo")
        file_type = "SysInfo"
    elif "minmax" in fname.lower():
        logger.info("File Category: MinMax")
        file_type = "MinMax"
    elif "rfmin" in fname.lower():
        logger.info("File Category: RFMin")
        file_type = "RFMin"
    else:
        logger.error("File in not in one of the 4 categories")
        logger.error("file: %s", fname)
        return False

    station_id = fname_splitted[0]
    site_id = station_id + "_" + iteration
    station_name = fname_splitted[1] # Station Name
    instrument_id = site_id + "_" + file_type

    # Check if site exists, else create site and instruments
    logger2.info(station_id)
    logger2.info(station_name)
    if exists("./site.cache"):
        file = open("site.cache", "r")
    else:
        file = open("site.cache", "w+")
    
    cached_sites = file.read()
    file.close()
    logger2.info(cached_sites)
    if station_id not in cached_sites:
        with site_create_lock:
            file = open("site.cache", "r")
            cached_sites = file.read()
            file.close()
            if station_id not in cached_sites:
                result = create_site(fname, project_id, site_id, station_name)
                if result is not None and "already use in project namepsace" not in result and "already exists" not in result:
                    logger.error("Unable to create site/instruments:")
                    logger.error("file: %s", fname)
                    logger.error("error: %s", result)
                    return False

    with open(data_path, "r", encoding="utf8", errors="backslashreplace") as file:
        inst_data_file = file.readlines()

        # Grabbing the list of variables from the file
        list_vars = inst_data_file[1].strip().replace("\"", "").split(",")

        # Check if variables exist, else create variables
        # if exists("./var.cache" + iteration + file_type):
        #     file = open("var.cache" + iteration + file_type, "r")
        # else:
        #     file = open("var.cache" + iteration + file_type, "w")
        #     json.dump({}, file)
        #     file.close()
        #     file = open("var.cache"+iteration + file_type, "r")
    
        # cached_vars = json.load(file)
        # file.close()

        # if station_id not in cached_vars.keys():
        #     with var_create_lock:
        #         file = open("var.cache"+iteration + file_type, "r")
        #         cached_vars = json.load(file)
        #         file.close()
        #         if station_id not in cached_vars.keys():
        #             list_units = inst_data_file[2].strip().replace("\"", "").split(",")
        #             result = create_variable(fname, project_id, site_id, instrument_id, list_vars, list_units)
        #             if result is not None:
        #                 logger.error("Unable to create variable:")
        #                 logger.error("file: %s", fname)
        #                 logger.error("error: %s", result)
        #                 return False
        # elif file_type == "MetData" and station_id in cached_vars.keys():
        #         file_vars = set(list_vars[2:])
        #         if file_vars.difference(set(cached_vars[station_id])):
        #             with var_create_lock:
        #                 file = open("var.cache"+iteration + file_type, "r")
        #                 cached_vars = json.load(file)
        #                 file.close()
        #                 if file_vars.difference(set(cached_vars[station_id])):
        #                     list_units = inst_data_file[2].strip().replace("\"", "").split(",")
        #                     result = create_variable(fname, project_id, site_id, instrument_id, list_vars, list_units)
        #                     if result is not None:
        #                         logger.error("Unable to create variable:")
        #                         logger.error("file: %s", fname)
        #                         logger.error("error: %s", result)
        #                         return False

        
        try:
            # replace with cache file
            result = permitted_client.streams.list_variables(project_id=project_id, site_id=site_id, inst_id=instrument_id)
            tapis_vars = {i.var_id for i in result}

            file_vars = set(list_vars[2:])
            if file_type == "MetData":
                file_vars = set(standardized_vars(station_id, list_vars).values())
            if len(tapis_vars) == 0 or file_vars.difference(tapis_vars):
                with var_create_lock:
                    result = permitted_client.streams.list_variables(project_id=project_id, site_id=site_id, inst_id=instrument_id)
                    tapis_vars = {i.var_id for i in result}
                    if len(tapis_vars) == 0 or file_vars.difference(tapis_vars):
                        list_units = inst_data_file[2].strip().replace("\"", "").split(",")
                        result = create_variable(fname, project_id, site_id, instrument_id, list_vars, list_units)
                        if result is not None:
                            logger.error("Unable to create variable:")
                            logger.error("file: %s", fname)
                            logger.error("error: %s", result)
                            return False
        except Exception as e:
            msg = e
            if hasattr(e, 'message'):
               msg = e.message
            logger.error("Unable to list variables:")
            logger.error("file: %s", fname)
            logger.error("error: %s", msg)
            return False
          
        # Check filetype, if MinMax -> standardize variable names
        if file_type == "MetData":
            standard_var = standardized_vars(station_id, list_vars)
        else:
            standard_var = {list_vars[i]: list_vars[i] for i in range(len(list_vars))}
        
        # Parsing the measurements for each variable
        variables = []
        for i in range(4, len(inst_data_file)):
            measurements = inst_data_file[i].strip().replace(
                "\"", "").split(",")
            measurement = {}
            measurement_time = measurements[0].split(" ")

            if (int(measurement_time[1].split(":")[0]) > 23):
                time_string = measurement_time[0] + " 23:59:59"
                time_string = datetime.strptime(
                    time_string, '%Y-%m-%d %H:%M:%S')
                time_string += timedelta(seconds=1)
            else:
                time_string = datetime.strptime(
                    measurements[0], '%Y-%m-%d %H:%M:%S')

            measurement['datetime'] = time_string.isoformat()+"-10:00"


            for j in range(2, len(measurements)):
                measurement[standard_var[list_vars[j]]] = measurements[j]
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
    parser.add_argument("-pid","--project_id",help="provide the Tapis Streams Project ID to parse the data into")
    parser.add_argument("-t","--tenant", help="Tapis tenant to use like dev")
    parser.add_argument("-tu","--tapis_url",help="Tapis base URL to use like https://dev.develop.tapis.io")
    parser.add_argument("-th","--threads",type=int, help="Number of threads to use to process the mesonet files in parallel")
    parser.add_argument("-u","--username",help="Tapis username that was read/write access to the project and data")
    parser.add_argument("-p","--password",help="The Tapis password for the username provided")

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

        # TODO: REMOVE!!!
        # permitted_client.base_url = "http://localhost:5001"

    except Exception as e:
        msg = e
        if hasattr(e, 'message'):
            msg = e.message
        logger.error("Error: Tapis Client not created - %s", msg)
        exit(-1)

    # TODO: remove iteration and _test_
    project_id = args.project_id + '_' + iteration

    project_exist = False
    # Checks if project exists (can be removed once code is finalize)
    try:
        permitted_client.streams.get_project(project_id=project_id)
        project_exist = True
    except Exception as e:
        pass

    if project_exist == False:
        try:
            logger.error("trying to create PROJECT")
            permitted_client.streams.create_project(
                project_name=project_id, owner="testuser2", pi="testuser2")
        except Exception as e:
            msg = e
            if hasattr(e, 'message'):
                msg = e.message
            logger.error("error: %s", msg)
            exit(-1)

    data_dir = args.data_dir #"E:\GitHub\mesonet_station_parser\data\\temp"
    # data_dir = "/mnt/c/Users/Administrator/ikewai_data_upload/streams_processor/testing/data"

    # Count how many files were parsed into streams-api vs total num of files
    count = 0

    # Initialize the lock
    count_lock = threading.Lock()
    var_create_lock = threading.Lock()
    site_create_lock = threading.Lock()

    # Define the number of parallel workers
    num_workers = args.threads #6
    logger2.info("Progress: %d/%d", count, len(listdir(data_dir)))
    # Call the function to process files in parallel
    results = process_files_in_parallel(data_dir, num_workers)

    end_time = time.time()
    exec_time = end_time - start_time

    logger2.info("Files parsing complete: success: %d, failed: %d, time: %.2f seconds", count, len(listdir(data_dir)) - count, exec_time)
