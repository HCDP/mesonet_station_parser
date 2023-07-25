import urllib
import sys
from datetime import date, timedelta, datetime
import argparse
from tapipy.tapis import Tapis
import logging
from logging import FileHandler
import csv
from os.path import exists, basename, dirname, isfile, join
from os import listdir
import json
import pandas as pd
import time

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
                                                        "description": site_name}])
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

    return None


def standardized_vars(station_id: str, list_vars: list) -> list:
    conversion_dir = "./standard_var"
    standard = []
   
    if station_id in ["0119", "0152", "0153"]:
        df = pd.read_csv(conversion_dir + "/119,152,153.csv")
    elif station_id in ["0141", "0143", "0151", "0154", "0281", "0282", "0283", "0286", "0287", "0288", "0501", "0521", "0602"]:
        df = pd.read_csv(conversion_dir + "/" + station_id[1:] + ".csv")
    elif station_id in ["0502", "0601"]:
        df = pd.read_csv(conversion_dir + "/" + station_id[1:] + ".csv")
        if sorted(df['Raw data column name'].tolist()) != sorted(list_vars[2:]):
            df = pd.read_csv(conversion_dir + "/Universal.csv")
    else:
        df = pd.read_csv(conversion_dir + "/Universal.csv")

    standard = df['Standard short name'].tolist()
    raw = df['Raw data column name'].tolist()

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
                "units": list_units[i]
            })

        # Create variables in bulk
        permitted_client.streams.create_variable(project_id=project_id,
                                                 site_id=site_id,
                                                 inst_id=inst_id,
                                                 request_body=request_body, _tapis_debug=True)
        return None 
    except Exception as e:
        msg = e
        if hasattr(e, 'message'):
            msg = e.message
        return msg 

# Argument parser
parser = argparse.ArgumentParser(
    prog="streams_processor.py",
    description=""
)

# TODO: might remove for final prod
parser.add_argument(
    "iteration", help="set the iteration number for project version")
parser.add_argument("-v", "--verbose", action="store_true",
                    help="turn on verbose mode")

args = parser.parse_args()

# Set Tapis Tenant and Base URL
tenant = "dev"
base_url = 'https://' + tenant + '.develop.tapis.io'


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

permitted_username = "testuser2"
permitted_user_password = "testuser2"

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
    msg = e
    if hasattr(e, 'message'):
        msg = e.message
    logger.error("Error: Tapis Client not created - %s", msg)
    exit(-1)

# TODO: remove iteration and _test_
project_id = 'Mesonet_prod_test_' + iteration

project_exist = False
# Checks if project exists (can be removed once code is finalize)
try:
    permitted_client.streams.get_project(project_id=project_id)
    project_exist = True
except Exception as e:
    pass

if project_exist == False:
    try:
        permitted_client.streams.create_project(
            project_name=project_id, owner="testuser2", pi="testuser2")
    except Exception as e:
        msg = e
        if hasattr(e, 'message'):
            msg = e.message
        logger.error("error: %s", msg)
        exit(-1)

data_dir = "/mnt/c/Users/Administrator/ikewai_data_upload/streams_processor/testing/data"

# Count how many files were parsed into streams-api vs total num of files
count = 0
progress = 0
# process all the files in the data dir
for fname in listdir(data_dir):
    progress += 1
    logger2.info("Progress: %d/%d", progress, len(listdir(data_dir)))
    # get the full path
    data_file = join(data_dir, fname)
    # make sure it is a .dat file, otherwise skip
    if isfile(data_file) and fname.endswith(".dat"):
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
            continue

        site_id = fname_splitted[0] + "_" + iteration  # STATION ID
        station_id = fname_splitted[0]
        station_name = fname_splitted[1] # Station Name
        instrument_id = site_id + "_" + file_type


        # Check if site exists, else create site and instruments
        try:
            permitted_client.streams.get_site(project_id=project_id, site_id=site_id)
        except Exception as e:
            result = create_site(fname, project_id, site_id, station_name)
            if result != None:
                logger.error("Unable to create site/instruments:")
                logger.error("file: %s", fname)
                logger.error("error: %s", result)
                continue


        with open(data_file, "r", encoding="utf8", errors="backslashreplace") as file:
            inst_data_file = file.readlines()

            # Grabbing the list of variables from the file
            list_vars = inst_data_file[1].strip().replace("\"", "").split(",")

            # Check if variables exist, else create variables
            try:
                result = permitted_client.streams.list_variables(project_id=project_id, site_id=site_id, inst_id=instrument_id)
                tapis_vars = [i.var_id for i in result]

                if list_vars not in tapis_vars:
                    list_units = inst_data_file[2].strip().replace("\"", "").split(",")
                    result = create_variable(fname, project_id, site_id, instrument_id, list_vars, list_units)
                    if result != None:
                        logger.error("Unable to create variable:")
                        logger.error("file: %s", fname)
                        logger.error("error: %s", result)
                        continue
            except Exception as e:
                msg = e
                if hasattr(e, 'message'):
                   msg = e.message
                logger.error("Unable to list variables:") 
                logger.error("file: %s", fname)
                logger.error("error: %s", msg)
                continue
              

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
                    inst_id=instrument_id, vars=variables)
            except Exception as e:
                    msg = e
                    if hasattr(e, 'message'):
                        msg = e.message
                    logger.error("Unable to create measurement:")
                    logger.error("file: %s", fname)
                    logger.error("error: %s", msg)
                    continue
            count += 1
            
end_time = time.time()
exec_time = end_time - start_time

logger2.info("Files parsing complete: success: %d, failed: %d, time: %.2f seconds", count, len(listdir(data_dir)) - count, exec_time)
