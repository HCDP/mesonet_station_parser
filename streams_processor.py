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

def get_location_info(station_id: str) -> tuple:
    master_url = "https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/hi_mesonet_sta_status.csv"
    
    dtype_dict = {'sta_ID': str}
    
    df = pd.read_csv(master_url, dtype=dtype_dict)
    
    # Check if station exist in csv
    if len(df[df['sta_ID'] == station_id]) == 1:
        logger.info("Station's meta data for %s found", station_id)
        latitude = df[df['sta_ID'] == station_id]["LAT"]
        longitude = df[df['sta_ID'] == station_id]["LON"]
        elevation = df[df['sta_ID'] == station_id]["ELEV"]

        return (latitude, longitude, elevation)
    
    logger.info("Station's meta data for %s not found", station_id)
    return (20, -158, 0) # somewhere left of Hawaii island in the ocean

def create_site(fname: str, project_id: str, site_id: str, site_name: str) -> bool:
    logger.info("\033[1;31m --- Start of Creating Site for %s ---\033[0m", fname)
    try:
        (latitude, longitude, elevation) = get_location_info(station_id=site_id)

        result = permitted_client.streams.create_site(project_id=project_id,
                                                             request_body=[{
                                                                 "site_name": site_id,
                                                                 "site_id": site_id,
                                                                 "latitude": latitude, 
                                                                 "longitude": longitude,
                                                                 "elevation": elevation,
                                                                 "description": site_name}])
        logger.info(result)
        logger.info(
            "\033[1;31m --- End of Creating Site for %s ---\033[0m", fname)
        
    except Exception as error:
        logger.info("Error: Site was not created for %s - %s", fname, error.message)
        return False


    try:
        # creating instrument in bulk (MetData, MinMax, SysInfo, RFMin)
        logger.info("\033[1;31m --- Start of Creating Instruments for %s ---\033[0m", fname)
        result = permitted_client.streams.create_instrument(project_id=project_id,
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
        logger.info(result)
        logger.info("\033[1;31m --- End of Creating Instruments for %s ---\033[0m", fname)
    except Exception as e:
        logger.info("Error: Instruments was not created for %s - %s", fname, error.message)
        return False

    return True


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
            logger.info("same")
            df = pd.read_csv(conversion_dir + "/Universal.csv")
    else:
        df = pd.read_csv(conversion_dir + "/Universal.csv")

    standard = df['Standard short name'].tolist()
    raw = df['Raw data column name'].tolist()

    return {raw[i]: standard[i] for i in range(len(standard))}

def create_variable(fname: str, project_id: str, site_id: str, inst_id: str, list_vars: list, list_units: list) -> bool:
    try:
        logger.info(
            "\033[1;31m --- Start of Creating Variables for %s ---\033[0m", fname)
        logger.info(list_units)
        logger.info(list_vars)

        request_body = []

        station_id = site_id.split("_")[0]

        if inst_id.split("_")[-1] == "MetData":
            standard_var = standardized_vars(station_id, list_vars)
        else:
            standard_var = {list_vars[i]: list_vars[i] for i in range(len(list_vars))}

        logger.info(standard_var)
        for i in range(2, len(list_vars)):
            request_body.append({
                "var_id": standard_var[list_vars[i]],
                "var_name": standard_var[list_vars[i]],
                "units": list_units[i]
            })

        # Create variables in bulk
        result, debug = permitted_client.streams.create_variable(project_id=project_id,
                                                                 site_id=site_id,
                                                                 inst_id=inst_id,
                                                                 request_body=request_body, _tapis_debug=True)
        logger.info(result)
        logger.info(
            "\033[1;31m --- End of Creating Variables for %s---\033[0m", fname)
        return True
    except Exception as error:
        logger.info("Error: Variables was not created - %s", error.message)
        return False

# Argument parser
parser = argparse.ArgumentParser(
    prog="streams_processor.py",
    description=""
)

parser.add_argument("-d", "--debug", action="store_true",
                    help="turn on debug mode")
parser.add_argument(
    "iteration", help="set the iteration number for project version")
parser.add_argument("-v", "--verbose", action="store_true",
                    help="turn on verbose mode")

args = parser.parse_args()

# Set Tapis Tenant and Base URL
tenant = "dev"
base_url = 'https://' + tenant + '.develop.tapis.io'

if (args.debug):
    level = logging.DEBUG
else:
    level = logging.INFO



file_handler = FileHandler('parser.log')


logger = logging.getLogger('Logger1')

logger.setLevel(level)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s [%(pathname)s:%(lineno)d]')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if (args.verbose):
    stdout_handler = logging.StreamHandler()
    logger.addHandler(stdout_handler)

logger2 = logging.getLogger('Logger2')
logger2.setLevel(level)

file_handler2 = FileHandler('execution.log')
file_handler2.setFormatter(formatter)
logger2.addHandler(file_handler2)

permitted_username = "testuser2"
permitted_user_password = "testuser2"

iteration = args.iteration

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
    logger.error("Error: Tapis Client not created - %s", e.message)
    exit()

project_id = 'Mesonet_prod_test_' + iteration

project_exist = False
# Checks if project exists (can be removed once code is finalize)
try:
    permitted_client.streams.get_project(project_id=project_id)
    project_exist = True
except Exception as e:
    print(e.message)

if project_exist == False:
    try:
        logger.info("\033[1;31m --- Creating Project for %s---\033[0m", project_id)
        permitted_client.streams.create_project(
            project_name=project_id, owner="testuser2", pi="testuser2")
        logger.info("\033[1;31m --- End of Creating Project for %s---\033[0m", project_id)
    except Exception as e:
        logger.error("Project does not exist, exiting script...")
        exit(-1)

data_dir = "/mnt/c/Campbellsci/LoggerNet/Data"

# Count how many files were parsed into streams-api vs total num of files
count = 0

# process all the files in the data dir
for fname in listdir(data_dir):
    # get the full path
    data_file = join(data_dir, fname)
    # make sure it is a .dat file, otherwise skip
    if isfile(data_file) and fname.endswith(".dat"):
        logger.info("Working on %s", fname)

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
            logger.info("File Category: MetaData/SoilData")
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
            logger.warning("Error: %s is not a file in one of the 4 categories", fname)
            continue

        site_id = fname_splitted[0] + "_" + iteration  # STATION ID
        station_id = fname_splitted[0]
        station_name = fname_splitted[1] # Station Name
        instrument_id = site_id + "_" + file_type


        # Check if site exists, else create site and instruments
        try:
            permitted_client.streams.get_site(project_id=project_id, site_id=site_id)
        except Exception as e: 
            if create_site(fname, project_id, site_id, station_name) == False:
                logger.error("Unable to create site/instruments for %s", site_id)
                continue


        with open(data_file, "r", encoding="utf8", errors="backslashreplace") as file:
            logger.info("Parsing %s into Tapis...", fname)

            logger.info("Site Id: %s, Instrument Id: %s", site_id, instrument_id)

            inst_data_file = file.readlines()

            # Grabbing the list of variables from the file
            list_vars = inst_data_file[1].strip().replace("\"", "").split(",")

            # Check if variables exist, else create variables
            try:
                result = permitted_client.streams.list_variables(project_id=project_id, site_id=site_id, inst_id=instrument_id)
                
                if len(result) == 0:
                    list_units = inst_data_file[2].strip().replace("\"", "").split(",")
                    if create_variable(fname, project_id, site_id, instrument_id, list_vars, list_units) == False:
                        logger.error("Variable not created, Skipping %s", fname)
                        continue
            except Exception as e:
                logger.error(e.message)
                logger.error("Skipping %s...", fname)
                continue
              

            # Check filetype, if MinMax -> standardize variable names
            if file_type == "MetData":
                standard_var = standardized_vars(station_id, list_vars)
            else:
                standard_var = {list_vars[i]: list_vars[i] for i in range(len(list_vars))}
            
            logger.info("\033[1;31m ---Start of parsing measurement---\033[0m")

            # Parsing the measurements for each variable
            variables = []
            for i in range(4, len(inst_data_file)):
                measurements = inst_data_file[i].strip().replace(
                    "\"", "").split(",")
                measurement = {}
                time = measurements[0].split(" ")

                if (int(time[1].split(":")[0]) > 23):
                    time_string = time[0] + " 23:59:59"
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
                logger.info(
                    "\033[1;31m ---End of parsing measurement---\033[0m")
            except Exception as e:
                    logger.error(
                        "Error: unable to parse measurement into Tapis for %s - %s", fname, e.message)
                    logger.error("Skipping %s...", fname)
                    continue
            
            count += 1
            logger.info(f"Done parsing %s into Tapis", fname)

logger2.info("%d files out of %d were parsed into streams-api", count, len(listdir(data_dir)))
