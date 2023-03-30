import urllib
import sys
from datetime import date, timedelta, datetime
import argparse
from tapipy.tapis import Tapis
import logging
from logging import FileHandler
import csv
from datetime import datetime
from os.path import exists, basename, dirname, isfile, join
from os import makedirs, remove, listdir
import json
import pickle

def create_site(fname: str, project_id: str, site_id: str, inst_id: str) -> bool:
    logger.debug(f"\033[1;31m ---Start of Creating Site for {fname}---\033[0m")
    try:
        result, debug = permitted_client.streams.create_site(project_id=project_id, 
                                                             request_body=[{
                                                                 "site_name":site_id,
                                                                 "site_id":site_id,
                                                                 "latitude": 50, # needs to be changed
                                                                 "longitude": 10, # needs to be changed
                                                                 "elevation": 2, # needs to be changed
                                                                 "description":inst_id+"_"+site_id}], _tapis_debug=True)
        logger.debug(result)
        logger.debug(f"\033[1;31m ---End of Creating Site for {fname}---\033[0m")
        return True
    except Exception as e:
        logger.error(f"Error: Site was not created - {e.message}")
        return False

def create_instrument(fname: str, project_id: str, site_id: str, inst_id: str) -> bool:
    try:
        logger.debug(f"\033[1;31m ---Start of Creating Instrument for {fname}---\033[0m")
        result, debug = permitted_client.streams.create_instrument(project_id=project_id,
                                                                   site_id=site_id,
                                                                   request_body=[{
                                                                       "inst_name":instrument_id,
                                                                       "inst_id": instrument_id,
                                                                       "inst_description": instrument_id+"_"+site_id}], _tapis_debug=True)
        logger.debug(result)
        logger.debug(f"\033[1;31m ---End of Creating Instrument for {fname}---\033[0m")

        return True
    except Exception as e:
        logger.error(f"Error: Instrument was not created - {e.message}")
        return False

def create_variable(fname: str, project_id: str, site_id: str, inst_id: str, list_vars: list, list_units: list) -> bool:
   try:
       logger.debug(f"\033[1;31m --- Start of Creating Variables for {fname}---\033[0m")
       logger.debug(list_units)

       request_body = []

       for i in range(2, len(list_vars)):
           request_body.append({
               "var_id": list_vars[i],
               "var_name": list_vars[i],
               "units": list_units[i]
           })

       # Create variables in bulk
       result, debug = permitted_client.streams.create_variable(project_id=project_id,
                                               site_id=site_id,
                                               inst_id=inst_id,
                                               request_body=request_body,_tapis_debug=True)
       logger.debug(result) 
       logger.debug(f"\033[1;31m --- End of Creating Variables for {fname}---\033[0m")
       return True
   except Exception as e:
       logger.debug("Error: Variables was not created - {e.message}")
       return False

# Argument parser
parser = argparse.ArgumentParser(
    prog="streams_processor.py",
    description=""
)

parser.add_argument("-d", "--debug", action="store_true", help="turn on debug mode")
parser.add_argument("iteration", help="set the iteration number for project version")
parser.add_argument("-v", "--verbose", action="store_true", help="turn on verbose mode")

args = parser.parse_args()

#Set Tapis Tenant and Base URL
tenant="dev"
base_url = 'https://' + tenant + '.develop.tapis.io'

if(args.debug):
    level=logging.DEBUG
else:
    level=logging.INFO


handlers = []

file_handler = FileHandler('parser.log')

handlers.append(file_handler)

if(args.verbose):
    stdout_handler = logging.StreamHandler()
    handlers.append(stdout_handler)

logging.basicConfig(level=level,
                    format='%(asctime)s %(levelname)s: %(message)s [%(pathname)s:%(lineno)d]',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    handlers=handlers)

logger = logging.getLogger()

permitted_username = "testuser2"
permitted_user_password = "testuser2"

iteration = args.iteration

try:
    # #Create python Tapis client for user
    permitted_client = Tapis(base_url= base_url, 
                            username=permitted_username,
                            password=permitted_user_password, 
                            account_type='user', 
                            tenant_id=tenant
                            )

    #Generate an Access Token that will be used for all API calls
    permitted_client.get_tokens()
except Exception as e:
    print("Error: ", e.message, file=sys.srderr)

project_id = 'Mesonet_test_' + iteration

#data_dir = "/mnt/c/Users/Administrator/ikewai_data_upload/streams_processor/testing/data"
data_dir = "/Users/wongy/Desktop/testing/data"

#process all the files in the data dir
for fname in listdir(data_dir):
    #get the full path
    data_file = join(data_dir, fname)
    #make sure it is a file, otherwise skip
    if isfile(data_file) and "MetData" in fname:
        row_to_file = {}
        date_to_file = {}

        header = ""
        outfile = None
        file_date = None
        row_i = 0
        timestamp_col = 0

        with open(data_file, "r", encoding = "utf8", errors = "backslashreplace") as file:
            logging.info(f"Parsing {fname} into Tapis...")

            site_id = fname.split("_")[1] + "_test_" + iteration
            instrument_id = fname.split("_")[0] + "_test_" + iteration

            logger.debug(f"Site Id: {site_id}, Instrument Id: {instrument_id}")

            inst_data_file = file.readlines()
            
            list_vars = inst_data_file[1].strip().replace("\"", "").split(",")
            
            logger.debug(f"\033[1;31m ---Start of parsing measurement---\033[0m")
            # Parsing the measurements for each variable
            variables = []
            for i in range(4, len(inst_data_file)):
                measurements = inst_data_file[i].strip().replace("\"", "").split(",")
                measurement = {}
                time = measurements[0].split(" ")

                if(int(time[1].split(":")[0]) > 23):
                    time_string = time[0] + " 23:59:59"
                    time_string = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')
                    time_string += timedelta(seconds=1)
                else:
                    time_string = datetime.strptime(measurements[0], '%Y-%m-%d %H:%M:%S')

                measurement['datetime'] = time_string.isoformat()+"-10:00"
                for j in range(2, len(measurements)):
                    measurement[list_vars[j]] = float(measurements[j])
                variables.append(measurement)
            
            # Creating the Tapis measurements
            try:
                result = permitted_client.streams.create_measurement(inst_id=instrument_id, vars=variables)
                logger.debug("\033[1;31m ---End of parsing measurement---\033[0m")
            except Exception as e:
                if e.message ==  "No Instrument found matching inst_id.":
                    if create_site(fname, project_id, site_id, instrument_id):
                        pass
                    else:
                        continue

                    if create_instrument(fname, project_id, site_id, instrument_id):
                        pass
                    else:
                        continue                        

                    list_units = inst_data_file[2].strip().replace("\"", "").split(",")
                    if create_variable(fname, project_id, site_id, instrument_id, list_vars, list_units):
                        pass
                    else:
                        continue

                    result = permitted_client.streams.create_measurement(inst_id=instrument_id, vars=variables)
                elif e.message == "Unrecognized exception type: <class 'KeyError'>. Exception: 'variables'":
                    list_units = inst_data_file[2].strip().replace("\"", "").split(",")
                    if create_variable(fname, project_id, site_id, instrument_id, list_vars, list_units):
                        result = permitted_client.streams.create_measurement(inst_id=instrument_id, vars=variables)
                    else:
                        continue
                else:
                    logger.error(f"Error: unable to parse measurement into Tapis for {fname} - {e.message}")
                    continue
            logging.info(f"Done parsing {fname} into Tapis")


