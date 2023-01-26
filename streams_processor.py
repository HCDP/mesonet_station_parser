### TODO: CHANGE SCRIPT TO CREATE SITE AND INSTRUMENT BEFORE LOOPING THROUGH DATES

import urllib
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


# Argument parser
parser = argparse.ArgumentParser(
    prog="streams_processor.py",
    description=""
)

### TODO: add a flag to indicate if instrument/site exist, or a seperate script, or a cache file
parser.add_argument("-d", "--debug", action="store_true", help="turn on debug mode")
### TODO: add a verbose argument

args = parser.parse_args()

#Set Tapis Tenant and Base URL
tenant="dev"
base_url = 'https://' + tenant + '.develop.tapis.io'

if(args.debug):
    level=logging.DEBUG
else:
    level=logging.INFO

file_handler = FileHandler('parser.log')

logging.basicConfig(level=level,
                    format='%(asctime)s %(levelname)s: %(message)s [%(pathname)s:%(lineno)d]',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    handlers=[file_handler])

logger = logging.getLogger()

permitted_username = "testuser2"
permitted_user_password = "testuser2"

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
    print("Error: ", e.message)

project_id = 'Mesonet_test_00'

data_dir = "/mnt/c/Campbellsci/LoggerNet/Data"

#process all the files in the data dir
for fname in listdir(data_dir):
    #get the full path
    data_file = join(data_dir, fname)
    #make sure it is a file, otherwise skip
    if isfile(data_file):
        row_to_file = {}
        date_to_file = {}

        header = ""
        outfile = None
        file_date = None
        row_i = 0
        timestamp_col = 0

        with open(data_file, "r", encoding = "utf8", errors = "backslashreplace") as file:
            print(f"Parsing {fname} into Tapis...")

            site_id = fname.split("_")[1] + "_test_00"
            instrument_id = fname.split("_")[0] + "_test_00"

            logger.debug(site_id)
            logger.debug(instrument_id)

            inst_data_file = file.readlines()

            list_vars = inst_data_file[1].decode("UTF-8").strip().replace("\"", "").split(",")
            logger.debug(list_vars)


            # Parsing the measurements for each variable
            variables = []
            for i in range(4, len(inst_data_file)):
                measurements = inst_data_file[i].decode("UTF-8").strip().replace("\"", "").split(",")
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
            logger.debug(variables)

            # Creating the Tapis measurements
            result = permitted_client.streams.create_measurement(inst_id=instrument_id, vars=variables)
            logger.debug(result)
