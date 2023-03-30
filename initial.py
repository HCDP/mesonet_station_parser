import urllib
from datetime import date, timedelta, datetime
from dateutil.rrule import rrule, DAILY
import argparse
from tapipy.tapis import Tapis
import logging
from logging import FileHandler

version = "03"
create_project = True

# Add argument parser
parser = argparse.ArgumentParser(
    prog="all_inst_parser.py",
    description=""
)

parser.add_argument("-d", "--debug", action="store_true", help="turn on debug mode")
parser.add_argument("-v", "--verbose", action="store_true", help="turn on verbose mode")

args = parser.parse_args()

#Set Tapis Tenant and Base URL
tenant="dev"
base_url = 'https://' + tenant + '.develop.tapis.io'

if(args.debug):
    level=logging.DEBUG
    print("---debug mode on---")
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
        
# #Create python Tapis client for user
permitted_client = Tapis(base_url= base_url, 
                        username=permitted_username,
                        password=permitted_user_password, 
                        account_type='user', 
                        tenant_id=tenant
                        )

#Generate an Access Token that will be used for all API calls
permitted_client.get_tokens()

project_id = 'Mesonet_test_' + version

if create_project:

    result, debug = permitted_client.streams.create_project(project_name=project_id,
                                            description='TEST project for MesoNet',
                                            owner='testuser2', pi='wongy', 
                                            funding_resource='test', 
                                            project_url='https://www.hawaii.edu/climate-data-portal/',
                                            active=True,_tapis_debug=True)
    logger.debug(result)
    logger.debug(debug)

inst_to_file = {
    '0115': '0115_Piiholo_MetData.dat'
    # '0116': '0116_Keokea_MetData.dat',
    # '0119': '0119_KulaAg_MetData.dat',
    # '0143': '0143_Nakula_MetData.dat',
    # '0151': '0151_ParkHQ_MetData.dat',
    # '0152': '0152_NeneNest_MetData.dat',
    # '0153': '0153_Summit_MetData.dat',
    # '0281': '0281_IPIF_MetData.dat',
    # '0282': '0282_Spencer_MetData.dat',
    # '0283': '0283_Laupahoehoe_MetData.dat',
    # '0286': '0286_Palamanui_MetData.dat',
    # '0287': '0287_Mamalahoa_MetData.dat',
    # '0501': '0501_Lyon_MetData_5min.dat',
    # '0502': '0502_NuuanuRes1_MetData.dat',
    # '0601': '0601_Waipa_MetData.dat',
    # '0602': '0602_CommonGround_MetData.dat'
}

# Creating cache file to store instrument_id
cache_file = open("inst_cache.txt", "a+")

# Creating sites and instruments
for file in inst_to_file:
    site_id = inst_to_file[file].split("_")[1] + "_test_" + version
    instrument_id = file + "_test_" + version
    logger.debug(instrument_id)
    logger.debug(site_id)

    created_vars = False

    # check if inst_id is in cached file
    cache_ids = cache_file.readlines()

    cache_ids = [id.rstrip() for id in cache_ids]

    if file not in cache_ids:

        # Creating the Tapis Site
        result, debug = permitted_client.streams.create_site(project_id=project_id,
                                                    request_body=[{
                                                    "site_name":site_id, 
                                                    "site_id":site_id,
                                                    "latitude":50, 
                                                    "longitude":10, 
                                                    "elevation":2,
                                                    "description":'test_site'
                                                    }], _tapis_debug=True)
        logger.debug(result)
        # logger.debug(debug)

        # Creating the Tapis Instrument
        result, debug = permitted_client.streams.create_instrument(project_id=project_id,
                                                           site_id=site_id,
                                                           request_body=[{
                                                            "inst_name":instrument_id,
                                                            "inst_description": instrument_id+"_"+site_id,
                                                            "inst_id":instrument_id
                                                           }], _tapis_debug=True)
        logger.debug(result)


        ### TODO: Add instrument_id to a cache file?
        cache_file.write(f"{file}\n")

    # Begin loading in measurements and variables
    start_date = date(2023, 1, 23) # change to date of first raw data
    end_date = datetime.today().date()

    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        curr_date = dt.date()

        ### TODO: get request to end point: "https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/raw/list/<iso formatted date>"

        base_url = "https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/raw/"

        year = curr_date.year
        month = str(curr_date.month).zfill(2)
        day = str(curr_date.day).zfill(2)

        logger.info(f"Attempting to parse {file} for {month}/{day}/{year} into Tapis...")

        link = f"{base_url}{year}/{month}/{day}/{inst_to_file[file]}"
        logging.debug("URL LINK: " + link)

        try:
            data_file = urllib.request.urlopen(link).readlines()

            list_vars = data_file[1].decode("UTF-8").strip().replace("\"", "").split(",")
            logger.debug(list_vars)

            # Creating the Tapis Variables
            if not created_vars:
                logger.debug(f"---IN CREATING TAPIS VARIABLES FOR {instrument_id}_{site_id}---")
                created_vars = True

                list_units = data_file[2].decode("UTF-8").strip().replace("\"", "").split(",")
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
                                                        inst_id=instrument_id,
                                                        request_body=request_body,_tapis_debug=True)
                logger.debug(result)
                # logger.debug(debug)
                logger.debug(f"---END OF CREATING VARIABLES FOR {instrument_id}_{site_id}---")

            # Parsing the measurements for each variable
            logger.debug(f"---IN CREATING MEASUREMENT FOR {instrument_id}_{site_id}---")
            variables = []
            for i in range(4, len(data_file)):
                measurements = data_file[i].decode("UTF-8").strip().replace("\"", "").split(",")
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
            logger.debug(variables[0])
            logger.debug(f"---END OF CREATING MEASUREMENT FOR {instrument_id}_{site_id}---")

            # Creating the Tapis measurements
            result = permitted_client.streams.create_measurement(inst_id=instrument_id, vars=variables)
            # logger.debug(result)
        except Exception as e:
            logger.error("Error: ", e)
            logger.error("File probably doesn't exist, Continuing...")


cache_file.close()
