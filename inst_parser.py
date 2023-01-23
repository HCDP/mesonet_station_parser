### TODO: CHANGE SCRIPT TO CREATE SITE AND INSTRUMENT BEFORE LOOPING THROUGH DATES

import urllib
from datetime import date, timedelta, datetime
from dateutil.rrule import rrule, DAILY
import argparse
from tapipy.tapis import Tapis
import logging
from logging import FileHandler



# Argument parser
parser = argparse.ArgumentParser(
    prog="parser.py",
    description=""
)

parser.add_argument("instrument_id")
parser.add_argument("-u", "--username")
parser.add_argument("-p", "--password")
parser.add_argument("-t", "--token")
parser.add_argument("-f", "--file")
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
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    handlers=[file_handler])

if args.username and args.password:
    permitted_username = args.username
    permitted_user_password = args.password

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
elif args.token:
    #Create python Tapis client for user
    try:
        permitted_client = Tapis(base_url= base_url, 
                                jwt=args.token, 
                                account_type='user', 
                                tenant_id=tenant
                                )
    except Exception as e:
        print("Error: ", e.message)
else:
    print("Error: Either --username and --password or --token must be provided.")

project_id = 'Mesonet' + str(datetime.today().isoformat())

result, debug = permitted_client.streams.create_project(project_name=project_id,
                                        description='TEST project for MesoNet',
                                        owner='testuser2', pi='wongy', 
                                        funding_resource='test', 
                                        project_url='https://www.hawaii.edu/climate-data-portal/',
                                        active=True,_tapis_debug=True)
logger.debug(result)
# logger.debug(debug)

start_date = date(2023, 1, 16)
end_date = datetime.today().date()

for dt in rrule(DAILY, dtstart=start_date, until=end_date):
    curr_date = dt.date()

#     # Add a mapping of instrumentID to filename
#     # Maybe an array of files?
    inst_to_file = {
        '0115': '0115_Piiholo_MetData.dat',
        '0116': '0116_Keokea_MetData.dat',
        '0119': '0119_KulaAg_MetData.dat',
        '0143': '0143_Nakula_MetData.dat',
        '0151': '0151_ParkHQ_MetData.dat',
        '0152': '0152_NeneNest_MetData.dat',
        '0153': '0153_Summit_MetData.dat',
        '0281': '0281_IPIF_MetData.dat',
        '0282': '0282_Spencer_MetData.dat',
        '0283': '0283_Laupahoehoe_MetData.dat',
        '0286': '0286_Palamanui_MetData.dat',
        '0287': '0287_Mamalahoa_MetData.dat',
        '0501': '0501_Lyon_MetData_5min.dat',
        '0502': '0502_NuuanuRes1_MetData.dat',
        '0601': '0601_Waipa_MetData.dat',
        '0602': '0602_CommonGround_MetData.dat'}

#     # Add a mapping of instrumentID to a common name

#     inst_to_name = {
#         '0115': '',
#         '0116': '',
#         '0119': '',
#         '0143': '',
#         '0151': '',
#         '0152': '',
#         '0153': '',
#         '0281': '',
#         '0282': '',
#         '0283': '',
#         '0286': '',
#         '0287': '',
#         '0501': '',
#         '0502': '',
#         '0601': '',
#         '0602': ''
#     }

    ### TODO: if file flag is provided, grab file locally
    base_url = "https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/raw/"

    year = curr_date.year
    month = str(curr_date.month).zfill(2)
    day = str(curr_date.day).zfill(2)

    # for file in files:
    ### TODO: try except
    with inst_to_file[str(arg.instrument_id)] as file:
        print(f"Parsing {file} into Tapis...")

        site_id = file.split("_")[1] + str(datetime.today().isoformat()).replace(".", "-").replace(":", "-")
        instrument_id = file.split("_")[0] + str(datetime.today().isoformat()).replace(".", "-").replace(":", "-")

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
        # logger.debug(debug)

        link = f"{base_url}{year}/{month}/{day}/{file}"

        # print(link)

        try:
            data_file = urllib.request.urlopen(link).readlines()

            list_vars = data_file[1].decode("UTF-8").strip().replace("\"", "").split(",")
            list_units = data_file[2].decode("UTF-8").strip().replace("\"", "").split(",")

            logger.debug(list_vars)
            logger.debug(list_units)

            # Creating the Tapis Variables
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

            # Parsing the measurements for each variable
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
            logger.debug(variables)

            # Creating the Tapis measurements
            result = permitted_client.streams.create_measurement(inst_id=instrument_id, vars=variables)
            logger.debug(result)
        except Exception as e:
            print("Error: ", e)
