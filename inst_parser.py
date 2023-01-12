import urllib
from datetime import date, timedelta, datetime
from dateutil.rrule import rrule, DAILY
import argparse
from tapipy.tapis import Tapis

# #TODO: add argument parser
# #      Something like python3 parser.py <API TOKEN> <InstrumentID>
# #      or             python3 <username> <password> <InstrumentID>

parser = argparse.ArgumentParser(
    prog="parser.py",
    description=""
)

parser.add_argument("instrument_id")
parser.add_argument("-u", "--username")
parser.add_argument("-p", "--password")
parser.add_argument("-t", "--token")

args = parser.parse_args()

#Set Tapis Tenant and Base URL
tenant="dev"
base_url = 'https://' + tenant + '.develop.tapis.io'

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
        #Generate an Access Token that will be used for all API calls
        permitted_client.get_tokens()
    except Exception as e:
        print("Error: ", e.message)
else:
    print("Error: Either --username and --password or --token must be provided.")

# #Load Python SDK
# from tapipy.tapis import Tapis

# #Create python Tapis client for user
# permitted_client = Tapis(base_url= base_url, 
#                          username=permitted_username,
#                          password=permitted_user_password, 
#                          account_type='user', 
#                          tenant_id=tenant
#                         ) 

# start_date = date(2022, 8, 1)
# end_date = datetime.today().date()

# for dt in rrule(DAILY, dtstart=start_date, until=end_date):
#     curr_date = dt.date()

#     # Add a mapping of instrumentID to filename
#     # Add a mapping of instrumentID to a common name

#     files = [
#         "0115_Piiholo_MetData.dat",
#         "0116_Keokea_MetData.dat",
#         "0119_KulaAg_MetData.dat",
#         "0143_Nakula_MetData.dat",
#         "0151_ParkHQ_MetData.dat",
#         "0152_NeneNest_MetData.dat",
#         "0153_Summit_MetData.dat",
#         "0281_IPIF_MetData.dat",
#         "0282_Spencer_MetData.dat",
#         "0283_Laupahoehoe_MetData.dat",
#         "0286_Palamanui_MetData.dat",
#         "0287_Mamalahoa_MetData.dat",
#         "0501_Lyon_MetData_5min.dat",
#         "0502_NuuanuRes1_MetData.dat",
#         "0601_Waipa_MetData.dat",
#         "0602_CommonGround_MetData.dat"
#     ]
#     base_url = "https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/raw/"

#     year = curr_date.year
#     month = curr_date.month
#     day = curr_date.day

#     for file in files:
#         link = f"{base_url}{year}/{month}/{day}/{file}"

#     data_file = urllib.request.urlopen(link).readlines()

#     #TODO: error check if data_file exists

#     list_vars = data_file[1].decode("UTF-8").strip().replace("\"", "").split(",")

#     list_units = data_file[2].decode("UTF-8").strip().replace("\"", "").split(",")

#     variables = []

#     for i in range(4, len(data_file)):
#         measurements = data_file[i].decode("UTF-8").strip().replace("\"", "").split(",")
#         measurement = {}
#         time = measurements[0].split(" ")

#         if(int(time[1].split(":")[0]) > 23):
#             time_string = time[0] + " 23:59:59"
#             time_string = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')
#             time_string += timedelta(seconds=1)
#         else:
#             time_string = datetime.strptime(measurements[0], '%Y-%m-%d %H:%M:%S')

#         measurement['datetime'] = time_string.isoformat()+"-10:00"
#         for j in range(2, len(measurements)):
#             measurement[list_vars[j]] = float(measurements[j])
#         variables.append(measurement)

#     result = permitted_client.streams.create_measurement(inst_id=instrument_id,
#                                                         vars=variables)

#     ###TODO: error check on result