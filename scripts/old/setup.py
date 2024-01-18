import time
import sys
from tapipy.tapis import Tapis
import pandas as pd

def create_site(project_id: str, site_id: str, site_name: str, latitude, longitude, elevation) -> bool:
    try:
        permitted_client.streams.create_site(project_id=project_id,
                                                    request_body=[{
                                                        "site_name": site_id,
                                                        "site_id": site_id,
                                                        "latitude": latitude, 
                                                        "longitude": longitude,
                                                        "elevation": elevation,
                                                        "description": site_name,
                                                        "metadata": {"station_name": site_name}}])
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

# MAIN FUNCTION
if __name__ == "__main__":
    permitted_username = "testuser2"
    permitted_user_password = "testuser2"

    start_time = time.time()

    # Set Tapis Tenant and Base URL
    tenant = "dev"
    base_url = 'https://' + tenant + '.develop.tapis.io'

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
        print(f"Error: {msg}")
        sys.exit(-1)

    project_id = "test-p2"

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
            print(f"Error: {msg}")
            sys.exit(-1)

    master_url = "https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/hi_mesonet_sta_status.csv"
    dtype_dict = {'sta_ID': str}

    df = pd.read_csv(master_url, dtype=dtype_dict)

    # Check if station exist in csv
    for index, row in df.iterrows():
        station_id = row['sta_ID']
        site_id = row['staName'].split("_")[0]
        site_name = row['staName'].split("_")[1]
        latitude = row['LAT']
        longitude = row['LON']
        elevation = row["ELEV"]

        print(site_id)

        result = create_site(project_id, site_id, site_name, latitude, longitude, elevation)
        if result != None:
            print(f"Error for {site_id}: {result}")
