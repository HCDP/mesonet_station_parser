# mesonet_station_parser

This repository contains codes that parse mesonet station data (.dat) files into Tapis streams_api.

The repository contains 2 primary script files that achieve this.
- streams_processor.py
  - This file parses incoming station data that is stored in a directory(data_dir).
  - **There are variations of this file that speed up the parsing process by using Python's multiprocessing module**
- legacy.py
  - This file downloads past data by sending API requests to the ikewai gateway, thus allowing us to parse past data into Tapis streams-api.
  - You are able to provide a start_date and an end_date range for what files you want to parse.
 
The *standard_var* folder contains csv files that maps the raw shortnames of the stations' data to the standardized shortnames
- Note: the csv files are not extremely accurate as the raw shortnames for the stations' data changes occasionally.


## TODO List
- [ ] Ensure that standardize_variable function is working properly (still waiting for HCDP folks to decide on a more permanent variable naming convention)
- [ ] Move username, password, data_dir, (start_date and end_date for legacy.py) into a config file for easier editability
- [ ] Potentially add a field in the config file to enable parallelism and define number of workers
- [ ] For production use, remove iteration counter from project_id, site_id and inst_id
