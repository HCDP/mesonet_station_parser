# mesonet_station_parser

This repository contains codes that parse mesonet station data (.dat) files into Tapis streams_api.

The repository contains 2 primary script files that achieve this.
- streams_processor.py
  - This file parses incoming station data that is stored in a directory(data_dir).
  - **There are variations of this file that speed up the parsing process by using Python's multiprocessing module**
- legacy.py
  - This file downloads past data by sending API requests to the ikewai gateway, thus allowing us to parse past data into Tapis streams-api.
  - You are able to provide a start_date and an end_date range for what files you want to parse.
