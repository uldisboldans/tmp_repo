import sqlite3
import pandas as pd
import os
import importlib
from datetime import datetime
from shutil import move

CONNECTION_CONFIG_FILE_NAME = "connection_config.xlsx"
CONNECTION_CONFIG_FILE_SHEET_NAME = "config"
DB_NAME = "connection_data.db" 
FILE_FOLDER = "file_processing" # Contains a folder structure for file processing [receive, data quality, upload to db]

# Get project variables
working_directory_path = os.path.dirname(os.path.realpath(__file__))
module_name = os.path.basename(__file__).split(".")[0] 
project_name = working_directory_path.split("\\")[-1]

# Create logger object for errors, warnings, info etc.
logging_config = importlib.import_module("logs.logging_config")
logger = logging_config.create_logger(name = module_name,log_file_name = module_name,project_directory_path=working_directory_path)

# Get database host
db_host = os.path.join(working_directory_path,DB_NAME)

# Get active connection list of all developed custom connections from APIs, Excel files and other data sources [for this project only Finland's average income by postal code]
config_file_path = os.path.join(working_directory_path,CONNECTION_CONFIG_FILE_NAME) 
connections = pd.read_excel(config_file_path,sheet_name=CONNECTION_CONFIG_FILE_SHEET_NAME,engine="openpyxl")
active_connections = connections[connections["active_connection"]==True]

# Process active connections step-by-step: 1) load its processing module, 2) request data from source, 3) save data to local file, 4) check data quality in the local file, 5) upload data to databse 
if len(active_connections) > 0:
  active_connection_list = active_connections["connectionId"].to_list()
  connection_names = active_connections["connection_name"].to_list()
  sql_table_configruation = active_connections["sql_config"].to_list()
  for connection_index, active_connectionId in enumerate(active_connection_list):
    # Check if the active connection has a module which gets and processes the source data
    connection_module_path = os.path.join(working_directory_path,"connections")
    connection_modules = os.listdir(connection_module_path)
    active_connection_module = f"{active_connectionId}.py"
    if active_connection_module in connection_modules:
      print(f"Started processing connectionId: {active_connectionId} | {connection_names[connection_index]}")
      # Import connection's python module
      try:
        connection_data_processor = importlib.import_module(f"connections.{active_connectionId}")
      except Exception as e:
        logger.error(f"Failed to import {active_connectionId} data processing module. Error: {e}")
        continue
      # Request data from the source and return it in a dataframe
      try:
        data_df = connection_data_processor.get_custom_data()
      except Exception as e:
        logger.error(f"Failed to get data from source. connectionId: {active_connectionId}. Error: {e}")
        continue

      # Create Excel file name in which dataframe will be written
      update_date = str(datetime.now()).split(" ")[0].replace("-","") 
      update_time = str(datetime.now()).split(" ")[1].split(".")[0].replace(":","")
      update_datetime = str(update_date) + "T"+ str(update_time)
      raw_file_name = f"{active_connectionId}_{update_datetime}.xlsx"
      # Write dataframe to Excel file
      raw_in_path = os.path.join(working_directory_path,FILE_FOLDER,"received_files")
      raw_file_path =os.path.join(raw_in_path,raw_file_name)
      data_df.to_excel(raw_file_path,index=False)
      # Check if data file was created successfully
      if raw_file_name not in os.listdir(raw_in_path): 
        logger.error(f"Failed to create a file '{raw_file_name}' for connectionId {active_connectionId}")
        continue
      
      # Perform data quality analysis
      standard_controls = importlib.import_module(f"data_quality.standard_controls")
      report = standard_controls.run_empty_field_report(raw_file_path) # The standard control module at the moment checks only for "empty" fields
      print(report) # Need to define some criteria that would change 'quality_issues_exists' flag, e.g. major problems with data integrity, completness, datatypes and formats etc. 
      quality_issues_exist = False # Acceptance criteria could change this variable to False. The file then would be moved to "bad files" for further investigation. The automatic decision can be also overriden by manually moving the file to "ready_for_db" folder, thus skipping/ignoring the data quality checks
      if quality_issues_exist == False:
        # No data quality issues detected. File is moved to folder from which data is uploaded to database
        ready_for_db_path = os.path.join(working_directory_path,FILE_FOLDER,"ready_for_db")
        move(raw_file_path,ready_for_db_path)
        # Get a table name based on connection's name in configuration file
        table_name = connection_names[connection_index]
        # Creates a table (if does not exist) in database in which to upload the data from Excel. Table's column creation is based on connection's configuration file
        connection_data_processor.create_table_in_db(conn = sqlite3.connect(db_host),table_name = table_name, columns = sql_table_configruation[connection_index])
        # Get a list of all files that are ready for upload to database
        all_files_ready_for_db = os.listdir(ready_for_db_path)
        for file in all_files_ready_for_db:
          # Process files that are associated with the current connection module
          if str(active_connectionId) in file: 
            file_to_load = os.path.join(ready_for_db_path,file)
            # Read data from Excel file into a dataframe
            data_df = pd.read_excel(file_to_load,engine="openpyxl")
            # Write dataframe to database table
            with sqlite3.connect(db_host) as conn:
              data_df.to_sql(table_name, con=conn, index = False, if_exists="append")
            # Move uploaded Excel file to "processed" folder
            processed_file_path = os.path.join(working_directory_path,FILE_FOLDER,"processed")
            move(file_to_load,processed_file_path)
            print(f"Success: imported data in database for connectionId: {active_connectionId} | {connection_names[connection_index]}")
      else: 
        bad_files_directory = os.path.join(working_directory_path,FILE_FOLDER,"bad_files")
        move(raw_file_path,bad_files_directory)        
    else:
      logger.error(f"Failed to get data: connectionId {active_connectionId} is missing {active_connection_module} file in {connection_module_path}")
else:
  logger.warning(f"Failed to get any active connections from {CONNECTION_CONFIG_FILE_NAME}")

