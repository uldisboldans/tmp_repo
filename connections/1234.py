import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime
import importlib
import sys

AVG_INCOME_CODE = "Hr_ktu" # data code used in the API to get average income

# get project variables
working_directory_path = os.path.dirname(os.path.realpath(__file__))
module_name = os.path.basename(__file__).split(".")[0] 
tmp_path = working_directory_path.split("tmp_repo")[0]
project_directory = os.path.join(tmp_path,"tmp_repo")

# log file setup for errors, warnings, info etc.
sys.path.insert(1, project_directory) #adding a python path to runtime so that the other py scripts at the same folder level can be imported | insert at 1, 0 is the script path
logging_config = importlib.import_module("logs.logging_config")
logger = logging_config.create_logger(name = module_name,log_file_name = module_name,project_directory_path=project_directory)

def request_statistic_data(url,query_json):
  """Post a request to API and return a JSON with data.
  API requests based on https://pxnet2.stat.fi/api1.html documentation
  """
  try:
      response = requests.post(url,json=query_json)
      if response.status_code == 200:
          response = response.json()
      else: 
          logger.error(f"Received '{response.status_code}', Expected a '200' HTTP response code")
  except Exception as e:
      logger.error(f"Failed to get a valid response from the API. Error: {e}")
  else:
    return response

# get the table name "report_statistic_name" returned by the API and year for which the data was reported "report_year"
def get_statistic_name(url):
  """Send an API request and get the NAME of the statistic"""
  response = requests.get(url)
  query_params = response.json()["variables"][1]
  index = query_params["values"].index(AVG_INCOME_CODE)
  api_table_name = query_params["valueTexts"][index]
  report_statistic_name = api_table_name.split(",")[0] # Trim the "YEAR (HR)" from the API table's name
  return report_statistic_name

def get_report_year(url,default_year=1900):
  """Send an API request and get YEAR for which the statistic was reported"""
  response = requests.get(url)
  query_params = response.json()["variables"][1]
  index = query_params["values"].index(AVG_INCOME_CODE)
  api_table_name = query_params["valueTexts"][index]
  year_in_field_name = [int(s) for s in api_table_name.split() if s.isdigit()]
  # If statistic title contains more than one digit, replace the year with default_year
  if len(year_in_field_name) > 1:
    logger.warning(f"Replaced year with a default value. Expected only one integer in the API table title, got {year_in_field_name}")
    report_year = default_year 
  else:
    report_year = year_in_field_name[0]
  return report_year


def extract_statistic_values(json):
  """Extracts from JSON structure 3 lists: postal_code list, area_label list and average income list

  postal_code: list containing postal codes (also known as zip code)
  area_label:  lsit of area names associated with a particular postal code
  avg_income: list containging average income in EUR
  """
  # Access area and income values in JSON
  areas_json = json["dimension"]["Postinumeroalue"]["category"]["label"] #index and label
  avg_income = json["value"]
  # Store area values in two lists -> postal codes and their associated labels
  postal_codes = []
  area_labels = []
  for key,value in areas_json.items():
    if key in value:
      value = value.split(key)[1].strip() #remove postal code from area name
    area_labels.append(value)
    postal_codes.append(key)
  return postal_codes, area_labels, avg_income

def create_table_in_db(conn,table_name,columns):
  """Creates a table in database. Returns if the creation operation was successful or not."""
  query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({columns});"""
  success_flag = False
  try:
    with conn:
      cursor = conn.cursor()
      cursor.execute(query)
      success_flag = True
  except Exception as e:
    logger.error(f"Failed to create a table {table_name}. Error: {e}")
  return success_flag

def get_custom_data():
  """Executes a custom data processing flow for initial data load. Returns a dataframe."""
  data_update_years = [2015,2016,2017,2018,2019] # Data range currently accepted by the API for "Average income of inhabitants" statistic
  # Create lists for each variable that is expected in the connection's database table. Based on connection's configuration file.
  updated_at = [] 
  postal_codes =[]
  area_labels = []
  avg_income = []
  report_statistic_names = []
  report_years = []
  variable_name = []
  # Request data for each year from the API
  for year in data_update_years:
    url = f"https://pxnet2.stat.fi/PXWeb/api/v1/en/Postinumeroalueittainen_avoin_tieto/{year}/paavo_3_hr_{year}.px"
    query = {
      "query": [
        {
          "code": "Tiedot",
          "selection": {
            "filter": "item",
            "values": [
              AVG_INCOME_CODE
            ]
          }
        }
      ],
      "response": {
        "format": "json-stat2"
      }
    }
    # Get data from API into a JSON structure
    data_json = request_statistic_data(url = url,query_json=query)
    # Extract list of values from JSON
    tmp_postal_codes, tmp_area_labels, tmp_avg_income = extract_statistic_values(json = data_json)
    data_array_lenght = len(tmp_postal_codes)
    tmp_updated_at = [datetime.now()] * data_array_lenght # Create a list of update timestamp values
    report_statistic_name = [get_statistic_name(url)] * data_array_lenght # Create a list of statistic name values
    report_year = [get_report_year(url)] * data_array_lenght # Create a list of report year values for which the data was returned 
    tmp_variable_name = ["EUR"] * data_array_lenght
    # Insert in lists the latest returned data values from API to already processed values
    updated_at.extend(tmp_updated_at)
    postal_codes.extend(tmp_postal_codes)
    area_labels.extend(tmp_area_labels)
    avg_income.extend(tmp_avg_income)
    report_statistic_names.extend(report_statistic_name)
    report_years.extend(report_year)
    variable_name.extend(tmp_variable_name)
  # Create a dataframe from 
  data_array = np.array([updated_at,report_statistic_names,report_years,postal_codes,area_labels,variable_name,avg_income])
  data_array = np.transpose(data_array) # Created lists have a horizontal orientation, transpose to vertical
  data_df = pd.DataFrame(data_array)
  data_df.columns =["updated_at","statistic_name","report_year","postal_code", "area_label","variable_name","value"]
  return data_df

  












