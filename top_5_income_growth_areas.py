import sqlite3
import os

DB_NAME = "connection_data.db" 
BASE_YEAR = 2012
COMPARE_TO_YEAR = 2016
NUMBER_OF_TOP_RESULTS = 5

# Get project variables
working_directory_path = os.path.dirname(os.path.realpath(__file__))
module_name = os.path.basename(__file__).split(".")[0] 
project_name = working_directory_path.split("\\")[-1]
db_host = os.path.join(working_directory_path,DB_NAME)

# SQL query to select base year income and comparative year income and calculate relative change in it
# The query returns TOP 5 highest growth areas by postal code
query = f"""SELECT
base.postal_code,
base.area_label,
(((eur_{COMPARE_TO_YEAR} - base.value) * 1.0) / base.value) as income_growth_percent,
base.value as eur_{BASE_YEAR},
eur_{COMPARE_TO_YEAR}
FROM FI_avg_income base
    LEFT JOIN 
        (SELECT postal_code,
                value as eur_{COMPARE_TO_YEAR}
            FROM FI_avg_income
            WHERE report_year = {COMPARE_TO_YEAR}) compare
        ON base.postal_code = compare.postal_code
WHERE base.report_year = {BASE_YEAR}
AND (base.value IS NOT NULL and eur_{COMPARE_TO_YEAR} IS NOT NULL)
AND (base.value <> 0 AND eur_{COMPARE_TO_YEAR} <> 0)
ORDER BY income_growth_percent DESC
LIMIT {NUMBER_OF_TOP_RESULTS}
"""

with sqlite3.connect(db_host) as conn:
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    # Format and print results to console
    for index, line in enumerate(results):
        area_label = line[1]
        base_year_income = line[3]
        latest_income = line[4]
        growth = line[2]
        print(f"No {index+1} area with highest growth income is {area_label}: {round(growth*100,1)}% ({BASE_YEAR}: EUR {base_year_income} -> {COMPARE_TO_YEAR}: EUR {latest_income})")


    