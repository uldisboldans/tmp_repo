import pandas as pd

def run_empty_field_report(file,file_type="excel"):
    """Returns a report of fields affected by empty values in the file.

    Arguments:
    file_type -- excel, text, json
    """
    if file_type == "excel": # Currently only Excel is accepted
        df = pd.read_excel(file,engine="openpyxl")
        df_lenght = len(df)
        df_columns = list(df.columns.values)
        report = []
        for column_name in df_columns:
            empty_field_count = sum(df[column_name].isna()) # Count all empty fields in the column
            if empty_field_count != 0:
                report.append(f"{empty_field_count} '{column_name}' fields out of {df_lenght} are EMPTY") 
    return report
