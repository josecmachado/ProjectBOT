import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pandas as pd
import os
from configs import conf_big_query
from functions.create_table_bigquery import create_table_if_not_exists
from functions import read_and_concat_excel
from functions.write_dataframe_bigquery import write_dataframe_to_bigquery
from functions.add_metadata import add_metadata
from functions.setup_logger import get_logger

app_name = 'ingestion-excel-bigquery'

# Configure the global logger
log_folder = 'logs'
os.makedirs(log_folder, exist_ok=True)

# Include the program name and date in the log file
log_filename = f"{app_name}_{datetime.now().strftime('%Y%m%d')}.log"
log_file_path = os.path.join(log_folder, log_filename)

#Log config
logger = get_logger(f"{app_name}", level='INFO', log_file=log_file_path)

def log_message_error(message):
    # Helper function to log error messages
    logger.error(message)

# Main function
def execute():
    logger.info(f"Starting program execution: {app_name}")

    # Connect to the data source
    try:
        folder_source = 'C:/Users/c10447q/Projetcts/ProjectBOT/bases'
        logger.info("Connection to the data source successful.")
    except Exception as e:
        log_message_error(f"Error connecting to the data source: {e}")
        return

    # Get data
    try:
        # Call the function that performs the concatenation of Excel files."
        df_source = read_and_concat_excel.read_and_concat_excel(folder_source)
        logger.info("Data extraction successful.")
    except Exception as e:
        log_message_error(f"Error during data extraction: {e}")
        return

    # Call the function that adds metadata to the dataframe.
    add_metadata(df_source, source="excel", tool="python")

    #Call the function that generates the schema of the table to be created.
    #generate_bigquery_schema(df_source)

    #Schema
    table_schema = [
        bigquery.SchemaField('ID_MARCA', 'INTEGER'),
        bigquery.SchemaField('MARCA', 'STRING'),
        bigquery.SchemaField('ID_LINHA', 'INTEGER'),
        bigquery.SchemaField('LINHA', 'STRING'),
        bigquery.SchemaField('DATA_VENDA', 'DATE'),
        bigquery.SchemaField('QTD_VENDA', 'INTEGER')
    ]

    # Authenticate for BigQuery
    try:
        path_to_bigquery_credentials = conf_big_query.credential_big_query
        bigquery_credentials = service_account.Credentials.from_service_account_info(conf_big_query.credential_big_query)
        logger.info("Connection to BigQuery successful.")
    except Exception as e:
        log_message_error(f"Error connecting to BigQuery: {e}")
        return

    # Destination in BigQuery
    client_bigquery = bigquery.Client(credentials=bigquery_credentials)
    dataset_id_bigquery = 'dataset_bot'
    table_id_bigquery = 'bases-BOT'

    # Create table in BigQuery
    table_bigquery = create_table_if_not_exists(logger, client_bigquery, dataset_id_bigquery, table_id_bigquery, schema=table_schema)

    # Job configuration
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    # Load data into BigQuery
    write_dataframe_to_bigquery(logger, client_bigquery, df_source, table_bigquery, job_config)

    #End Application
    logger.info(f"End program execution was successful: {app_name}")

if __name__ == "__main__":
    execute()