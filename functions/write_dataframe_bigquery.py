from google.cloud import bigquery
from functions.setup_logger import get_logger

def write_dataframe_to_bigquery(logger, client, dataframe, table, job_config):
    """Writes a DataFrame to BigQuery and waits for the job to complete."""
    try:
        job = client.load_table_from_dataframe(dataframe, table, job_config=job_config)
        job.result()  # Waits for the job to complete
        logger.info(f'Job ID: {job.job_id}')
        logger.info(f'Records inserted: {job.output_rows}')
    except Exception as e:
        logger.error(f'An error occurred during data insertion in BigQuery: {e}')