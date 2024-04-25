import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from datetime import datetime
from tabulate import tabulate
from google.cloud import bigquery
from configs import conf_big_query
from google.oauth2 import service_account
from functions.setup_logger import get_logger
from functions.write_dataframe_bigquery import write_dataframe_to_bigquery

import json
import requests
import pandas as pd

app_name = 'ingestion-spotify-bigquery'

# Configuraç
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

def get_access_token(client_id, client_secret):
    # Function to obtain Spotify access token
    url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "client_credentials",
        'client_id': client_id,
        'client_secret': client_secret,
    }
    response = requests.post(url, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print("Error obtaining access token from Spotify. Status code:", response.status_code)
        response.raise_for_status()
        return None

def search_spotify_podcasts(query, access_token):
    # Function to search podcasts on Spotify
    url = "https://api.spotify.com/v1/search"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "q": query,
        "type": "show",
        "limit": 50,
        "market": "BR"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        try:
            results = response.json()
            return results.get('shows', {}).get('items', [])
        except json.JSONDecodeError as e:
            print("Error decoding JSON response:", e)
            return None
    else:
        print("Error when searching on Spotify. Status code:", response.status_code)
        response.raise_for_status()

def create_podcast_table(results):
    """Creates a DataFrame with podcast information."""
    table_data = []

    for result in results:
        if result:
            podcast_info = {
                'Nome_Podcast': result.get('name'),
                'Descricao': result.get('description'),
                'Identificador_Unico': result.get('id'),
                'Total_Episodios': result.get('total_episodes')
            }
            table_data.append(podcast_info)

    podcast_df = pd.DataFrame(table_data)
    return podcast_df

def search_spotify_episodes(show_id, access_token):
    """Searches for podcast episodes on Spotify."""
    url = f"https://api.spotify.com/v1/shows/{show_id}/episodes"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "limit": 50
    }

    all_episodes = []

    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            try:
                results = response.json()
                episodes = results.get('items', [])
                all_episodes.extend(episodes)

                if results.get('next'):
                    url = results['next']
                else:
                    break
            except json.JSONDecodeError as e:
                print("Error decoding JSON response:", e)
                return None
        else:
            print("Error when searching for episodes on Spotify. Status code:", response.status_code)
            response.raise_for_status()

    episode_data = []

    for episode in all_episodes:
        episode_info = {
            'ID': episode['id'],
            'Nome': episode['name'],
            'Descricao': episode['description'],
            'Data_Lançamento': episode['release_date'],
            'Duracao_ms': episode['duration_ms'],
            'Idioma': episode['language'],
            'Conteudo_Explicito': 'Sim' if episode['explicit'] else 'Não',
            'Tipo_Faixa_Audio': episode['type']
        }
        episode_data.append(episode_info)

    episode_df = pd.DataFrame(episode_data)
    return episode_df
    
def filter_episodes_by_keyword(episodes_df, keyword):
    """Filters episodes based on keyword in description."""
    filtered_episodes = episodes_df[episodes_df['Descricao'].str.contains(keyword, case=False)]
    return filtered_episodes

client_id = "9be79e95797f4e819d3376d0331eb5b8"
client_secret = "be675fa7dcb445719f7b1d1461f1913e"

# Main function
def execute():
    logger.info(f"Starting program execution: {app_name}")

    # Authenticate for BigQuery
    try:
        path_to_bigquery_credentials = conf_big_query.credential_big_query
        bigquery_credentials = service_account.Credentials.from_service_account_info(conf_big_query.credential_big_query)
        logger.info("Connection to BigQuery successful.")
    except Exception as e:
        log_message_error(f"Error connecting to BigQuery: {e}")
        return

    # Job configuration
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    # Destination in BigQuery
    client_bigquery = bigquery.Client(credentials=bigquery_credentials)
    dataset_id_bigquery = 'dataset_bot'

    # Get access token for Spotify
    access_token = get_access_token(client_id, client_secret)
    if access_token:
        # Search podcasts on Spotify
        results = search_spotify_podcasts("Data Hackers", access_token)
        if results:
            # Show first 50 podcast results
            podcast_table = create_podcast_table(results)
            print("Top 50 podcast results:")
            print(podcast_table)
            # If there is at least one podcast found
            if len(results) > 0:
                # Get the ID of the first podcast (assuming there is only one)
                show_id = results[0]['id']
                # Search for episodes of this podcast
                episodes = search_spotify_episodes(show_id, access_token)
                print("Episodios completos:")
                print(episodes)
                if not episodes.empty:
                    # Filter episodes by keyword
                    keyword = "Grupo Boticário"
                    filtered_episodes = filter_episodes_by_keyword(episodes, keyword)
                    print("Episodios BOT:")
                    print(filtered_episodes)

                    if not filtered_episodes.empty:
                        # Write podcast table to BigQuery
                        write_dataframe_to_bigquery(logger, client_bigquery, podcast_table, f"{dataset_id_bigquery}.podcast_table", job_config)

                        # Write episode table to BigQuery
                        write_dataframe_to_bigquery(logger, client_bigquery, episodes, f"{dataset_id_bigquery}.episode_table", job_config)

                        # Write filtered episode table to BigQuery
                        write_dataframe_to_bigquery(logger, client_bigquery, filtered_episodes, f"{dataset_id_bigquery}.filtered_episode_table", job_config)
                    else:
                        logger.info("No episodes found with mention of Grupo Boticário.")
                else:
                    logger.info("No episodes found for the 'Data Hackers' podcast.")
            else:
                logger.info("No podcasts found for the search query.")
        else:
            logger.info("No results returned from Spotify.")
    else:
        logger.error("Failed to obtain access token from Spotify.")

    # End Application
    logger.info(f"End program execution was successful: {app_name}")


if __name__ == "__main__":
    execute()