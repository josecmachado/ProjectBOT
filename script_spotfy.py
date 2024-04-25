import json
import requests
import pandas as pd

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

def filter_episodes_by_keyword(episodes_df, keyword):
    # Filter episodes based on keyword in description
    filtered_episodes = episodes_df[episodes_df['Descrição'].str.contains(keyword, case=False)]
    return filtered_episodes

def create_podcast_table(results):
    # Function to create a table with the specified fields
    table_data = {
        'Nome do Podcast': [],
        'Descrição': [],
        'Identificador Único': [],
        'Total de Episódios': []
    }

    for result in results:
        if result:
            table_data['Nome do Podcast'].append(result.get('name'))
            table_data['Descrição'].append(result.get('description'))
            table_data['Identificador Único'].append(result.get('id'))
            table_data['Total de Episódios'].append(result.get('total_episodes', 0))

    podcast_df = pd.DataFrame(table_data)
    return podcast_df

def search_spotify_episodes(show_id, access_token):
    # Function to search for podcast episodes on Spotify
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

                # Check if there are more episodes to fetch
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

    # Create a list to hold episode data
    episode_data = []

    for episode in all_episodes:
        episode_info = {
            'ID': episode['id'],
            'Nome': episode['name'],
            'Descrição': episode['description'],
            'Data de Lançamento': episode['release_date'],
            'Duração (ms)': episode['duration_ms'],
            'Idioma': episode['language'],
            'Conteúdo Explícito': 'Sim' if episode['explicit'] else 'Não',
            'Tipo de Faixa de Áudio': episode['type']
        }
        episode_data.append(episode_info)

    # Create a DataFrame from the list of episode data
    episode_df = pd.DataFrame(episode_data)
    
    return episode_df


client_id = "9be79e95797f4e819d3376d0331eb5b8"
client_secret = "be675fa7dcb445719f7b1d1461f1913e"

access_token = get_access_token(client_id, client_secret)
if access_token:
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
            if not episodes.empty:
                # Create DataFrame for episodes
                episode_df = search_spotify_episodes(show_id, access_token)

                # Print the DataFrame
                print("\n[TABLE 6] - Episodes of the 'Data Hackers' podcast:")
                print(episode_df)

                # Create DataFrame for episodes
                episode_df = search_spotify_episodes(show_id, access_token)

                # Filter episodes by keyword
                keyword = "Grupo Boticário"
                filtered_episodes = filter_episodes_by_keyword(episode_df, keyword)

                if not filtered_episodes.empty:
                    # Print the DataFrame with filtered episodes
                    print("\n[TABLE 7] - Episodes of the 'Data Hackers' podcast with mention of Grupo Boticário:")
                    print(filtered_episodes)
                else:
                    print("No episodes found with mention of Grupo Boticário.")