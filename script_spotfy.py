import requests

def get_access_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic <base64 encoded(client_id:client_secret)>",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print("Erro ao obter token de acesso do Spotify. Código de status:", response.status_code)
        return None

def search_spotify_podcasts(query, access_token):
    url = "https://api.spotify.com/v1/search"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "q": query,
        "type": "show",
        "limit": 50
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        results = response.json()
        return results['shows']['items']
    else:
        print("Erro ao pesquisar no Spotify. Código de status:", response.status_code)
        return None

# Exemplo de uso:
client_id = "9be79e95797f4e819d3376d0331eb5b8"
client_secret = "be675fa7dcb445719f7b1d1461f1913e"

access_token = get_access_token(client_id, client_secret)
if access_token:
    results = search_spotify_podcasts("data hackers", access_token)
    if results:
        for result in results:
            print(result['name'])