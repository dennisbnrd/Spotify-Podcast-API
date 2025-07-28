import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def extract_podcast_data(output_folder="data"):
    """
    Extract all episodes and meta info from Spotify podcasts,
    and save as CSV in output_folder.
    """
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    AUTH_URL = "https://accounts.spotify.com/api/token"

    # Get Access Token
    auth_response = requests.post(AUTH_URL, {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    })
    auth_response_data = auth_response.json()
    access_token = auth_response_data['access_token']
    headers = {'Authorization': f'Bearer {access_token}'}

    # Podcast List
    podcast_list = [
        "podkesmas",
        "rintik sedu",
        "do you see what i see",
        "raditya dika",
        "makna talks"
    ]

    # Helper Functions
    def get_show_id(query, headers):
        SEARCH_URL = "https://api.spotify.com/v1/search"
        params = {"q": query, "type": "show", "limit": 1}
        res = requests.get(SEARCH_URL, headers=headers, params=params)
        res.raise_for_status()
        items = res.json()["shows"]["items"]
        if items:
            show = items[0]
            return show["id"], show["name"], show["publisher"]
        return None, None, None

    def get_episodes(show_id, headers):
        episode_list = []
        url = f"https://api.spotify.com/v1/shows/{show_id}/episodes"
        params = {"limit": 50, "offset": 0}
        while True:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
            items = data["items"]
            for ep in items:
                if ep is not None:
                    episode_list.append({
                        "episode_id": ep["id"],
                        "episode_name": ep["name"],
                        "release_date": ep["release_date"],
                        "duration_ms": ep["duration_ms"],
                        "description": ep["description"],
                        "external_url": ep["external_urls"]["spotify"],
                        "show_id": show_id
                    })
            if data.get("next"):
                params["offset"] += params["limit"]
            else:
                break
        return episode_list

    # Main Extract Loop
    all_episodes = []
    podcast_meta = []

    for podcast in podcast_list:
        show_id, show_name, publisher = get_show_id(podcast, headers)
        if show_id:
            print(f"Extracting podcast: {show_name} (Publisher: {publisher}) ...")
            eps = get_episodes(show_id, headers)
            for e in eps:
                e['show_name'] = show_name
                e['publisher'] = publisher
            all_episodes.extend(eps)
            podcast_meta.append({
                "show_id": show_id,
                "show_name": show_name,
                "publisher": publisher,
                "total_episodes": len(eps)
            })
            print(f"  {len(eps)} episodes fetched.")
        else:
            print(f"Podcast not found: {podcast}")

    # SAVE TO FOLDER
    os.makedirs(output_folder, exist_ok=True)
    episodes_path = os.path.join(output_folder, "all_podcast_episodes.csv")
    meta_path = os.path.join(output_folder, "podcast_meta.csv")

    df_episodes = pd.DataFrame(all_episodes)
    df_podcasts = pd.DataFrame(podcast_meta)

    print("\n=== Sample Episode Data ===")
    print(df_episodes.head())
    print("\n=== Podcast Meta Data ===")
    print(df_podcasts)

    df_episodes.to_csv(episodes_path, index=False)
    df_podcasts.to_csv(meta_path, index=False)

    print(f"\nData saved to folder: {output_folder}/ (Cek file hasilnya di folder ini)")
    return episodes_path, meta_path

