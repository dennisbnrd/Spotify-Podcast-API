import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
AUTH_URL = "https://accounts.spotify.com/api/token"


# =======================
# GET ACCESS TOKEN
# =======================

auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
})
auth_response_data = auth_response.json()
access_token = auth_response_data['access_token']
headers = {'Authorization': f'Bearer {access_token}'}

# =======================
# PODCAST LIST (BISA DITAMBAH)
# =======================
podcast_list = [
    "podkesmas",
    "rintik sedu",
    "do you see what i see",
    "raditya dika",
    "makna talks"
]

# =======================
# FUNGSI GET SHOW ID
# =======================
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

# =======================
# FUNGSI GET EPISODES
# =======================
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

# def safe_get(d, k, default=None):
#     return d[k] if k in d and d[k] is not None else default

# =======================
# MAIN LOOP – EXTRACT ALL EPISODES
# =======================
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
        podcast_meta.append({"show_id": show_id, "show_name": show_name, "publisher": publisher, "total_episodes": len(eps)})
        print(f"  {len(eps)} episodes fetched.")
    else:
        print(f"Podcast not found: {podcast}")

# =======================
# SAVE TO DATAFRAME
# =======================
df_episodes = pd.DataFrame(all_episodes)
df_podcasts = pd.DataFrame(podcast_meta)

print("\n=== Sample Episode Data ===")
print(df_episodes.head())
print("\n=== Podcast Meta Data ===")
print(df_podcasts)

# =======================
# OPTIONAL: SAVE TO CSV
# =======================
df_episodes.to_csv("all_podcast_episodes.csv", index=False)
df_podcasts.to_csv("podcast_meta.csv", index=False)

print("\nData saved to CSV! Selanjutnya bisa lanjut ke cleaning/transform/load ke BigQuery.")
