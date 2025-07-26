import pandas as pd
import os

def transform_podcast_data(input_csv, output_csv):
    """
    Transformasi dan cleaning data podcast ke format siap pakai.
    """
    # 1. LOAD DATA
    df = pd.read_csv(input_csv)

    # 2. TRANSFORM & CLEANING
    if 'duration_ms' in df.columns:
        df['duration_min'] = df['duration_ms'] / 60000

    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        df['year'] = df['release_date'].dt.year
        df['month'] = df['release_date'].dt.month

    if 'episode_name' in df.columns:
        df['title_word_count'] = df['episode_name'].astype(str).apply(lambda x: len(x.split()))

    if 'description' in df.columns:
        df['desc_length'] = df['description'].astype(str).apply(len)

    # 3. SAVE RESULT
    df.to_csv(output_csv, index=False)
    print(f"Transformasi selesai! Data hasil cleaning sudah disimpan ke {output_csv}")
    print(df.head())
    return df

# =====================
# IMPLEMENTASI LANGSUNG
# =====================
output_folder = "data"
os.makedirs(output_folder, exist_ok=True)

input_csv = os.path.join(output_folder, "all_podcast_episodes.csv")
output_csv = os.path.join(output_folder, "podcast_episodes_clean.csv")

transform_podcast_data(input_csv, output_csv)