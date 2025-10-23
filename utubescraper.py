import os
import time
import json
import itertools
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ==========================================================
# CONFIGURATION
# ==========================================================
API_KEYS = [
    "AIza*********************************-8",  # Project 1 key
    "AIza*********************************ko",  # Project 2 key
    "AIza*********************************g8",
    "AIza*********************************Pk",
    "AIza*********************************E",
    "AIza*********************************z0",  # Project 3 key
]

SEARCH_QUERIES = [
    "tech influencer",
    "fashion influencer",
    "fitness influencer",
    "travel influencer",
    "education influencer",
    "gaming influencer",
    "music creator",
    "beauty vlogger",
    "food blogger",
    "finance creator",
    "lifestyle influencer",
    "parenting influencer",
    "motivational speaker",
    "DIY influencer",
    "photography vlogger",
]

OUTPUT_FILE = "youtube_influencers_partial.csv"
MAX_INFLUENCERS = 5000
SAVE_EVERY = 100  # save progress every N channels
MAX_RESULTS_PER_QUERY = 50

# ==========================================================
# API SETUP
# ==========================================================
api_cycle = itertools.cycle(API_KEYS)
current_key = next(api_cycle)


def create_youtube_client(api_key):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


youtube = create_youtube_client(current_key)
print(f" Using API key: {current_key}")


def switch_api_key():
    global youtube, current_key
    current_key = next(api_cycle)
    print(f" Switching API key to: {current_key}")
    youtube = create_youtube_client(current_key)


# ==========================================================
# RESUME SUPPORT
# ==========================================================
if os.path.exists(OUTPUT_FILE):
    df_existing = pd.read_csv(OUTPUT_FILE)
    all_data = df_existing.to_dict("records")
    collected_ids = set(df_existing["channelId"].astype(str))
    print(f" Resuming from {len(df_existing)} saved channels.")
else:
    print(" No previous data found. Starting fresh.")
    all_data = []
    collected_ids = set()

# ==========================================================
# MAIN SCRAPER LOGIC
# ==========================================================
for query in SEARCH_QUERIES:
    print(f"\n Searching for: {query}")
    next_page_token = None

    while len(all_data) < MAX_INFLUENCERS:
        try:
            res = (
                youtube.search()
                .list(
                    part="snippet",
                    type="channel",
                    q=query,
                    maxResults=MAX_RESULTS_PER_QUERY,
                    pageToken=next_page_token,
                )
                .execute()
            )

            channels = res.get("items", [])
            channel_ids = [c["id"]["channelId"] for c in channels]

            # Get stats
            stats = (
                youtube.channels()
                .list(part="snippet,statistics", id=",".join(channel_ids))
                .execute()
            )

            for ch in stats.get("items", []):
                ch_id = ch["id"]
                if ch_id in collected_ids:
                    continue

                data = {
                    "channelId": ch_id,
                    "title": ch["snippet"]["title"],
                    "description": ch["snippet"].get("description", ""),
                    "publishedAt": ch["snippet"].get("publishedAt", ""),
                    "subscriberCount": int(ch["statistics"].get("subscriberCount", 0)),
                    "viewCount": int(ch["statistics"].get("viewCount", 0)),
                    "videoCount": int(ch["statistics"].get("videoCount", 0)),
                    "query": query,
                }

                if data["subscriberCount"] >= 5000:  # filter min 5K subs
                    all_data.append(data)
                    collected_ids.add(ch_id)

                if len(all_data) % SAVE_EVERY == 0:
                    pd.DataFrame(all_data).to_csv(OUTPUT_FILE, index=False)
                    print(f" Saved {len(all_data)} influencers so far...")

            next_page_token = res.get("nextPageToken")
            if not next_page_token:
                break

        except HttpError as e:
            if "quotaExceeded" in str(e):
                print(" Quota exceeded for this key. Switching API key...")
                switch_api_key()
                continue
            elif "userRateLimitExceeded" in str(e):
                print(" Temporary rate limit hit. Waiting 10s...")
                time.sleep(10)
                continue
            else:
                print(f" API error: {e}")
                time.sleep(5)
                continue

        if len(all_data) >= MAX_INFLUENCERS:
            break

# ==========================================================
# FINAL SAVE
# ==========================================================
pd.DataFrame(all_data).to_csv(OUTPUT_FILE, index=False)
print(f"\n Finished! Collected {len(all_data)} influencer channels.")
print(f" Data saved to {OUTPUT_FILE}")

