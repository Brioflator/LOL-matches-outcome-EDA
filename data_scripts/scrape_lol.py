import requests
import pandas as pd
import time
import os
import datetime

# --- CONFIGURATION ---
API_KEY = ''  # <--- PASTE YOUR API KEY HERE
TARGET_MATCHES = 30000           # 30,000 matches * 10 players = 300,000 datapoints
CSV_FILE = 'league_dataset_diamond_emerald.csv'
START_TIME = time.time()

# Region configuration ordered specifically as requested
ORDERED_REGIONS = [
    {'id': 'NA1',  'route': 'americas'},
    {'id': 'EUN1', 'route': 'europe'},
    {'id': 'EUW1', 'route': 'europe'}
]

# Order: Collect Diamond first for all regions, then Emerald
TIERS = ['DIAMOND', 'EMERALD'] 
DIVISIONS = ['I', 'II', 'III', 'IV']

# --- RATE LIMIT HANDLER ---
def request_riot(url):
    """
    Wrapper to handle Riot API Rate Limits (429) automatically.
    """
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 15))
                print(f"Rate limit hit! Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            elif response.status_code == 200:
                return response.json()
            elif response.status_code in [403, 401]:
                print("API Key expired or invalid. Please update configuration.")
                exit()
            else:
                return None
        except Exception as e:
            print(f"Request Error: {e}")
            return None

def get_runtime():
    """Returns formatted runtime string (HH:MM)."""
    elapsed_seconds = time.time() - START_TIME
    hours, remainder = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return "{:02}:{:02}".format(int(hours), int(minutes))

# --- DATA EXTRACTION LOGIC ---
def process_match(match_id, region_route, region_id):
    """
    Fetches Match V5 and Timeline V5 to extract specific columns.
    Returns a list of 10 dictionaries (one per player).
    """
    base_url = f"https://{region_route}.api.riotgames.com"
    
    # 1. Get General Match Info
    match_data = request_riot(f"{base_url}/lol/match/v5/matches/{match_id}?api_key={API_KEY}")
    if not match_data or 'info' not in match_data: return []
    
    info = match_data['info']
    
    # Filter: Game must be longer than 15 mins (900s) to get 15m stats
    game_duration = info['gameDuration']
    if game_duration < 910: 
        return []

    duration_min = game_duration / 60

    # 2. Get Timeline for 15 min stats
    timeline_data = request_riot(f"{base_url}/lol/match/v5/matches/{match_id}/timeline?api_key={API_KEY}")
    if not timeline_data or 'info' not in timeline_data: return []

    try:
        # Frames index 15 corresponds to minute 15
        frames_15 = timeline_data['info']['frames'][15]['participantFrames']
    except IndexError:
        return [] # Safety catch if game barely passed check but timeline is short

    # Pre-calculate team objectives to map to players
    team_objs = {}
    for team in info['teams']:
        t_id = team['teamId']
        objs = team['objectives']
        team_objs[t_id] = {
            'team_first_tower': 1 if objs['tower']['first'] else 0,
            'team_first_dragon': 1 if objs['dragon']['first'] else 0,
            'team_first_baron': 1 if objs['baron']['first'] else 0,
            'team_first_inhibitor': 1 if objs['inhibitor']['first'] else 0
        }

    rows = []
    
    for p in info['participants']:
        p_id = p['participantId']
        t_id = p['teamId']
        
        # Timeline data is keyed by string integer "1", "2", etc.
        p_frame = frames_15.get(str(p_id))
        
        # Stats Calculations
        cs_at_15 = p_frame['minionsKilled'] + p_frame['jungleMinionsKilled']
        cs_total = p['totalMinionsKilled'] + p['neutralMinionsKilled']
        
        # Feature Mapping
        player_row = {
            'matchId': match_id,
            'region': region_id,
            'match_length': game_duration,
            'win': 1 if p['win'] else 0,
            'teamposition': p['teamPosition'],
            'kills': p['kills'],
            'deaths': p['deaths'],
            'assists': p['assists'],
            'gold at 15': p_frame['totalGold'],
            'cs at 15': cs_at_15,
            'team_first_tower': team_objs[t_id]['team_first_tower'],
            'team_first_dragon': team_objs[t_id]['team_first_dragon'],
            'team_first_baron': team_objs[t_id]['team_first_baron'],
            'team_first_inhibitor': team_objs[t_id]['team_first_inhibitor'],
            'total_gold': p['goldEarned'],
            'total_damage': p['totalDamageDealtToChampions'],
            'total_cs': cs_total,
            'dragon kills': p['dragonKills'],
            'baron kills': p['baronKills'],
            'tower kills': p['turretKills'],
            'inhib kills': p['inhibitorKills']
        }
        rows.append(player_row)
        
    return rows

# --- MAIN CRAWLER ---
def main():
    global START_TIME
    
    # --- CONFIGURATION FOR DISTRIBUTION ---
    # 30,000 total / (3 Regions * 2 Tiers * 4 Divs) = ~1250 matches per segment
    # We add a small buffer (e.g., 1300) to ensure we hit the goal even if some fail later
    MATCHES_PER_DIVISION_CAP = 1500 
    
    seen_matches = set()
    if os.path.exists(CSV_FILE):
        try:
            df_existing = pd.read_csv(CSV_FILE, usecols=['matchId'])
            seen_matches = set(df_existing['matchId'].unique())
            print(f"Resuming... {len(seen_matches)} matches already collected.")
        except:
            print("Could not read existing CSV, starting fresh.")

    matches_collected = len(seen_matches)
    buffer_data = []

    for tier in TIERS:
        if matches_collected >= TARGET_MATCHES: break
        
        for region_conf in ORDERED_REGIONS:
            region_id = region_conf['id']
            route = region_conf['route']
            
            if matches_collected >= TARGET_MATCHES: break
            
            for division in DIVISIONS:
                if matches_collected >= TARGET_MATCHES: break

                # RESET COUNTER FOR THIS DIVISION
                division_matches_collected = 0 
                page = 1
                empty_pages_count = 0
                
                print(f"--- Switching to {region_id} | {tier} {division} ---")
                
                while True:
                    # STOPPING CONDITIONS:
                    # 1. We hit the global target
                    if matches_collected >= TARGET_MATCHES: break
                    
                    # 2. We hit the "Quota" for this specific division (Ensures we move to next region/rank)
                    if division_matches_collected >= MATCHES_PER_DIVISION_CAP:
                        print(f"  >>> Quota met for {region_id} {tier} {division}. Moving next.")
                        break

                    # 3. Riot runs out of players (Empty pages)
                    if empty_pages_count > 1: 
                        print(f"  >>> No more players found in {tier} {division}. Moving next.")
                        break
                    
                    # 1. Get Players (Seed)
                    url_seed = f"https://{region_id}.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={API_KEY}"
                    players = request_riot(url_seed)
                    
                    if not players:
                        empty_pages_count += 1
                        page += 1
                        continue

                    # SAFETY CHECK: Ensure 'players' is actually a list
                    if isinstance(players, dict):
                        # If Riot returns a dictionary (like an error we missed), skip this page
                        print(f"  >>> Warning: Unexpected Data format (Dict). Skipping page. Keys: {list(players.keys())}")
                        page += 1
                        continue
                    
                    # Reset empty page count if we found data
                    empty_pages_count = 0 
                        
                    print(f"Scanning Page {page} of players... (Matches this div: {division_matches_collected}/{MATCHES_PER_DIVISION_CAP})")
                    
                    for player in players:
                        if matches_collected >= TARGET_MATCHES: break
                        if division_matches_collected >= MATCHES_PER_DIVISION_CAP: break
                        
                        # --- CHANGE STARTS HERE ---
                        # The API now gives us 'puuid' directly, so we use that.
                        puuid = player.get('puuid')
                        
                        # If for some reason puuid is missing, try to get it via summonerId (fallback)
                        if not puuid:
                            sum_id = player.get('summonerId')
                            if sum_id:
                                puuid_data = request_riot(f"https://{region_id}.api.riotgames.com/lol/summoner/v4/summoners/{sum_id}?api_key={API_KEY}")
                                if puuid_data:
                                    puuid = puuid_data.get('puuid')
                        
                        # If we still don't have a puuid, skip this player
                        if not puuid:
                            # print(f"  >>> Skipping player (No PUUID found). Keys: {list(player.keys())}")
                            continue
                        # --- CHANGE ENDS HERE ---

                        # 2. Get Match History (Now we jump straight to this step)
                        hist_url = f"https://{route}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&start=0&count=20&api_key={API_KEY}"
                        match_ids = request_riot(hist_url)
                        
                        if not match_ids: continue
                        
                        for m_id in match_ids:
                            if matches_collected >= TARGET_MATCHES: break
                            if division_matches_collected >= MATCHES_PER_DIVISION_CAP: break
                            
                            if m_id in seen_matches: continue
                            
                            # 3. Process Match
                            new_rows = process_match(m_id, route, region_id)
                            
                            if new_rows:
                                buffer_data.extend(new_rows)
                                seen_matches.add(m_id)
                                matches_collected += 1
                                division_matches_collected += 1
                                
                                # Save every 50 matches
                                if matches_collected % 50 == 0:
                                    df = pd.DataFrame(buffer_data)
                                    df.to_csv(CSV_FILE, mode='a', header=not os.path.exists(CSV_FILE), index=False)
                                    buffer_data = [] 
                                    print(f"[{get_runtime()}] Saved Data. Total: {matches_collected}/{TARGET_MATCHES}")
                                    
                            time.sleep(1.2)
                            
                    page += 1

    # Final Save
    if buffer_data:
        pd.DataFrame(buffer_data).to_csv(CSV_FILE, mode='a', header=not os.path.exists(CSV_FILE), index=False)
        print(f"[{get_runtime()}] Final Save. Script Complete.")

if __name__ == "__main__":
    main()