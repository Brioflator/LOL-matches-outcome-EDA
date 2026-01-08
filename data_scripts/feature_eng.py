import pandas as pd
import numpy as np

def feature_engineer_lol_data(input_file, output_file):
    # 1. Load the data
    df = pd.read_csv(input_file)
    
    # Store the list of original columns to separate them later
    original_cols = df.columns.tolist()

    # 2. Perform Feature Engineering
    
    # Individual Performance Metrics
    # Replace 0 deaths with 1 to avoid division by zero errors
    df['KDA'] = (df['kills'] + df['assists']) / df['deaths'].replace(0, 1)
    
    # Match length in minutes for per-minute stats
    match_length_min = df['match_length'] / 60
    
    df['GPM'] = df['total_gold'] / match_length_min
    df['DPM'] = df['total_damage'] / match_length_min
    df['CSPM'] = df['total_cs'] / match_length_min

    # Team Aggregates
    # Group by matchId and win (side) to get team totals
    team_stats = df.groupby(['matchId', 'win'])[['kills', 'total_gold', 'total_damage', 'total_cs']].sum().reset_index()
    team_stats = team_stats.rename(columns={
        'kills': 'team_total_kills',
        'total_gold': 'team_total_gold',
        'total_damage': 'team_total_damage',
        'total_cs': 'team_total_cs'
    })

    # Merge team stats back to the main dataframe
    df = pd.merge(df, team_stats, on=['matchId', 'win'], how='left')

    # Relative Performance (Shares & Participation)
    df['KP'] = (df['kills'] + df['assists']) / df['team_total_kills'].replace(0, 1)
    df['Gold_Share'] = df['total_gold'] / df['team_total_gold']
    df['Damage_Share'] = df['total_damage'] / df['team_total_damage']
    df['CS_Share'] = df['total_cs'] / df['team_total_cs']

    # Early Game Metrics (Normalized)
    df['Gold_at_15_PM'] = df['gold at 15'] / 15
    df['CS_at_15_PM'] = df['cs at 15'] / 15

    # 3. Create the Empty Separator Column
    
    # Identify the new columns by subtracting original columns from current columns
    current_cols = df.columns.tolist()
    new_cols = [c for c in current_cols if c not in original_cols]
    
    # Create a temporary column filled with NaNs (empty values)
    sep_col_name = 'separator_col'
    df[sep_col_name] = np.nan
    
    # Define the final column order: Original + Separator + New
    final_cols = original_cols + [sep_col_name] + new_cols
    df = df[final_cols]
    
    # Rename the separator column to an empty string so the header is blank
    df = df.rename(columns={sep_col_name: ''})

    # 4. Save to CSV
    df.to_csv(output_file, index=False)
    print(f"File saved to {output_file} with {len(new_cols)} new features.")

if __name__ == "__main__":
    # Replace 'example.csv' with your actual file name
    feature_engineer_lol_data('league_dataset_initial.csv', 'lol_data_engineered.csv')