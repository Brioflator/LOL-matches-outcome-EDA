import pandas as pd

def transform_to_team_stats(input_file, output_file):
    # Load the player-level data
    df = pd.read_csv(input_file)

    # Define aggregation rules: 
    # Sum for player stats, 'first' for team-wide stats and identifiers
    agg_rules = {
        'region': 'first',
        'match_length': 'first',
        'kills': 'sum',
        'deaths': 'sum',
        'assists': 'sum',
        'gold at 15': 'sum',
        'cs at 15': 'sum',
        'team_first_tower': 'first',
        'team_first_dragon': 'first',
        'team_first_baron': 'first',
        'team_first_inhibitor': 'first',
        'total_gold': 'sum',
        'total_damage': 'sum',
        'total_cs': 'sum',
        'dragon kills': 'sum',
        'baron kills': 'sum',
        'tower kills': 'sum',
        'inhib kills': 'sum'
    }

    # Group by matchId and win (effectively separating the two teams)
    team_df = df.groupby(['matchId', 'win']).agg(agg_rules).reset_index()

    # --- Calculate Gold Advantage at 15 ---
    # We join the table with itself to find the opponent's gold
    opponents = team_df[['matchId', 'win', 'gold at 15']].copy()
    opponents['win'] = 1 - opponents['win']  # Flip win (0 becomes 1, 1 becomes 0) to match with opponent
    opponents = opponents.rename(columns={'gold at 15': 'opponent_gold_at_15'})
    
    team_df = team_df.merge(opponents, on=['matchId', 'win'], how='left')
    team_df['team_gold_adv_at_15'] = team_df['gold at 15'] - team_df['opponent_gold_at_15']
    
    # Rename columns to match the requested format
    column_mapping = {
        'kills': 'team_kills',
        'deaths': 'team_deaths',
        'assists': 'team_assists',
        'gold at 15': 'team_gold_at_15',
        'cs at 15': 'team_cs_at_15',
        'total_gold': 'total_gold_team',
        'total_damage': 'total_damage_team',
        'total_cs': 'total_cs_team',
        'dragon kills': 'dragon_kills_total',
        'baron kills': 'baron_kills_total',
        'tower kills': 'tower_kills_total',
        'inhib kills': 'inhibitor_kills_total'
    }
    team_df = team_df.rename(columns=column_mapping)
    
    # Drop the temporary helper column and reorder for cleanliness
    team_df = team_df.drop(columns=['opponent_gold_at_15'])
    
    # Save the result
    team_df.to_csv(output_file, index=False)
    return team_df

# Usage

transform_to_team_stats('league_dataset_initial.csv', 'team_stats.csv')