from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import pytz
import json

default_args = {
    'owner': 'duane',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def fetch_last_heat_game_details(**context):
    et = pytz.timezone('America/New_York')
    now_et = datetime.now(et)

    # Look back 7 days for the most recent completed Heat game
    last_game = None
    for days_ago in range(1, 8):
        date_str = (now_et - timedelta(days=days_ago)).strftime('%Y%m%d')
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
        
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Failed to fetch {date_str}: {e}")
            continue

        for event in data.get('events', []):
            comp = event['competitions'][0]
            teams = comp['competitors']
            if any(t['team']['abbreviation'] == 'MIA' for t in teams):
                status = comp['status']['type']['state']
                if status == 'post':
                    last_game = {
                        'game_id': event['id'],
                        'date': event['date'][:10],
                        'name': event['name'],
                        'home': teams[0] if teams[0]['homeAway'] == 'home' else teams[1],
                        'away': teams[1] if teams[1]['homeAway'] == 'away' else teams[0],
                        'score_home': teams[0]['score'] if 'score' in teams[0] else 'N/A',
                        'score_away': teams[1]['score'] if 'score' in teams[1] else 'N/A',
                    }
                    # Stop at the most recent completed game
                    break
        if last_game:
            break

    if not last_game:
        print("No completed Heat game found in the last 7 days.")
        return

    # Print basic info
    home_name = last_game['home']['team']['displayName']
    away_name = last_game['away']['team']['displayName']
    print(f"🏀 Last completed Heat game: {away_name} @ {home_name}")
    print(f"Date: {last_game['date']}")
    print(f"Final score: {away_name} {last_game['score_away']} - {home_name} {last_game['score_home']}")

    # Fetch more details from boxscore endpoint
    box_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={last_game['game_id']}"
    try:
        box_resp = requests.get(box_url, timeout=10)
        box_data = box_resp.json()
    except Exception as e:
        print(f"Failed to fetch boxscore: {e}")
        box_data = {}

    # Extract top scorers (example: first 3 from each team)
    top_scorers = []
    for team in box_data.get('boxscore', {}).get('players', []):
        team_name = team.get('team', {}).get('displayName', 'Unknown')
        statistics = team.get('statistics', [])
        if not isinstance(statistics, list):
            continue

        for stat_group in statistics:
            athletes = stat_group.get('athletes', [])
            if not isinstance(athletes, list):
                continue

            for player in athletes:
                athlete = player.get('athlete', {})
                name = athlete.get('displayName', 'Unknown')
                active = player.get('active', False)
                if not active:
                    continue

                stats = player.get('stats', [])
                if not isinstance(stats, list) or len(stats) < 12:
                    continue  # skip if too short

                try:
                    minutes = stats[0] if len(stats) > 0 else '0'
                    points = int(stats[1]) if len(stats) > 1 else 0
                    fg = stats[2] if len(stats) > 2 else '0-0'
                    three_pt = stats[3] if len(stats) > 3 else '0-0'
                    ft = stats[4] if len(stats) > 4 else '0-0'
                    rebounds = int(stats[5]) if len(stats) > 5 else 0
                    off_reb = int(stats[6]) if len(stats) > 6 else 0
                    assists = int(stats[7]) if len(stats) > 7 else 0
                    steals = int(stats[8]) if len(stats) > 8 else 0
                    blocks = int(stats[9]) if len(stats) > 9 else 0
                    turnovers = int(stats[10]) if len(stats) > 10 else 0
                    fouls = int(stats[11]) if len(stats) > 11 else 0
                    plus_minus = stats[12] if len(stats) > 12 else '+0'

                    top_scorers.append({
                        'name': name,
                        'team': team_name,
                        'pts': points,
                        'reb': rebounds,
                        'ast': assists,
                        'stl': steals,
                        'blk': blocks,
                        'to': turnovers,
                        'min': minutes,
                        'fg': fg,
                        '3pt': three_pt,
                        'ft': ft,
                        '+/-': plus_minus,
                    })
                except (IndexError, ValueError):
                    continue  # skip bad stat lines

    top_scorers = sorted(top_scorers, key=lambda x: x['pts'], reverse=True)[:6]
    print("\nTop performers:")
    for p in top_scorers:
        print(f"  {p['name']} ({p['team']}): {p['pts']} PTS, {p['reb']} REB, {p['ast']} AST")

    # Generate recap with Grok API
    grok_key = Variable.get("GROK_API_KEY", default_var=None)
    if not grok_key:
        print("GROK_API_KEY not set in Airflow Variables. Skipping LLM recap.")
        return

    prompt = f"""
Write a short, engaging recap of the Miami Heat vs {home_name if 'MIA' not in away_name else away_name} game on {last_game['date']}.
Final score: {away_name} {last_game['score_away']} - {home_name} {last_game['score_home']}.
Highlight key performances, turning points, and team momentum.
Keep it under 200 words, exciting but factual.
"""

    try:
        grok_url = "https://api.x.ai/v1/chat/completions"  # Grok API endpoint (check x.ai for latest)
        headers = {
            "Authorization": f"Bearer {grok_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "grok-beta",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 300
        }
        resp = requests.post(grok_url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        recap_text = resp.json()['choices'][0]['message']['content']
        print("\nGrok Recap:\n" + recap_text)
    except Exception as e:
        print(f"Failed to generate recap with Grok: {e}")

with DAG(
    dag_id='miami_heat_last_game_summary',
    default_args=default_args,
    description='Fetch and summarize the most recent completed Miami Heat game',
    start_date=datetime(2026, 2, 1),
    schedule='0 8 * * *',  # 8 AM ET daily
    catchup=False,
    tags=['heat', 'summary', 'last-game'],
) as dag:

    fetch_last = PythonOperator(
        task_id='fetch_last_heat_game',
        python_callable=fetch_last_heat_game_details,
    )