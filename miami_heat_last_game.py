from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pytz

default_args = {
    'owner': 'duane',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def fetch_last_heat_game():
    """
    Find and print summary of the most recent completed Heat game.
    Runs daily, but only prints when a new game is found.
    """
    et = pytz.timezone('America/New_York')
    now_et = datetime.now(et)
    
    # Look back 7 days to find the last game
    for days_ago in range(1, 8):
        date = (now_et - timedelta(days=days_ago)).strftime('%Y%m%d')
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date}"
        
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Failed to fetch {date}: {e}")
            continue

        for event in data.get('events', []):
            comp = event['competitions'][0]
            teams = comp['competitors']
            if any(t['team']['abbreviation'] == 'MIA' for t in teams):
                status = comp['status']['type']['state']
                if status == 'post':  # game completed
                    score_home = teams[0]['score'] if 'score' in teams[0] else 'N/A'
                    score_away = teams[1]['score'] if 'score' in teams[1] else 'N/A'
                    home = teams[0]['team']['displayName'] if teams[0]['homeAway'] == 'home' else teams[1]['team']['displayName']
                    away = teams[1]['team']['displayName'] if teams[1]['homeAway'] == 'away' else teams[0]['team']['displayName']
                    
                    print(f"🏀 Last completed Heat game: {away} @ {home}")
                    print(f"Date: {event['date'][:10]}")
                    print(f"Final score: {away} {score_away} - {home} {score_home}")
                    print("Full recap / LLM summary coming soon...")
                    
                    # TODO: Add LLM call here
                    # recap = call_llm_for_recap(event['id'])
                    # print(recap)
                    
                    return  # Stop after finding the most recent one

    print("No completed Heat game found in the last 7 days.")

with DAG(
    dag_id='miami_heat_last_game_summary',
    default_args=default_args,
    description='Fetch and summarize the most recent completed Miami Heat game',
    start_date=datetime(2026, 2, 1),
    schedule='0 8 * * *',  # 8 AM ET daily (after overnight games)
    catchup=False,
    tags=['heat', 'summary', 'last-game'],
) as dag:

    fetch_last = PythonOperator(
        task_id='fetch_last_heat_game',
        python_callable=fetch_last_heat_game,
    )