from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
import requests
import pytz
from airflow.exceptions import AirflowSkipException

default_args = {
    'owner': 'duane',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Helper to fetch game ID for today's Heat game
def get_heat_game_id():
    et = pytz.timezone('America/New_York')
    now_et = datetime.now(et)
    today = now_et.strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={today}"

    resp = requests.get(url, timeout=10)
    data = resp.json()

    for event in data.get('events', []):
        comp = event['competitions'][0]
        teams = comp['competitors']
        if any(t['team']['abbreviation'] == 'MIA' for t in teams):
            return event['id']
    raise AirflowSkipException("No Heat game today.")

# Sensor to wait for specific quarter data
def quarter_sensor_func(quarter_number, is_half=False):
    """
    Sensor that returns True if the quarter/half has started or finished.
    - If game is over → True
    - If current quarter >= target quarter → True (catch-up logic)
    - Otherwise wait
    """
    try:
        game_id = get_heat_game_id()
    except AirflowSkipException:
        return True  # no game → skip sensor gracefully

    box_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
    try:
        resp = requests.get(box_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except:
        return False  # retry on API failure
    
    print(f"quarter_number: {quarter_number}")
    print(f"game_id: {game_id}")
    print(f"data: {data}")

    status = data['header']['competitions'][0]['status']['type']['state']
    current_quarter = data['header']['competitions'][0]['status']['period']

    if status == 'post':
        return True  # game over → all sensors pass

    if status != 'in':
        return False  # pre-game → keep waiting

    # Catch-up logic: if we're already past the target quarter/half
    target = quarter_number
    if is_half:
        target = 2 if quarter_number == 2 else 4  # first half = 2, second half = 4

    if current_quarter > target:
        print(f"Game already in Q{current_quarter} — catching up past {period_type}")
        return True

    return False  # still waiting for this quarter/half

# Task to generate update for a quarter or half
def generate_update(quarter_number, is_half=False):
    game_id = get_heat_game_id()
    box_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
    resp = requests.get(box_url, timeout=10)
    data = resp.json()

    # Get scoreboard
    comp = data['header']['competitions'][0]
    teams = comp['competitors']
    home = teams[0] if teams[0]['homeAway'] == 'home' else teams[1]
    away = teams[1] if teams[1]['homeAway'] == 'away' else teams[0]
    score_home = home['score']
    score_away = away['score']

    period_type = f"Quarter {quarter_number}" if not is_half else f"{'First' if quarter_number == 2 else 'Second'} Half"
    print(f"\n{period_type} Update:")
    print(f"{away['team']['displayName']} {score_away} - {home['team']['displayName']} {score_home}")

    # Fetch per-period stats if available (ESPN has limited per-quarter boxscore, so approximate with full for now)
    # For real per-quarter, we'd need play-by-play parsing (add nba_api later)
    # For now, use full boxscore leaders

    # Leaders per category
    categories = ['points', 'rebounds', 'assists']
    leaders = {cat: {'home': [], 'away': []} for cat in categories}

    for team in data['boxscore']['players']:
        team_abbr = team['team']['abbreviation']
        athletes = team['statistics'][0]['athletes']
        for player in athletes:
            stats = player['stats']
            if len(stats) > 1:
                pts = int(stats[1])
                reb = int(stats[5])
                ast = int(stats[7])
                name = player['athlete']['displayName']
                if team_abbr == 'MIA':
                    leaders['points']['home'].append((name, pts))
                    leaders['rebounds']['home'].append((name, reb))
                    leaders['assists']['home'].append((name, ast))
                else:
                    leaders['points']['away'].append((name, pts))
                    leaders['rebounds']['away'].append((name, reb))
                    leaders['assists']['away'].append((name, ast))

    for cat in categories:
        print(f"\n{cat.capitalize()} leaders:")
        for side in ['home', 'away']:
            sorted_leaders = sorted(leaders[cat][side], key=lambda x: x[1], reverse=True)[:3]
            for name, value in sorted_leaders:
                print(f"  {side.capitalize()} - {name}: {value}")

with DAG(
    dag_id='miami_heat_quarter_updates',
    default_args=default_args,
    description='Live updates for Miami Heat games per quarter and half',
    start_date=datetime(2026, 2, 1),
    schedule='*/5 * * * *',  # every 5 minutes
    catchup=False,
    tags=['heat', 'live', 'quarter'],
) as dag:

    wait_q1 = PythonSensor(
        task_id='wait_for_q1',
        python_callable=quarter_sensor_func,
        op_args=[0],  # Q1
        poke_interval=60,  # check every 60 seconds
        timeout=3600 * 4,  # 4 hours max wait
    )

    update_q1 = PythonOperator(
        task_id='update_q1',
        python_callable=generate_update,
        op_args=[1],
    )

    wait_q2 = PythonSensor(
        task_id='wait_for_q2',
        python_callable=quarter_sensor_func,
        op_args=[1],  # Q2
        poke_interval=60,
        timeout=3600 * 4,
    )

    update_q2 = PythonOperator(
        task_id='update_q2',
        python_callable=generate_update,
        op_args=[2],
    )

    update_first_half = PythonOperator(
        task_id='update_first_half',
        python_callable=generate_update,
        op_args=[2, True],  # Half = True
    )

    wait_q3 = PythonSensor(
        task_id='wait_for_q3',
        python_callable=quarter_sensor_func,
        op_args=[2],  # Q3
        poke_interval=60,
        timeout=3600 * 4,
    )

    update_q3 = PythonOperator(
        task_id='update_q3',
        python_callable=generate_update,
        op_args=[3],
    )

    wait_q4 = PythonSensor(
        task_id='wait_for_q4',
        python_callable=quarter_sensor_func,
        op_args=[3],  # Q4
        poke_interval=60,
        timeout=3600 * 4,
    )

    update_q4 = PythonOperator(
        task_id='update_q4',
        python_callable=generate_update,
        op_args=[4],
    )

    update_second_half = PythonOperator(
        task_id='update_second_half',
        python_callable=generate_update,
        op_args=[4, True],
    )

    wait_q1 >> update_q1 >> wait_q2 >> update_q2 >> update_first_half >> wait_q3 >> update_q3 >> wait_q4 >> update_q4 >> update_second_half