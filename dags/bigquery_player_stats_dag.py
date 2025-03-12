from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1
}

with DAG(
    dag_id="bigquery_player_stats_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="DAG to build playerStatsbyGame table in BigQuery"
) as dag:

    # 1) Create or replace the Pitcher performance table
    CREATE_PITCHER_STATS_QUERY = """
    CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.pitcherStats_game` AS
    SELECT
      pitcherId AS playerId,
      CONCAT(pitcherFirstName, " ", pitcherLastName) AS playerName,
      ANY_VALUE(pitcherThrowHand) AS throwHand,
      gameId,
      COUNTIF(pitchType = 'FA') AS fastballCount,
      COUNTIF(pitchType = 'SL') AS sliderCount,
      COUNTIF(pitchType = 'CU') AS curveballCount,
      COUNTIF(pitchType = 'CH') AS changeupCount,
      COUNTIF(pitchType = 'SP') AS splitterCount,
      COUNTIF(pitchType = 'CT') AS cutterCount,
      COUNTIF(pitchType = 'KN') AS knuckleballCount,
      COUNTIF(pitchType = 'SI') AS sinkerCount,
      COUNTIF(pitchType = 'IB') AS intentionalBallCount,
      COUNTIF(pitchType = 'PI') AS pitchoutCount,
      COUNTIF(pitchType = 'FO') AS forkballCount,
      COUNTIF(pitchType = 'SC') AS screwballCount,
      SUM(is_hit) AS hits_allowed,
      AVG(pitchSpeed) AS avg_pitch_speed,
      AVG(is_wild_pitch) AS avg_wild_pitch,
      COUNT(*) AS totalPitches
    FROM `bigquery-public-data.baseball.games_wide`
    WHERE
      atBatEventType = "PITCH"
      AND pitchType != ""
      AND (is_ab_over = 1 OR is_ab = 1)
      AND pitchSpeed > 0
    GROUP BY playerId, playerName, gameId
    ORDER BY totalPitches DESC
    """

    create_pitcher_stats = BigQueryInsertJobOperator(
        task_id="create_pitcher_stats_table",
        configuration={"query": {"query": CREATE_PITCHER_STATS_QUERY, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location="US"
    )

    # 2) Create or replace the Hitter performance table
    CREATE_HITTER_STATS_QUERY = """
    CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.hitterStats_game` AS
    SELECT
      hitterId AS playerId,
      CONCAT(hitterFirstName, ' ', hitterLastName) AS playerName,
      ANY_VALUE(hitterBatHand) AS throwHand,
      gameId,
      SUM(CASE WHEN hitType = 'GB' THEN 1 ELSE 0 END) AS groundBallCount,
      SUM(CASE WHEN hitType = 'FB' THEN 1 ELSE 0 END) AS flyBallCount,
      SUM(CASE WHEN hitType = 'LD' THEN 1 ELSE 0 END) AS lineDriveCount,
      SUM(CASE WHEN hitType = 'PU' THEN 1 ELSE 0 END) AS popUpCount,
      COUNT(*) AS total_at_bats
    FROM `bigquery-public-data.baseball.games_wide`
    WHERE
      inningEventType = "AT_BAT"
      AND is_ab_over = 1
    GROUP BY playerId, playerName, gameId
    """

    create_hitter_stats = BigQueryInsertJobOperator(
        task_id="create_hitter_stats_table",
        configuration={"query": {"query": CREATE_HITTER_STATS_QUERY, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location="US"
    )


    # 3) Create aggregated pitcher stats by team
    CREATE_PITCHER_STATS_BY_TEAM_QUERY = """
    CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.pitcherStats_team` AS
    SELECT
      teamName,
      playerId,
      playerName,
      SUM(fastballCount) AS fastball_total,
      SUM(sliderCount) AS slider_total,
      SUM(curveballCount) AS curveball_total,
      SUM(changeupCount) AS changeup_total,
      SUM(splitterCount) AS splitter_total,
      SUM(cutterCount) AS cutter_total,
      SUM(knuckleballCount) AS knuckleball_total,
      SUM(sinkerCount) AS sinker_total,
      SUM(intentionalBallCount) AS intentional_ball_total,
      SUM(pitchoutCount) AS pitchout_total,
      SUM(forkballCount) AS forkball_total,
      SUM(screwballCount) AS screwball_total,
      ANY_VALUE(throwHand) AS throwHand,
      SUM(totalPitches) AS totalPitches,
      SUM(hits_allowed) AS totalHitsAllowed,
      AVG(avg_pitch_speed) AS avgPitchSpeed
    FROM `devoteamtechchallenge.baseball_mlb_analysis.pitcherStats_game`
    LEFT JOIN `devoteamtechchallenge.baseball_mlb_analysis.playerTeam_game` player_teams USING (playerId, gameId)
    GROUP BY teamName, playerId, playerName
    """

    create_pitcher_stats_team = BigQueryInsertJobOperator(
        task_id="create_pitcher_stats_team_table",
        configuration={"query": {"query": CREATE_PITCHER_STATS_BY_TEAM_QUERY, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location="US"
    )

    # 4) Create aggregated hitter stats by team
    CREATE_HITTER_STATS_BY_TEAM_QUERY = """
    CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.hitterStats_team` AS
    SELECT
      teamName,
      playerId,
      playerName,
      ANY_VALUE(throwHand) AS throwHand,
      SUM(total_at_bats) AS totalAtBats,
      SUM(groundBallCount) AS totalGroundBalls,
      SUM(flyBallCount) AS totalFlyBalls,
      SUM(lineDriveCount) AS totalLineDrives,
      SUM(popUpCount) AS totalPopUps
    FROM `devoteamtechchallenge.baseball_mlb_analysis.hitterStats_game` games
    LEFT JOIN `devoteamtechchallenge.baseball_mlb_analysis.playerTeam_game` player_teams USING (playerId, gameId)
    GROUP BY teamName, playerId, playerName
    """

    create_hitter_stats_team = BigQueryInsertJobOperator(
        task_id="create_hitter_stats_team_table",
        configuration={"query": {"query": CREATE_HITTER_STATS_BY_TEAM_QUERY, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location="US"
    )

    # DAG dependencies:
    create_pitcher_stats >> create_pitcher_stats_team
    create_hitter_stats >> create_hitter_stats_team
