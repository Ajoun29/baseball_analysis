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
    CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.pitcherStats` AS
    SELECT
      pitcherId AS playerId,
      CONCAT(pitcherFirstName, " ", pitcherLastName) AS playerName,
      pitcherThrowHand AS throwHand,
      gameId,
      "Pitcher" AS playerType,
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
      0 AS groundBallCount,
      0 AS flyBallCount,
      0 AS lineDriveCount,
      0 AS popUpCount,
      COUNT(*) AS totalPitches,
      0 AS totalHits
    FROM `bigquery-public-data.baseball.games_wide`
    WHERE
      atBatEventType = "PITCH"
      AND pitchType != ""
      AND is_ab_over = 1
    GROUP BY
      playerId,
      playerName,
      throwHand,
      gameId
    """

    create_pitcher_stats = BigQueryInsertJobOperator(
        task_id="create_pitcher_stats_table",
        configuration={
            "query": {
                "query": CREATE_PITCHER_STATS_QUERY,
                "useLegacySql": False
            }
        },
        gcp_conn_id="google_cloud_default",
        location="US"
    )

    # 2) Create or replace the Hitter performance table
    CREATE_HITTER_STATS_QUERY = """
    CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.hitterStats` AS
    SELECT
      hitterId AS playerId,
      CONCAT(hitterFirstName, " ", hitterLastName) AS playerName,
      hitterBatHand AS throwHand,
      gameId,
      "Hitter" AS playerType,
      0 AS fastballCount,
      0 AS sliderCount,
      0 AS curveballCount,
      0 AS changeupCount,
      0 AS splitterCount,
      0 AS cutterCount,
      0 AS knuckleballCount,
      0 AS sinkerCount,
      0 AS intentionalBallCount,
      0 AS pitchoutCount,
      0 AS forkballCount,
      0 AS screwballCount,
      COUNTIF(hitType = 'GB') AS groundBallCount,
      COUNTIF(hitType = 'FB') AS flyBallCount,
      COUNTIF(hitType = 'LD') AS lineDriveCount,
      COUNTIF(hitType = 'PU') AS popUpCount,
      0 AS totalPitches,
      COUNT(*) AS totalHits
    FROM `bigquery-public-data.baseball.games_wide`
    WHERE
      atBatEventType = "PITCH"
      AND pitchType != ""
      AND is_ab_over = 1
    GROUP BY
      playerId,
      playerName,
      throwHand,
      gameId
    """

    create_hitter_stats = BigQueryInsertJobOperator(
        task_id="create_hitter_stats_table",
        configuration={
            "query": {
                "query": CREATE_HITTER_STATS_QUERY,
                "useLegacySql": False
            }
        },
        gcp_conn_id="google_cloud_default",
        location="US"
    )

    # 3) Combine pitcherStats and hitterStats into the final table
    CREATE_PLAYER_STATS_QUERY = """
    CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.playerStatsbyGame` AS
    SELECT * FROM `devoteamtechchallenge.baseball_mlb_analysis.pitcherStats`
    UNION ALL
    SELECT * FROM `devoteamtechchallenge.baseball_mlb_analysis.hitterStats`
    """

    combine_player_stats = BigQueryInsertJobOperator(
        task_id="create_player_statsbygame_table",
        configuration={
            "query": {
                "query": CREATE_PLAYER_STATS_QUERY,
                "useLegacySql": False
            }
        },
        gcp_conn_id="google_cloud_default",
        location="US"
    )

    # DAG dependencies:
    # The final step (combine_player_stats) depends on both pitcher and hitter tables being created
    [create_pitcher_stats, create_hitter_stats] >> combine_player_stats
