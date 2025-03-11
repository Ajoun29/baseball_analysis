from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
        dag_id='bigquery_games_teams_stats_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
) as dag:
        create_games_table = BigQueryInsertJobOperator(
        task_id='create_games_table',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.games` AS
                SELECT
                    DISTINCT
                    gameId,
                    EXTRACT(DATE FROM startTime) AS date,
                    venueName,
                    homeTeamName,
                    awayTeamName,
                    homeTeamId,
                    awayTeamId,
                    homeFinalRuns,
                    awayFinalRuns,
                    homeFinalHits,
                    awayFinalHits,
                    homeFinalErrors,
                    awayFinalErrors,
                    durationMinutes
                FROM
                    `bigquery-public-data.baseball.games_wide`;
                """,
                "useLegacySql": False
            }
        },
        location="US"      )

        create_team_stats_table = BigQueryInsertJobOperator(
        task_id='create_team_stats_table',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `devoteamtechchallenge.baseball_mlb_analysis.teamStats` AS
                WITH team_games AS (
                  -- HOME perspective
                  SELECT
                    homeTeamId AS teamId,
                    homeTeamName AS teamName,
                    gameId,
                    homeFinalRuns AS finalRuns,
                    homeFinalHits AS finalHits,
                    homeFinalErrors AS finalErrors,
                    CASE WHEN homeFinalRuns > awayFinalRuns THEN 1 ELSE 0 END AS homeWin,
                    CASE WHEN homeFinalRuns < awayFinalRuns THEN 1 ELSE 0 END AS homeLoss,
                    0 AS awayWin,
                    0 AS awayLoss
                  FROM `devoteamtechchallenge.baseball_mlb_analysis.games`

                  UNION ALL

                  -- AWAY perspective
                  SELECT
                    awayTeamId AS teamId,
                    awayTeamName AS teamName,
                    gameId,
                    awayFinalRuns AS finalRuns,
                    awayFinalHits AS finalHits,
                    awayFinalErrors AS finalErrors,
                    0 AS homeWin,
                    0 AS homeLoss,
                    CASE WHEN awayFinalRuns > homeFinalRuns THEN 1 ELSE 0 END AS awayWin,
                    CASE WHEN awayFinalRuns < homeFinalRuns THEN 1 ELSE 0 END AS awayLoss
                  FROM `devoteamtechchallenge.baseball_mlb_analysis.games`
                ),
                team_stats AS (
                  SELECT
                    teamId,
                    teamName,
                    COUNT(DISTINCT gameId) AS totalGames,
                    SUM(finalRuns) AS totalRuns,
                    SUM(finalHits) AS totalHits,
                    SUM(finalErrors) AS totalErrors,
                    SUM(homeWin) AS homeWins,
                    SUM(homeLoss) AS homeLosses,
                    SUM(awayWin) AS awayWins,
                    SUM(awayLoss) AS awayLosses
                  FROM team_games
                  GROUP BY teamId, teamName
                )
                SELECT
                  teamId,
                  teamName,
                  totalGames,
                  totalRuns,
                  totalHits,
                  totalErrors,
                  homeWins,
                  homeLosses,
                  awayWins,
                  awayLosses
                FROM team_stats
                ORDER BY teamId;
                """,
                "useLegacySql": False
            }
        },
        location="US"          
    )

        create_games_table >> create_team_stats_table
