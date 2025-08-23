## i have 11 dag that manage flow of data form api to kafka and from kafka to spark to validate then to kafka valid and invalid topics 
# 11 dags splits to followin :
    - 7 dags send historical data as batches every 24h at midnight
    - 3 dags send live data every 2m 
    - 1 dag sends invalid data to parquet file for analysis the reason of rejecting every 24h at 6-am

# every dag has 3 retries if not succeeded it sends a message to mail to alert the failure
# we use duckdb and grafana to monitor the invalid message everyday and get insighit about reason of failure and stats in dashboard sending email every day with new dashboard

# historical data dags are :
    - venue_proucer_daily_dag
    - team_proucer_daily_dag
    - schedual_proucer_daily_dag
    - player_proucer_daily_dag
    - league_proucer_daily_dag
    - event_stats_daily_dag
    - event_proucer_daily_dag

# live data dags are :
    - broadcast_dag
    - event_lookup_dag
    - live_score_dag

# invalid data dag is :
    - save_rejected_topics_as_parquet_daily



##-------------------------------

SELECT COUNT(*) AS 'number of all invalid data'
FROM read_parquet(
    '/data/kafka_invalid/' || strftime(current_date - INTERVAL 1 DAY, '%Y-%m-%d') || '.parquet'
)

##------------------------------

SELECT message ->> 'strSport' AS sport, COUNT(*) AS null_and_empty
FROM read_parquet('/data/kafka_invalid/' || strftime(current_date - INTERVAL 1 DAY, '%Y-%m-%d') || '.parquet')
GROUP BY 1
ORDER BY null_and_empty DESC
LIMIT 20

##------------------------------

SELECT message ->> 'strStatus' AS 'Status of match', COUNT(*) AS null_values
FROM read_parquet('/data/kafka_invalid/' || strftime(current_date - INTERVAL 1 DAY, '%Y-%m-%d') || '.parquet')
GROUP BY 1
ORDER BY null_values DESC
LIMIT 20

##------------------------------

SELECT topic, COUNT(*) AS message_count
FROM read_parquet(
    '/data/kafka_invalid/' || strftime(current_date - INTERVAL 1 DAY, '%Y-%m-%d') || '.parquet'
)
GROUP BY topic
ORDER BY message_count DESC;
