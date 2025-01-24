create index source_tables_USER_INDEX.source_ga4_events on (SELECT
  session_id,
  user_pseudo_id,
  device,
  DATE(TIMESTAMP_MICROS(session_start_timestamp)) AS date,
  session_start_timestamp,
  TIMESTAMP_DIFF(session_end_timestamp, session_start_timestamp, SECOND) AS session_duration_in_sec,
IF
  ( COUNTIF(event_name = 'purchase') > 0
    OR COUNTIF(event_name = 'view_item') >= 2
    OR TIMESTAMP_DIFF(session_end_timestamp, session_start_timestamp, SECOND) > 10, TRUE, FALSE ) AS is_session_engaged
FROM
  ecomtest-197322.source_tables.USER_INDEX.source_ga4_events
GROUP BY
  session_id,
  user_pseudo_id,
  device,
  session_start_timestamp,
  session_end_timestamp);
