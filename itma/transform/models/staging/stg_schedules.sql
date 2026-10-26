{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_schedules') }}
),

parsed AS (
    SELECT
        trip_id,
        route_id,
        stop_id,
        service_date,
        scheduled_arrival_time,
        -- Parse scheduled_arrival_time (which supports >24:00:00) into a WIB timestamptz
        (
            service_date::TIMESTAMP + (
                INTERVAL '1 hour' * SPLIT_PART(scheduled_arrival_time, ':', 1)::INTEGER
                + INTERVAL '1 minute' * SPLIT_PART(scheduled_arrival_time, ':', 2)::INTEGER
                + INTERVAL '1 second' * COALESCE(NULLIF(SPLIT_PART(scheduled_arrival_time, ':', 3), ''), '0')::INTEGER
            )
        ) AT TIME ZONE 'Asia/Jakarta' AS scheduled_arrival_at,
        actual_arrival_time,
        is_synthetic,
        ingested_at
    FROM source
),

calculated AS (
    SELECT
        trip_id,
        route_id,
        stop_id,
        service_date,
        scheduled_arrival_time,
        scheduled_arrival_at,
        actual_arrival_time,
        ROUND(
            (EXTRACT(EPOCH FROM (actual_arrival_time - scheduled_arrival_at)) / 60.0)::NUMERIC,
            2
        ) AS delay_minutes,
        is_synthetic,
        ingested_at
    FROM parsed
),

flagged AS (
    SELECT
        trip_id,
        route_id,
        stop_id,
        service_date,
        scheduled_arrival_time,
        scheduled_arrival_at,
        actual_arrival_time,
        delay_minutes,
        -- Business Rules:
        -- Delayed if delay > 5 minutes
        CASE 
            WHEN delay_minutes > 5.0 THEN TRUE 
            ELSE FALSE 
        END AS is_delayed,
        -- Outlier if delay < -30 minutes
        CASE 
            WHEN delay_minutes < -30.0 THEN TRUE 
            ELSE FALSE 
        END AS is_outlier,
        -- Extract hour in WIB (Asia/Jakarta)
        EXTRACT(HOUR FROM actual_arrival_time AT TIME ZONE 'Asia/Jakarta')::INTEGER AS arrival_hour_wib,
        -- Peak hours: 06:00-09:00 (hours 6, 7, 8) and 17:00-20:00 (hours 17, 18, 19) WIB
        CASE 
            WHEN EXTRACT(HOUR FROM actual_arrival_time AT TIME ZONE 'Asia/Jakarta') BETWEEN 6 AND 8 THEN TRUE
            WHEN EXTRACT(HOUR FROM actual_arrival_time AT TIME ZONE 'Asia/Jakarta') BETWEEN 17 AND 19 THEN TRUE
            ELSE FALSE 
        END AS is_peak_hour,
        is_synthetic,
        ingested_at
    FROM calculated
)

SELECT * FROM flagged
