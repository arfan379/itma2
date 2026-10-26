{{ config(materialized='table') }}

WITH schedules AS (
    SELECT * FROM {{ ref('stg_schedules') }}
    -- Exclude extreme outliers (< -30 minutes) per business requirements
    WHERE is_outlier = FALSE
),

route_trip_status AS (
    -- Determine trip-level delay status (a trip is delayed if ANY of its stops experienced > 5 min delay)
    SELECT
        route_id,
        service_date,
        trip_id,
        MAX(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS trip_was_delayed,
        AVG(delay_minutes) AS trip_avg_delay,
        MAX(delay_minutes) AS trip_max_delay
    FROM schedules
    GROUP BY route_id, service_date, trip_id
),

aggregated AS (
    SELECT
        route_id,
        service_date,
        COUNT(DISTINCT trip_id) AS total_trips,
        SUM(trip_was_delayed) AS delayed_trips,
        ROUND(AVG(trip_avg_delay)::NUMERIC, 2) AS avg_delay_minutes,
        ROUND(
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_avg_delay)::NUMERIC,
            2
        ) AS median_delay_minutes,
        ROUND(
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY trip_max_delay)::NUMERIC,
            2
        ) AS p95_delay_minutes,
        -- On-Time Performance (OTP) Percentage:
        -- otp_percentage = (total_trips - delayed_trips) / total_trips * 100
        ROUND(
            (
                (COUNT(DISTINCT trip_id) - SUM(trip_was_delayed))::NUMERIC
                / NULLIF(COUNT(DISTINCT trip_id), 0) * 100.0
            )::NUMERIC,
            2
        ) AS otp_percentage
    FROM route_trip_status
    GROUP BY route_id, service_date
)

SELECT * FROM aggregated
