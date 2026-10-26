{{ config(materialized='table') }}

WITH peak_traffic AS (
    SELECT * FROM {{ ref('stg_traffic') }}
    -- Filter strictly for morning and evening peak hours per FR-06
    WHERE is_peak_hour = TRUE
),

route_segments AS (
    SELECT * FROM {{ ref('route_segments') }}
),

joined AS (
    SELECT
        rs.route_id,
        t.event_date,
        t.segment_id,
        t.congestion_score,
        t.speed_kmh,
        t.speed_ratio
    FROM peak_traffic t
    INNER JOIN route_segments rs ON t.segment_id = rs.segment_id
),

aggregated AS (
    SELECT
        route_id,
        event_date,
        COUNT(*) AS total_peak_telemetry_events,
        COUNT(DISTINCT segment_id) AS monitored_segments_count,
        ROUND(AVG(congestion_score)::NUMERIC, 2) AS avg_peak_congestion_score,
        ROUND(AVG(speed_kmh)::NUMERIC, 2) AS avg_peak_speed_kmh,
        ROUND(AVG(speed_ratio)::NUMERIC, 3) AS avg_peak_speed_ratio
    FROM joined
    GROUP BY route_id, event_date
)

SELECT * FROM aggregated
