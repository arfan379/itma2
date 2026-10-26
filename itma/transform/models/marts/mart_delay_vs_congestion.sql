{{ config(materialized='table') }}

WITH delays AS (
    SELECT * FROM {{ ref('fct_delay_index') }}
),

density AS (
    SELECT * FROM {{ ref('fct_route_density') }}
),

combined AS (
    SELECT
        d.route_id,
        d.service_date,
        d.total_trips,
        d.delayed_trips,
        d.avg_delay_minutes,
        d.median_delay_minutes,
        d.p95_delay_minutes,
        d.otp_percentage,
        -- Traffic congestion indicators
        c.avg_peak_congestion_score,
        c.avg_peak_speed_kmh,
        c.avg_peak_speed_ratio,
        c.monitored_segments_count,
        c.total_peak_telemetry_events,
        -- Classification of operational health
        CASE
            WHEN d.otp_percentage < 70.0 AND c.avg_peak_congestion_score >= 6.0 
                THEN 'Severe Delay Correlated with Congestion'
            WHEN d.otp_percentage < 70.0 AND (c.avg_peak_congestion_score < 6.0 OR c.avg_peak_congestion_score IS NULL)
                THEN 'Delay Likely Operational / Dispatch Issue'
            WHEN d.otp_percentage >= 70.0 AND c.avg_peak_congestion_score >= 6.0
                THEN 'Resilient Corridor (High Traffic but Maintained OTP)'
            ELSE 'Healthy Service'
        END AS corridor_health_diagnosis
    FROM delays d
    LEFT JOIN density c 
        ON d.route_id = c.route_id 
        AND d.service_date = c.event_date
)

SELECT * FROM combined
