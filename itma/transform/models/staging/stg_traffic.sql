{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_traffic') }}
),

calculated AS (
    SELECT
        segment_id,
        event_time,
        event_time AT TIME ZONE 'Asia/Jakarta' AS event_time_wib,
        (event_time AT TIME ZONE 'Asia/Jakarta')::DATE AS event_date,
        EXTRACT(HOUR FROM event_time AT TIME ZONE 'Asia/Jakarta')::INTEGER AS event_hour_wib,
        speed_kmh,
        free_flow_speed_kmh,

        -- Speed ratio (actual speed divided by free flow speed)
        CASE 
            WHEN free_flow_speed_kmh > 0 
            THEN ROUND((speed_kmh / free_flow_speed_kmh)::NUMERIC, 3)
            ELSE NULL 
        END AS speed_ratio,

        -- Congestion score mapped to 1-10 (1 = free-flow, 10 = severe gridlock)
        CASE 
            WHEN free_flow_speed_kmh > 0 THEN
                LEAST(10, GREATEST(1,
                    ROUND(10.0 - (speed_kmh / free_flow_speed_kmh * 9.0))::INTEGER
                ))
            ELSE NULL
        END AS congestion_score,

        -- Peak hours: 06:00-09:00 and 17:00-20:00 WIB
        CASE 
            WHEN EXTRACT(HOUR FROM event_time AT TIME ZONE 'Asia/Jakarta') BETWEEN 6 AND 8 THEN TRUE
            WHEN EXTRACT(HOUR FROM event_time AT TIME ZONE 'Asia/Jakarta') BETWEEN 17 AND 19 THEN TRUE
            ELSE FALSE 
        END AS is_peak_hour,

        is_synthetic,
        ingested_at
    FROM source
)

SELECT * FROM calculated
