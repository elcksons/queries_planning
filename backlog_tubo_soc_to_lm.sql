WITH regional AS (
    SELECT
        CAST(station_id AS BIGINT) AS station_id,
        regional AS lm_regional,
        sub_regional,
        superior_station_code,
        station_name,
        station_code AS code_name,
        CASE
        WHEN SUBSTRING(station_code, 1, 3) = 'XPT' THEN 
            CASE
            WHEN SUBSTRING(superior_station_code, LENGTH(superior_station_code), 1) = 'X' THEN SUBSTRING(superior_station_code, 1, 10)
            ELSE superior_station_code
            END
        ELSE station_code
        END AS superior_code_hub
    FROM dev_brbi_opslgc.regional_lm
    WHERE station_id NOT IN ('-', '?', '.', '','Inativo','Inactive')

),

max_station_sequence as (

select

shipment_id
,MAX(station_sequence) as max_sequence
from brbi_opslgc.da_log_spx_mm_process_hourly
group by 1

),

max_grass_date AS (

SELECT MAX(grass_date) AS max_date
FROM spx_mart.dim_spx_station_br

)

SELECT

  CASE 
       WHEN SUBSTRING(r.superior_code_hub, LENGTH(r.superior_code_hub), 1) = 'X' THEN SUBSTRING(r.superior_code_hub, 1, 10) ELSE r.superior_code_hub END AS station_code,
    
    
    --CASE WHEN SUBSTRING(r.superior_code_hub, LENGTH(r.superior_code_hub ),1) = 'X' THEN SUBSTRING(r.superior_code_hub, 1,10) else r.superior_code_hub end station_code,
    st.station_id,
    st.station_name,
    
    COUNT(CASE WHEN stu.latest_spx_tracking_code = 8 THEN stu.shipment_id END) AS "1st_SOC_Received",
    COUNT(CASE WHEN stu.latest_spx_tracking_code = 9 THEN stu.shipment_id END) AS "1st_SOC_Packing",
    COUNT(CASE WHEN stu.latest_spx_tracking_code = 33 THEN stu.shipment_id END) AS "1st_SOC_Packed",
    COUNT(CASE WHEN stu.latest_spx_tracking_code = 630 THEN stu.shipment_id END) AS "SOC_Staging",
    COUNT(CASE WHEN stu.latest_spx_tracking_code = 15 THEN stu.shipment_id END) AS "SOC_LHTransporting",
    COUNT(CASE WHEN stu.latest_spx_tracking_code IN (15,36) AND SUBSTRING(stu.next_point, 1,3) = 'SoC' THEN stu.shipment_id END) AS "SoC_to_SoC", 
    COUNT(CASE WHEN stu.latest_spx_tracking_code IN (15) AND SUBSTRING(stu.next_point, 1,2) = 'LM' THEN stu.shipment_id END) AS "SoC_to_LM_transporting",
    COUNT(CASE WHEN stu.latest_spx_tracking_code IN (36) AND SUBSTRING(stu.next_point, 1,2) = 'LM' THEN stu.shipment_id END) AS "SoC_to_LM_transported", --versao original COUNT(CASE WHEN stu.latest_spx_tracking_code IN (36) AND SUBSTRING(stu.next_point, 1,2) = 'LM' THEN stu.shipment_id END) AS "SOC_LHTransported"
    CAST((current_timestamp - interval '11' hour) as timestamp) as last_update_time
FROM
    brbi_opslgc.da_log_spx_mm_process_hourly stu
    LEFT JOIN spx_mart.dim_spx_station_br  sta ON stu.next_point = sta.station_name --//LEFT JOIN spx_mart.dim_spx_station_tab_ri_br_ro spx_mart.dim_spx_station_br
    LEFT JOIN regional r on CAST(r.station_id as bigint) = CAST(stu.final_spx_station_id as bigint)
    LEFT JOIN max_station_sequence mx ON mx.shipment_id = stu.shipment_id
    LEFT JOIN (SELECT
               r.station_id,
               r.superior_code_hub,
               r.station_name,
               r.superior_station_code
               FROM regional r
               WHERE SUBSTRING(code_name, 1,3) = 'HUB') as st ON r.superior_code_hub = st.superior_code_hub
WHERE stu.latest_spx_tracking_code IN (8,9,33,15,630,36) -- inclui o 36 aqui
    AND stu.latest_spx_location LIKE 'SoC%'
    AND (stu.final_spx_station_name LIKE 'LM%' or stu.final_spx_station_name like 'XPT%')
    AND mx.max_sequence = stu.station_sequence
    AND sta.grass_date = (SELECT max_date FROM max_grass_date)
    AND sta.tz_type = 'local'
    AND sta.grass_region = 'BR'

GROUP BY
1,2,3
--caos total pq não vai
--cade