/* essa query está com a produtividade pelo datetime da curva removendo os outliers 
considerando apenas o que representa acima de 5% do total da janela */

WITH sort_codes AS (
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
    WHEN SUBSTRING(superior_station_code, LENGTH(superior_station_code), 1) = 'X'
    THEN SUBSTRING(superior_station_code, 1, 10)
    ELSE superior_station_code
    END
    ELSE station_code
    END AS superior_code_hub
FROM dev_brbi_opslgc.regional_lm
WHERE station_id NOT IN ('-', '?', '.', '', 'Inativo', 'Inactive')
),

base_filtrada AS (
    SELECT
        CAST(station_id AS VARCHAR) AS station_id,
        shipment_id,
        data AS data_received,
        CAST(REPLACE(date_format(datetime_received,'%H:%i'),':','') AS INT) AS hora_received,
        datetime_received
    FROM dev_brbi_opslgc.di_hub_db_shipment_lm
    WHERE received_attempt = 1
    AND data BETWEEN CURRENT_DATE - INTERVAL '30' DAY AND CURRENT_DATE - INTERVAL '1' DAY
),

operadores_janela AS (
    SELECT DISTINCT
        CAST(station_id AS VARCHAR) AS station_id,
        DATE(datetime) AS data_base_janela,
        datetime AS datetime_received,
        operator_name
    FROM dev_brbi_opslgc.di_hub_order_tracking_lm_volume
    WHERE bin_received_attempt = 1
),

janela_processada AS (
    SELECT DISTINCT
        CAST(station_id AS VARCHAR) AS station_id,
        SUBSTRING(LPAD(labeling_start_time,8,'0'),1,5) AS labeling_start_time,
        SUBSTRING(LPAD(labeling_end_time,8,'0'),1,5) AS labeling_end_time,
        SUBSTRING(LPAD(dispatching_start_time_fleet,8,'0'),1,5) AS dispatching_start_time,
        SUBSTRING(LPAD(dispatching_end_time_fleet,8,'0'),1,5) AS dispatching_end_time
    FROM brbi_opslgc.ops_clock_planning_2025
    WHERE (labeling_start_time IS NOT NULL AND labeling_start_time <> '')
      AND (labeling_end_time IS NOT NULL AND labeling_end_time <> '')
      AND (dispatching_start_time_fleet IS NOT NULL AND dispatching_start_time_fleet <> '')
      AND (dispatching_end_time_fleet IS NOT NULL AND dispatching_end_time_fleet <> '')
),

janelas_expandidas AS (
    SELECT
        jp.station_id,
        jp.labeling_start_time,
        jp.labeling_end_time,
        jp.dispatching_start_time,
        jp.dispatching_end_time,
        TRY_CAST(REPLACE(jp.labeling_start_time,':','') AS INT) AS start_janela,
        TRY_CAST(REPLACE(jp.labeling_end_time,':','') AS INT) AS end_janela,
        CASE
            WHEN TRY_CAST(REPLACE(jp.labeling_start_time,':','') AS INT) < 100 THEN 2330
            ELSE TRY_CAST(REPLACE(jp.labeling_start_time,':','') AS INT) - 100
        END AS inicio_expandido,
        CASE
            WHEN TRY_CAST(REPLACE(jp.labeling_end_time,':','') AS INT) > 2259 THEN 2359
            ELSE TRY_CAST(REPLACE(jp.labeling_end_time,':','') AS INT) + 100
        END AS fim_expandido,
        LPAD(
            CAST(
                CASE
                    WHEN TRY_CAST(REPLACE(jp.labeling_start_time,':','') AS INT) < 100 THEN 2330
                    ELSE TRY_CAST(REPLACE(jp.labeling_start_time,':','') AS INT) - 100
                END AS VARCHAR),4,'0') AS horario_inicio_expandido_formatado,
        LPAD(
            CAST(
                CASE
                    WHEN TRY_CAST(REPLACE(jp.labeling_end_time,':','') AS INT) > 2259 THEN 2359
                    ELSE TRY_CAST(REPLACE(jp.labeling_end_time,':','') AS INT) + 100
                END AS VARCHAR),4,'0') AS horario_fim_expandido_formatado,
        CONCAT(jp.labeling_start_time,' às ',jp.labeling_end_time) AS janela_programada_received,
        CONCAT(
            LPAD(
                CAST(
                    CASE
                        WHEN TRY_CAST(REPLACE(jp.labeling_start_time,':','') AS INT) < 100 THEN 2330
                        ELSE TRY_CAST(REPLACE(jp.labeling_start_time,':','') AS INT) - 100
                    END AS VARCHAR),4,'0'),
            ' às ',
            LPAD(
                CAST(
                    CASE
                        WHEN TRY_CAST(REPLACE(jp.labeling_end_time,':','') AS INT) > 2259 THEN 2359
                        ELSE TRY_CAST(REPLACE(jp.labeling_end_time,':','') AS INT) + 100
                    END AS VARCHAR),4,'0')
        ) AS janela_expandida_received,
        TRY_CAST(REPLACE(jp.dispatching_start_time,':','') AS INT) AS start_janela_dispatch,
        TRY_CAST(REPLACE(jp.dispatching_end_time,':','') AS INT) AS end_janela_dispatch,
        CASE
            WHEN TRY_CAST(REPLACE(jp.dispatching_start_time,':','') AS INT) < 100 THEN 2330
            ELSE TRY_CAST(REPLACE(jp.dispatching_start_time,':','') AS INT) - 100
        END AS inicio_expandido_dispatch,
        CASE
            WHEN TRY_CAST(REPLACE(jp.dispatching_end_time,':','') AS INT) > 2259 THEN 2359
            ELSE TRY_CAST(REPLACE(jp.dispatching_end_time,':','') AS INT) + 100
        END AS fim_expandido_dispatch,
        LPAD(
            CAST(
                CASE
                    WHEN TRY_CAST(REPLACE(jp.dispatching_start_time,':','') AS INT) < 100 THEN 2330
                    ELSE TRY_CAST(REPLACE(jp.dispatching_start_time,':','') AS INT) - 100
                END AS VARCHAR),4,'0') AS horario_inicio_expandido_formatado_dispatch,
        LPAD(
            CAST(
                CASE
                    WHEN TRY_CAST(REPLACE(jp.dispatching_end_time,':','') AS INT) > 2259 THEN 2359
                    ELSE TRY_CAST(REPLACE(jp.dispatching_end_time,':','') AS INT) + 100
                END AS VARCHAR),4,'0') AS horario_fim_expandido_formatado_dispatch,
        CONCAT(jp.dispatching_start_time,' às ',jp.dispatching_end_time) AS janela_programada_dispatch,
        CONCAT(
            LPAD(
                CAST(
                    CASE
                        WHEN TRY_CAST(REPLACE(jp.dispatching_start_time,':','') AS INT) < 100 THEN 2330
                        ELSE TRY_CAST(REPLACE(jp.dispatching_start_time,':','') AS INT) - 100
                    END AS VARCHAR),4,'0'),
            ' às ',
            LPAD(
                CAST(
                    CASE
                        WHEN TRY_CAST(REPLACE(jp.dispatching_end_time,':','') AS INT) > 2259 THEN 2359
                        ELSE TRY_CAST(REPLACE(jp.dispatching_end_time,':','') AS INT) + 100
                    END AS VARCHAR),4,'0')
        ) AS janela_expandida_dispatch
    FROM janela_processada jp
),

shipments_classificados AS (
    SELECT
        bf.station_id,
        bf.shipment_id,
        bf.datetime_received,
        bf.data_received,
        bf.hora_received,
        j.inicio_expandido,
        j.fim_expandido,
        j.start_janela,
        j.end_janela,
        j.janela_programada_received,
        j.janela_expandida_received,
        CONCAT(j.horario_inicio_expandido_formatado, ' às ', j.horario_fim_expandido_formatado) AS janela_recebimento_expandida,
        CASE
            WHEN j.inicio_expandido > j.fim_expandido THEN
                CASE 
                    WHEN bf.hora_received >= j.inicio_expandido OR bf.hora_received <= j.fim_expandido THEN 1
                    ELSE 0
                END
            ELSE
                CASE 
                    WHEN bf.hora_received BETWEEN j.inicio_expandido AND j.fim_expandido THEN 1
                    ELSE 0
                END
        END AS flag_dentro,
        CASE
            WHEN j.inicio_expandido > j.fim_expandido THEN
                CASE
                    WHEN bf.hora_received >= j.inicio_expandido THEN DATE(bf.datetime_received)
                    WHEN bf.hora_received <= j.fim_expandido THEN DATE(bf.datetime_received) - INTERVAL '1' DAY
                END
            ELSE DATE(bf.datetime_received)
        END AS data_janela
    FROM base_filtrada bf
    JOIN janelas_expandidas j ON bf.station_id = j.station_id
),

shipments_dentro AS (
    SELECT
        station_id,
        shipment_id,
        data_janela AS data_base_janela,
        datetime_received,
        janela_recebimento_expandida,
        janela_programada_received,
        janela_expandida_received
    FROM shipments_classificados
    WHERE flag_dentro = 1
),

shipments_fora AS (
    SELECT
        bf.station_id,
        bf.shipment_id,
        bf.data_received AS data_base_janela,
        bf.datetime_received,
        'fora da janela' AS janela_recebimento_expandida,
        NULL AS janela_programada_received,
        NULL AS janela_expandida_received
    FROM base_filtrada bf
    LEFT JOIN shipments_dentro sd ON bf.shipment_id = sd.shipment_id
    WHERE sd.shipment_id IS NULL
),

todos_shipments AS (
    SELECT * FROM shipments_dentro
    UNION ALL
    SELECT * FROM shipments_fora
),

agrupamento_completo AS (
    SELECT
        station_id,
        data_base_janela,
        janela_recebimento_expandida,
        janela_programada_received,
        janela_expandida_received,
        HOUR(datetime_received) AS intervalo_chave,
        MIN(datetime_received) AS datetime_received,
        COUNT(DISTINCT shipment_id) AS qtd_shipments_60min
    FROM todos_shipments
    GROUP BY station_id, data_base_janela, janela_recebimento_expandida, janela_programada_received, janela_expandida_received, HOUR(datetime_received)
),

totais_por_janela AS (
    SELECT
        station_id,
        data_base_janela,
        janela_recebimento_expandida,
        janela_programada_received,
        janela_expandida_received,
        SUM(qtd_shipments_60min) AS total_da_janela_received
    FROM agrupamento_completo
    WHERE janela_recebimento_expandida <> 'fora da janela'
    GROUP BY station_id, data_base_janela, janela_recebimento_expandida, janela_programada_received, janela_expandida_received
),

percentuais_janela AS (
    SELECT
        a.station_id,
        a.data_base_janela,
        a.janela_recebimento_expandida,
        a.janela_programada_received,
        a.janela_expandida_received,
        a.datetime_received,
        a.intervalo_chave,
        a.qtd_shipments_60min,
        t.total_da_janela_received,
        ROUND(100.0 * a.qtd_shipments_60min / t.total_da_janela_received, 2) AS percentual_janela
    FROM agrupamento_completo a
    JOIN totais_por_janela t
        ON a.station_id = t.station_id
        AND a.data_base_janela = t.data_base_janela
        AND a.janela_recebimento_expandida = t.janela_recebimento_expandida
    WHERE a.janela_recebimento_expandida <> 'fora da janela'
),

curva_5_percent AS (
    SELECT
        station_id,
        data_base_janela,
        janela_recebimento_expandida,
        janela_programada_received,
        janela_expandida_received,
        MIN(datetime_received) AS datetime_inicio_da_janela_real,
        MAX(datetime_received) AS hora_fim_curva_5_percent,
        date_diff('minute', MIN(datetime_received), MAX(datetime_received)) AS total_minutos_real_received,
        SUM(CASE WHEN percentual_janela > 5 THEN qtd_shipments_60min ELSE 0 END) AS total_da_curva_received,
        COUNT(DISTINCT intervalo_chave) AS total_intervalos_5_percent,
        SUM(qtd_shipments_60min) AS total_pacotes_5_percent,
        MIN(CASE WHEN percentual_janela > 5 THEN datetime_received END) AS datetime_inicio_curva_received,
        MAX(CASE WHEN percentual_janela > 5 THEN datetime_received END) AS datetime_fim_curva_received,
        date_diff('minute', 
            MIN(CASE WHEN percentual_janela > 5 THEN datetime_received END),
            MAX(CASE WHEN percentual_janela > 5 THEN datetime_received END)
        ) AS duracao_curva
    FROM percentuais_janela
    GROUP BY station_id, data_base_janela, janela_recebimento_expandida, janela_programada_received, janela_expandida_received
),

operadores_por_janela AS (
    SELECT
        p.station_id,
        p.data_base_janela,
        p.janela_recebimento_expandida,
        COUNT(DISTINCT o.operator_name) AS total_distinto_operadores
    FROM percentuais_janela p
    LEFT JOIN operadores_janela o
        ON p.station_id = o.station_id
        AND (
            -- Caso 1: Janela normal (mesmo dia)
            (o.data_base_janela = p.data_base_janela
            AND o.datetime_received >= CAST(p.data_base_janela AS TIMESTAMP) + INTERVAL '1' HOUR * CAST(REGEXP_EXTRACT(p.janela_programada_received, '^(\d{2}):', 1) AS INTEGER) + INTERVAL '1' MINUTE * CAST(REGEXP_EXTRACT(p.janela_programada_received, '^(\d{2}):(\d{2})', 2) AS INTEGER)
            AND o.datetime_received <= CAST(p.data_base_janela AS TIMESTAMP) + INTERVAL '1' HOUR * CAST(REGEXP_EXTRACT(p.janela_programada_received, '(\d{2}):\d{2}$', 1) AS INTEGER) + INTERVAL '1' MINUTE * CAST(REGEXP_EXTRACT(p.janela_programada_received, '(\d{2}):(\d{2})$', 2) AS INTEGER))
            OR
            -- Caso 2: Janela que passa para o dia seguinte
            (o.data_base_janela = p.data_base_janela + INTERVAL '1' DAY
            AND o.datetime_received <= CAST(p.data_base_janela + INTERVAL '1' DAY AS TIMESTAMP) + INTERVAL '1' HOUR * CAST(REGEXP_EXTRACT(p.janela_programada_received, '(\d{2}):\d{2}$', 1) AS INTEGER) + INTERVAL '1' MINUTE * CAST(REGEXP_EXTRACT(p.janela_programada_received, '(\d{2}):(\d{2})$', 2) AS INTEGER)
            AND CAST(REGEXP_EXTRACT(p.janela_programada_received, '^(\d{2}):', 1) AS INTEGER) > CAST(REGEXP_EXTRACT(p.janela_programada_received, '(\d{2}):\d{2}$', 1) AS INTEGER))
        )
    WHERE o.operator_name IS NOT NULL
    GROUP BY p.station_id, p.data_base_janela, p.janela_recebimento_expandida
),

base_filtrada_dispatch AS (
    SELECT
        CAST(station_id AS VARCHAR) AS station_id,
        shipment_id,
        data AS data_delivered,
        CAST(REPLACE(date_format(datetime_delivering,'%H:%i'),':','') AS INT) AS hora_delivered,
        datetime_delivering
    FROM dev_brbi_opslgc.di_hub_db_shipment_lm
    WHERE delivering_attempt = 1
),

shipments_classificados_dispatch AS (
    SELECT
        bf.station_id,
        bf.shipment_id,
        bf.datetime_delivering,
        bf.data_delivered,
        bf.hora_delivered,
        j.inicio_expandido AS inicio_expandido,
        j.fim_expandido AS fim_expandido,
        j.start_janela_dispatch AS start_janela_dispatch,
        j.end_janela_dispatch AS end_janela_dispatch,
        j.janela_programada_dispatch,
        j.janela_expandida_dispatch,
        CONCAT(j.horario_inicio_expandido_formatado, ' às ', j.horario_fim_expandido_formatado) AS janela_dispatch_expandida,
        CASE
            WHEN j.start_janela_dispatch <= j.end_janela_dispatch THEN
                CASE 
                    WHEN bf.hora_delivered >= j.start_janela_dispatch AND bf.hora_delivered <= j.end_janela_dispatch THEN 1
                    ELSE 0
                END
            WHEN j.start_janela_dispatch > j.end_janela_dispatch THEN
                CASE
                    WHEN bf.hora_delivered >= j.start_janela_dispatch OR bf.hora_delivered <= j.end_janela_dispatch THEN 1
                    ELSE 0
                END
            ELSE 0
        END AS flag_dentro,
        CASE
            WHEN j.start_janela_dispatch <= j.end_janela_dispatch THEN DATE(bf.datetime_delivering)
            WHEN j.start_janela_dispatch > j.end_janela_dispatch THEN
                CASE
                    WHEN bf.hora_delivered >= j.start_janela_dispatch THEN DATE(bf.datetime_delivering)
                    WHEN bf.hora_delivered <= j.end_janela_dispatch THEN DATE(bf.datetime_delivering) - INTERVAL '1' DAY
                    ELSE DATE(bf.datetime_delivering)
                END
            ELSE DATE(bf.datetime_delivering)
        END AS data_janela
    FROM base_filtrada_dispatch bf
    JOIN janelas_expandidas j ON bf.station_id = j.station_id
),

shipments_dentro_dispatch AS (
    SELECT
        station_id,
        shipment_id,
        data_janela AS data_base_janela,
        datetime_delivering,
        janela_dispatch_expandida,
        janela_programada_dispatch,
        janela_expandida_dispatch
    FROM shipments_classificados_dispatch
    WHERE flag_dentro = 1
),

intervalos_60min_dispatch AS (
    SELECT
        station_id,
        data_base_janela,
        shipment_id,
        datetime_delivering,
        janela_dispatch_expandida,
        janela_programada_dispatch,
        janela_expandida_dispatch,
        HOUR(datetime_delivering) AS intervalo_chave
    FROM shipments_dentro_dispatch
),

agrupamento_completo_dispatch AS (
    SELECT
        station_id,
        data_base_janela,
        janela_dispatch_expandida,
        janela_programada_dispatch,
        janela_expandida_dispatch,
        intervalo_chave,
        MIN(datetime_delivering) AS datetime_delivering,
        COUNT(DISTINCT shipment_id) AS qtd_shipments_dispatch_60min
    FROM intervalos_60min_dispatch
    GROUP BY station_id, data_base_janela, janela_dispatch_expandida, janela_programada_dispatch, janela_expandida_dispatch, intervalo_chave
),

totais_por_janela_dispatch AS (
    SELECT
        station_id,
        data_base_janela,
        janela_dispatch_expandida,
        janela_programada_dispatch,
        janela_expandida_dispatch,
        SUM(qtd_shipments_dispatch_60min) AS total_da_janela_dispatch
    FROM agrupamento_completo_dispatch
    WHERE janela_dispatch_expandida <> 'fora da janela'
    GROUP BY station_id, data_base_janela, janela_dispatch_expandida, janela_programada_dispatch, janela_expandida_dispatch
),

percentuais_janela_dispatch AS (
    SELECT
        a.station_id,
        a.data_base_janela,
        a.janela_dispatch_expandida,
        a.janela_programada_dispatch,
        a.janela_expandida_dispatch,
        a.datetime_delivering,
        a.intervalo_chave,
        a.qtd_shipments_dispatch_60min,
        t.total_da_janela_dispatch,
        ROUND(100.0 * a.qtd_shipments_dispatch_60min / t.total_da_janela_dispatch, 2) AS percentual_janela_dispatch
    FROM agrupamento_completo_dispatch a
    JOIN totais_por_janela_dispatch t
        ON a.station_id = t.station_id
        AND a.data_base_janela = t.data_base_janela
        AND a.janela_dispatch_expandida = t.janela_dispatch_expandida
    WHERE a.janela_dispatch_expandida <> 'fora da janela'
),

curva_5_percent_dispatch AS (
    SELECT
        station_id,
        data_base_janela,
        janela_dispatch_expandida,
        janela_programada_dispatch,
        janela_expandida_dispatch,
        MIN(CASE 
            WHEN CAST(REGEXP_EXTRACT(janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) > CAST(REGEXP_EXTRACT(janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) THEN
                CASE
                    WHEN HOUR(datetime_delivering) >= CAST(REGEXP_EXTRACT(janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) THEN datetime_delivering
                    ELSE datetime_delivering + INTERVAL '1' DAY
                END
            ELSE datetime_delivering
        END) AS datetime_inicio_real_dispatch,
        MAX(CASE 
            WHEN CAST(REGEXP_EXTRACT(janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) > CAST(REGEXP_EXTRACT(janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) THEN
                CASE
                    WHEN HOUR(datetime_delivering) <= CAST(REGEXP_EXTRACT(janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) THEN datetime_delivering
                    ELSE datetime_delivering - INTERVAL '1' DAY
                END
            ELSE datetime_delivering
        END) AS datetime_fim_real_dispatch,
        date_diff('minute', 
            MIN(CASE 
                WHEN CAST(REGEXP_EXTRACT(janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) > CAST(REGEXP_EXTRACT(janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) THEN
                    CASE
                        WHEN HOUR(datetime_delivering) >= CAST(REGEXP_EXTRACT(janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) THEN datetime_delivering
                        ELSE datetime_delivering + INTERVAL '1' DAY
                    END
                ELSE datetime_delivering
            END),
            MAX(CASE 
                WHEN CAST(REGEXP_EXTRACT(janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) > CAST(REGEXP_EXTRACT(janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) THEN
                    CASE
                        WHEN HOUR(datetime_delivering) <= CAST(REGEXP_EXTRACT(janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) THEN datetime_delivering
                        ELSE datetime_delivering - INTERVAL '1' DAY
                    END
                ELSE datetime_delivering
            END)
        ) AS total_minutos_real_dispatch,
        SUM(CASE WHEN percentual_janela_dispatch > 5 THEN qtd_shipments_dispatch_60min ELSE 0 END) AS total_da_curva_received_dispatch,
        COUNT(DISTINCT intervalo_chave) AS total_intervalos_5_percent_dispatch,
        SUM(qtd_shipments_dispatch_60min) AS total_pacotes_5_percent_dispatch,
        MIN(CASE WHEN percentual_janela_dispatch > 5 THEN datetime_delivering END) AS datetime_inicio_curva_dispatch,
        MAX(CASE WHEN percentual_janela_dispatch > 5 THEN datetime_delivering END) AS datetime_fim_curva_dispatch,
        date_diff('minute', 
            MIN(CASE WHEN percentual_janela_dispatch > 5 THEN datetime_delivering END),
            MAX(CASE WHEN percentual_janela_dispatch > 5 THEN datetime_delivering END)
        ) AS duracao_curva_dispatch
    FROM percentuais_janela_dispatch
    WHERE janela_dispatch_expandida <> 'fora da janela'
    GROUP BY station_id, data_base_janela, janela_dispatch_expandida, janela_programada_dispatch, janela_expandida_dispatch
)

SELECT DISTINCT
    p.station_id,
    sc.lm_regional,
    sc.sub_regional,
    sc.station_name,
    sc.code_name,
    p.data_base_janela,
    p.janela_programada_received,
    p.janela_expandida_received,
    p.total_da_janela_received,
    c.total_da_curva_received,
    date_format(c.datetime_inicio_da_janela_real, '%Y-%m-%d %H:%i') AS datetime_inicio_real_received,
    date_format(c.hora_fim_curva_5_percent, '%Y-%m-%d %H:%i') AS datetime_fim_real_received,
    c.total_minutos_real_received,
    ROUND(CAST(c.total_minutos_real_received AS DOUBLE) / 60, 2) AS duracao_horas_received,
    date_format(c.datetime_inicio_curva_received, '%Y-%m-%d %H:%i') AS datetime_inicio_curva_received,
    date_format(c.datetime_fim_curva_received, '%Y-%m-%d %H:%i') AS datetime_fim_curva_received,
    c.duracao_curva AS duracao_minutos_curva_received,
    ROUND(CAST(c.duracao_curva AS DOUBLE) / 60, 2) AS duracao_horas_curva_received,
    CASE 
        WHEN c.duracao_curva IS NOT NULL AND c.total_minutos_real_received IS NOT NULL 
        THEN c.duracao_curva - c.total_minutos_real_received 
        ELSE NULL 
    END AS diferenca_duracao_curva_received,
    -- Produtividade pela curva
    CAST(FLOOR((CAST(c.total_da_curva_received AS DOUBLE) / NULLIF(c.duracao_curva, 0)) * 60) AS INTEGER) AS produtividade_curva_received,
    o.total_distinto_operadores AS total_operadores_received,
    CAST(FLOOR(CAST(c.total_da_curva_received AS DOUBLE) / NULLIF(o.total_distinto_operadores, 0)) AS INTEGER) AS produtividade_por_operador_curva_received,
    pd.janela_programada_dispatch,
    pd.janela_expandida_dispatch,
    pd.total_da_janela_dispatch,
    cd.total_da_curva_received_dispatch AS total_da_curva_dispatch,
    date_format(cd.datetime_inicio_real_dispatch, '%Y-%m-%d %H:%i') AS datetime_inicio_real_dispatch,
    date_format(cd.datetime_fim_real_dispatch, '%Y-%m-%d %H:%i') AS datetime_fim_real_dispatch,
    cd.total_minutos_real_dispatch AS total_minutos_real_dispatch,
    ROUND(CAST(cd.total_minutos_real_dispatch AS DOUBLE) / 60, 2) AS duracao_horas_dispatch,
    date_format(cd.datetime_inicio_curva_dispatch, '%Y-%m-%d %H:%i') AS datetime_inicio_curva_dispatch,
    date_format(cd.datetime_fim_curva_dispatch, '%Y-%m-%d %H:%i') AS datetime_fim_curva_dispatch,
    cd.duracao_curva_dispatch,
    ROUND(CAST(cd.duracao_curva_dispatch AS DOUBLE) / 60, 2) AS duracao_horas_curva_dispatch,
    CASE 
        WHEN cd.duracao_curva_dispatch IS NOT NULL AND cd.total_minutos_real_dispatch IS NOT NULL 
        THEN cd.duracao_curva_dispatch - cd.total_minutos_real_dispatch 
        ELSE NULL 
    END AS diferenca_duracao_curva_dispatch,
    -- Produtividade pela curva
    CAST(FLOOR((CAST(cd.total_da_curva_received_dispatch AS DOUBLE) / NULLIF(cd.duracao_curva_dispatch, 0)) * 60) AS INTEGER) AS produtividade_curva_dispatch,
    (
        SELECT COUNT(DISTINCT o2.operator_name)
        FROM operadores_janela o2
        WHERE o2.station_id = pd.station_id
        AND o2.data_base_janela = pd.data_base_janela
        AND o2.datetime_received BETWEEN 
            CAST(pd.data_base_janela AS TIMESTAMP) + INTERVAL '1' HOUR * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) + INTERVAL '1' MINUTE * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '^(\d{2}):(\d{2})', 2) AS INTEGER)
            AND 
            CAST(pd.data_base_janela AS TIMESTAMP) + INTERVAL '1' HOUR * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) + INTERVAL '1' MINUTE * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '(\d{2}):(\d{2})$', 2) AS INTEGER)
    ) AS operadores_dispatch,
    CAST(FLOOR(CAST(cd.total_da_curva_received_dispatch AS DOUBLE) / NULLIF((
        SELECT COUNT(DISTINCT o2.operator_name)
        FROM operadores_janela o2
        WHERE o2.station_id = pd.station_id
        AND o2.data_base_janela = pd.data_base_janela
        AND o2.datetime_received BETWEEN 
            CAST(pd.data_base_janela AS TIMESTAMP) + INTERVAL '1' HOUR * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '^(\d{2}):', 1) AS INTEGER) + INTERVAL '1' MINUTE * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '^(\d{2}):(\d{2})', 2) AS INTEGER)
            AND 
            CAST(pd.data_base_janela AS TIMESTAMP) + INTERVAL '1' HOUR * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '(\d{2}):\d{2}$', 1) AS INTEGER) + INTERVAL '1' MINUTE * CAST(REGEXP_EXTRACT(pd.janela_programada_dispatch, '(\d{2}):(\d{2})$', 2) AS INTEGER)
    ), 0)) AS INTEGER) AS produtividade_por_operador_curva_dispatch
FROM percentuais_janela p
LEFT JOIN curva_5_percent c
    ON p.station_id = c.station_id
    AND p.data_base_janela = c.data_base_janela
    AND p.janela_recebimento_expandida = c.janela_recebimento_expandida
LEFT JOIN operadores_por_janela o
    ON p.station_id = o.station_id
    AND p.data_base_janela = o.data_base_janela
    AND p.janela_recebimento_expandida = o.janela_recebimento_expandida
LEFT JOIN janelas_expandidas je
    ON p.station_id = je.station_id
    AND p.janela_programada_received = je.janela_programada_received
LEFT JOIN percentuais_janela_dispatch pd
    ON je.station_id = pd.station_id
    AND p.data_base_janela = pd.data_base_janela
    AND je.janela_programada_dispatch = pd.janela_programada_dispatch
LEFT JOIN curva_5_percent_dispatch cd
    ON pd.station_id = cd.station_id
    AND pd.data_base_janela = cd.data_base_janela
    AND pd.janela_dispatch_expandida = cd.janela_dispatch_expandida
LEFT JOIN sort_codes sc
    ON p.station_id = CAST(sc.station_id AS VARCHAR)
WHERE p.janela_programada_received IS NOT NULL
GROUP BY 
    p.station_id,
    sc.lm_regional,
    sc.sub_regional,
    sc.station_name,
    sc.code_name,
    p.data_base_janela,
    p.janela_programada_received,
    p.janela_expandida_received,
    p.total_da_janela_received,
    c.total_da_curva_received,
    c.datetime_inicio_da_janela_real,
    c.hora_fim_curva_5_percent,
    c.total_minutos_real_received,
    c.datetime_inicio_curva_received,
    c.datetime_fim_curva_received,
    c.duracao_curva,
    o.total_distinto_operadores,
    pd.station_id,
    pd.data_base_janela,
    pd.janela_programada_dispatch,
    pd.janela_expandida_dispatch,
    pd.total_da_janela_dispatch,
    cd.total_da_curva_received_dispatch,
    cd.datetime_inicio_real_dispatch,
    cd.datetime_fim_real_dispatch,
    cd.total_minutos_real_dispatch,
    cd.datetime_inicio_curva_dispatch,
    cd.datetime_fim_curva_dispatch,
    cd.duracao_curva_dispatch,
    c.total_minutos_real_received
ORDER BY p.station_id, p.data_base_janela, p.janela_programada_received; 