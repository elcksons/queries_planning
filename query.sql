WITH 
base_filtrada AS (
    SELECT
        CAST(station_id AS VARCHAR) AS station_id,
        shipment_id,
        data AS data_received,
        CAST(REPLACE(DATE_FORMAT(datetime_received, '%H:%i'), ':', '') AS INT) AS hora_received,
        datetime_received
    FROM dev_brbi_opslgc.di_hub_db_shipment_lm
    WHERE received_attempt = 1
      AND station_id = 5003
      AND data IN (DATE '2025-05-05', DATE '2025-05-06')
),

janela_processada AS (
    SELECT
        CAST(station_id AS VARCHAR) AS station_id,
        SUBSTRING(LPAD(labeling_start_time, 8, '0'), 1, 5) AS labeling_start_time,
        SUBSTRING(LPAD(labeling_end_time, 8, '0'), 1, 5) AS labeling_end_time
    FROM brbi_opslgc.ops_clock_planning_2025
    WHERE labeling_start_time IS NOT NULL AND labeling_start_time != ''
      AND labeling_end_time IS NOT NULL AND labeling_end_time != ''
),

janelas_expandidas AS (
    SELECT
        jp.station_id,
        jp.labeling_start_time,
        jp.labeling_end_time,
        CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) AS start_janela,
        CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) AS end_janela,
        ROW_NUMBER() OVER (
            PARTITION BY jp.station_id
            ORDER BY CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT)
        ) AS janela_num,
        CASE 
            WHEN CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) < 100 THEN
                CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) + 2300
            ELSE
                CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) - 100
        END AS inicio_expandido,
        CASE 
            WHEN CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) > 2359 THEN
                CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) - 2400
            ELSE
                CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) + 100
        END AS fim_expandido,
        LPAD(
            CAST(
                CASE 
                    WHEN CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) < 100 THEN
                        CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) + 2300
                    ELSE
                        CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) - 100
                END AS VARCHAR
            ), 
            4, 
            '0'
        ) AS horario_inicio_expandido_formatado,
        LPAD(
            CAST(
                CASE 
                    WHEN CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) > 2359 THEN
                        CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) - 2400
                    ELSE
                        CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) + 100
                END AS VARCHAR
            ), 
            4, 
            '0'
        ) AS horario_fim_expandido_formatado,
        CONCAT(REPLACE(jp.labeling_start_time, ':', ''), ' às ', REPLACE(jp.labeling_end_time, ':', '')) AS janela_programada
    FROM janela_processada jp
),

shipments_dentro_janela AS (
    SELECT
        bf.station_id,
        bf.shipment_id,
        bf.data_received AS data_base_janela,
        bf.datetime_received,
        j.janela_num,
        j.inicio_expandido,
        j.fim_expandido,
        j.start_janela,
        j.end_janela,
        CONCAT(j.horario_inicio_expandido_formatado, ' às ', j.horario_fim_expandido_formatado) AS janela_recebimento_expandida,
        j.janela_programada,
        CASE 
            -- Se a janela começa após meia-noite
            WHEN j.start_janela < 100 THEN
                CASE 
                    -- Se o shipment é após meia-noite, usa a data do shipment
                    WHEN HOUR(bf.datetime_received) < 100 THEN DATE(bf.datetime_received)
                    -- Se o shipment é antes de meia-noite, usa o dia anterior
                    ELSE DATE(bf.datetime_received) - INTERVAL '1' DAY
                END
            -- Se a janela começa antes de meia-noite
            WHEN j.start_janela > j.end_janela THEN
                CASE 
                    WHEN HOUR(bf.datetime_received) >= 21 THEN DATE(bf.datetime_received)
                    ELSE DATE(bf.datetime_received) - INTERVAL '1' DAY
                END
            ELSE DATE(bf.datetime_received)
        END AS data_janela
    FROM base_filtrada bf
    JOIN janelas_expandidas j
      ON bf.station_id = j.station_id
     AND (
            -- Janela normal (não cruza meia-noite)
            (j.start_janela <= j.end_janela 
             AND bf.hora_received BETWEEN j.inicio_expandido AND j.fim_expandido)
            OR
            -- Janela que cruza meia-noite
            (j.start_janela > j.end_janela 
             AND (
                 -- Shipment após início expandido
                 (bf.hora_received >= j.inicio_expandido AND bf.hora_received <= 2359)
                 OR 
                 -- Shipment antes do fim expandido
                 (bf.hora_received >= 0 AND bf.hora_received <= j.fim_expandido)
             ))
            OR
            -- Janela que começa após meia-noite
            (j.start_janela < 100
             AND (
                 -- Shipment após início expandido (período anterior à meia-noite)
                 (bf.hora_received >= j.inicio_expandido AND bf.hora_received <= 2359)
                 OR
                 -- Shipment antes do fim expandido (período após meia-noite)
                 (bf.hora_received >= 0 AND bf.hora_received <= j.fim_expandido)
             ))
     )
),

shipments_fora_janela AS (
    SELECT
        bf.station_id,
        bf.shipment_id,
        bf.data_received AS data_base_janela,
        bf.datetime_received,
        NULL AS janela_num,
        NULL AS inicio_expandido,
        NULL AS fim_expandido,
        NULL AS start_janela,
        NULL AS end_janela,
        'fora da janela' AS janela_recebimento_expandida,
        NULL AS janela_programada,
        NULL AS data_janela
    FROM base_filtrada bf
    WHERE NOT EXISTS (
        SELECT 1
        FROM janelas_expandidas j
        WHERE j.station_id = bf.station_id
          AND (
                -- Janela normal (não cruza meia-noite)
                (j.start_janela <= j.end_janela 
                 AND bf.hora_received BETWEEN j.inicio_expandido AND j.fim_expandido)
                OR
                -- Janela que cruza meia-noite
                (j.start_janela > j.end_janela 
                 AND (
                     -- Shipment após início expandido
                     (bf.hora_received >= j.inicio_expandido AND bf.hora_received <= 2359)
                     OR 
                     -- Shipment antes do fim expandido
                     (bf.hora_received >= 0 AND bf.hora_received <= j.fim_expandido)
                 ))
                OR
                -- Janela que começa após meia-noite
                (j.start_janela < 100
                 AND (
                     -- Shipment após início expandido (período anterior à meia-noite)
                     (bf.hora_received >= j.inicio_expandido AND bf.hora_received <= 2359)
                     OR
                     -- Shipment antes do fim expandido (período após meia-noite)
                     (bf.hora_received >= 0 AND bf.hora_received <= j.fim_expandido)
                 ))
          )
    )
),

todos_shipments AS (
    SELECT * FROM shipments_dentro_janela
    UNION ALL
    SELECT * FROM shipments_fora_janela
),

intervalos_60min AS (
    SELECT
        station_id,
        data_base_janela,
        shipment_id,
        datetime_received,
        janela_recebimento_expandida,
        janela_programada,
        inicio_expandido,
        fim_expandido,
        start_janela,
        end_janela,
        data_janela,
        HOUR(datetime_received) AS intervalo_chave
    FROM todos_shipments
),

agrupamento_completo AS (
    SELECT
        station_id,
        data_base_janela,
        janela_recebimento_expandida,
        janela_programada,
        intervalo_chave,
        MIN(datetime_received) AS datetime_received,
        COUNT(DISTINCT shipment_id) AS qtd_shipments_60min,
        MAX(shipment_id) AS shipment_id
    FROM intervalos_60min
    GROUP BY station_id, data_base_janela, janela_recebimento_expandida, janela_programada, intervalo_chave
),

totais_por_janela AS (
    SELECT
        station_id,
        data_base_janela,
        janela_recebimento_expandida,
        SUM(qtd_shipments_60min) AS total_da_janela
    FROM agrupamento_completo
    WHERE janela_recebimento_expandida <> 'fora da janela'
    GROUP BY station_id, data_base_janela, janela_recebimento_expandida
),

percentual_acumulado AS (
    SELECT
        a.station_id,
        a.data_base_janela,
        a.janela_recebimento_expandida,
        a.datetime_received,
        a.qtd_shipments_60min,
        SUM(a.qtd_shipments_60min) OVER (
            PARTITION BY a.station_id, a.data_base_janela, a.janela_recebimento_expandida
            ORDER BY a.datetime_received
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS acumulado,
        SUM(a.qtd_shipments_60min) OVER (
            PARTITION BY a.station_id, a.data_base_janela, a.janela_recebimento_expandida
        ) AS total
    FROM agrupamento_completo a
),

intervalo_percentil_80_base AS (
    SELECT
        station_id,
        data_base_janela,
        janela_recebimento_expandida,
        MIN(datetime_received) AS inicio_janela,
        MAX(datetime_received) AS fim_80_percent
    FROM percentual_acumulado
    WHERE acumulado <= 0.8 * total
    GROUP BY station_id, data_base_janela, janela_recebimento_expandida
),

primeiro_ultimo_recebimento AS (
    SELECT
        station_id,
        data_base_janela,
        janela_recebimento_expandida,
        MIN(datetime_received) AS primeiro_recebimento,
        MAX(datetime_received) AS ultimo_recebimento
    FROM todos_shipments
    WHERE janela_recebimento_expandida <> 'fora da janela'
    GROUP BY station_id, data_base_janela, janela_recebimento_expandida
),

operadores_por_shipment AS (
    SELECT
        datetime,
        CAST(station_id AS VARCHAR) AS station_id,
        shipment_id,
        HOUR(datetime) AS intervalo_chave,
        COUNT(DISTINCT operator_name) AS qtd_operadores,
        COUNT(DISTINCT shipment_id) AS total_shipments
    FROM dev_brbi_opslgc.di_hub_order_tracking_lm_volume
    WHERE bin_received_attempt = 1
    GROUP BY datetime, CAST(station_id AS VARCHAR), shipment_id, HOUR(datetime)
)

SELECT 
    a.station_id,
    a.data_base_janela,
    a.datetime_received,
    a.janela_recebimento_expandida,
    a.janela_programada,
    a.intervalo_chave,
    a.qtd_shipments_60min,
    t.total_da_janela,
    ROUND(100.0 * a.qtd_shipments_60min / t.total_da_janela, 2) AS percentual_na_janela,
    date_diff('minute', ip.inicio_janela, ip.fim_80_percent) AS duracao_80_percent_minutos,
    CONCAT(
        date_format(ip.inicio_janela, '%Y-%m-%d %H:%i'),
        ' até ',
        date_format(ip.fim_80_percent, '%Y-%m-%d %H:%i')
    ) AS intervalo_percentil_80,
    ROUND(COALESCE(o.total_shipments, 0) / NULLIF(o.qtd_operadores, 0), 2) AS produtividade
FROM agrupamento_completo a
LEFT JOIN totais_por_janela t
  ON a.station_id = t.station_id
 AND a.data_base_janela = t.data_base_janela
 AND a.janela_recebimento_expandida = t.janela_recebimento_expandida
LEFT JOIN intervalo_percentil_80_base ip
  ON a.station_id = ip.station_id
 AND a.data_base_janela = ip.data_base_janela
 AND a.janela_recebimento_expandida = ip.janela_recebimento_expandida
LEFT JOIN primeiro_ultimo_recebimento p
  ON a.station_id = p.station_id
 AND a.data_base_janela = p.data_base_janela
 AND a.janela_recebimento_expandida = p.janela_recebimento_expandida
LEFT JOIN operadores_por_shipment o
  ON a.shipment_id = o.shipment_id
 AND a.station_id = o.station_id
 AND a.intervalo_chave = o.intervalo_chave
WHERE ROUND(100.0 * a.qtd_shipments_60min / t.total_da_janela, 2) > 5
ORDER BY a.station_id, a.data_base_janela, a.datetime_received;
