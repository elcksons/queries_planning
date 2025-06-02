# Análise Detalhada da Query de Análise de Shipments

## Visão Geral
Esta query foi desenvolvida para analisar o fluxo de shipments em uma estação específica (station_id = 5003), considerando janelas de tempo programadas e expandidas. A query utiliza Common Table Expressions (CTEs) para organizar e processar os dados de forma estruturada.

## Estrutura da Query

### 1. Base Filtrada (base_filtrada)
```sql
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
)
```
- Filtra os dados da tabela `di_hub_db_shipment_lm`
- Considera apenas tentativas de recebimento bem-sucedidas (received_attempt = 1)
- Foca na estação específica (station_id = 5003)
- Converte o horário de recebimento para um formato numérico (HHMM)

### 2. Janela Processada (janela_processada)
```sql
janela_processada AS (
    SELECT
        CAST(station_id AS VARCHAR) AS station_id,
        SUBSTRING(LPAD(labeling_start_time, 8, '0'), 1, 5) AS labeling_start_time,
        SUBSTRING(LPAD(labeling_end_time, 8, '0'), 1, 5) AS labeling_end_time
    FROM brbi_opslgc.ops_clock_planning_2025
    WHERE labeling_start_time IS NOT NULL AND labeling_start_time != ''
      AND labeling_end_time IS NOT NULL AND labeling_end_time != ''
)
```
- Obtém as janelas de tempo programadas da tabela `ops_clock_planning_2025`
- Formata os horários de início e fim das janelas
- Remove registros com horários inválidos

### 3. Janelas Expandidas (janelas_expandidas)
```sql
janelas_expandidas AS (
    SELECT
        jp.station_id,
        CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT) AS start_janela,
        CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT) AS end_janela,
        ROW_NUMBER() OVER (
            PARTITION BY jp.station_id
            ORDER BY CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT)
        ) AS janela_num,
        -- Ajustando para 1 hora antes
        (CAST(REPLACE(jp.labeling_start_time, ':', '') AS INT)) - 100 AS inicio_expandido,
        -- Ajustando para 1 hora depois
        (CAST(REPLACE(jp.labeling_end_time, ':', '') AS INT)) + 100 AS fim_expandido,
        -- Formatando horários expandidos e programados
        ...
    FROM janela_processada jp
)
```
- Expande as janelas de tempo em 1 hora antes e depois
- Numera as janelas sequencialmente
- Formata os horários para exibição

### 4. Intervalos de 30 Minutos (intervalos_30min_expandidos e intervalos_30min_programados)
```sql
intervalos_30min_expandidos AS (
    SELECT 
        j.station_id,
        j.janela_num,
        ...
        -- Gerando intervalos de 30 minutos
        seq AS intervalo_num,
        -- Calculando início e fim de cada intervalo
        ...
    FROM janelas_expandidas j
    CROSS JOIN (
        SELECT seq
        FROM (
            SELECT 0 AS seq UNION ALL SELECT 1 UNION ALL SELECT 2 ...
        ) numbers
    ) n
    WHERE ...
)
```
- Divide cada janela em intervalos de 30 minutos
- Considera casos especiais de janelas que cruzam a meia-noite
- Gera 24 intervalos possíveis (0-23)

### 5. Cálculo de Totais (totais_janela)
```sql
totais_janela AS (
    SELECT
        station_id,
        data_received,
        janela_recebimento_expandida,
        janela_programada,
        COUNT(DISTINCT shipment_id) AS total_janela
    FROM ...
    GROUP BY ...
)
```
- Calcula o total de shipments por janela
- Considera tanto janelas expandidas quanto programadas

### 6. Shipments por Intervalo (shipments_por_intervalo_expandido e shipments_por_intervalo_programado)
```sql
shipments_por_intervalo_expandido AS (
    SELECT
        bf.station_id,
        bf.data_received,
        ...
        COUNT(DISTINCT bf.shipment_id) AS qtd_shipments_intervalo_expandido
    FROM base_filtrada bf
    JOIN intervalos_30min_expandidos i
    ...
    GROUP BY ...
)
```
- Contabiliza shipments por intervalo de 30 minutos
- Considera tanto janelas expandidas quanto programadas

### 7. Shipments Fora das Janelas (shipments_fora_janelas)
```sql
shipments_fora_janelas AS (
    SELECT
        bf.station_id,
        bf.data_received,
        'fora das janelas' AS janela_recebimento_expandida,
        ...
    FROM base_filtrada bf
    LEFT JOIN janelas_expandidas j
    ...
    WHERE j.station_id IS NULL
    GROUP BY ...
)
```
- Identifica shipments que não se encaixam em nenhuma janela
- Mantém o controle desses casos especiais

## Resultado Final
A query final combina todos os resultados e calcula:
- Quantidade de shipments por intervalo
- Percentual do total para cada intervalo
- Ordenação por estação, data e janela
- Tratamento especial para shipments fora das janelas

## Considerações Importantes
1. A query lida com diferentes formatos de horário
2. Considera casos especiais de janelas que cruzam a meia-noite
3. Expande as janelas em 1 hora para análise mais abrangente
4. Mantém rastreabilidade de shipments fora das janelas programadas

## Uso da Query
Esta query é útil para:
- Análise de distribuição de shipments ao longo do dia
- Identificação de picos de atividade
- Verificação de aderência ao planejamento
- Análise de eficiência operacional 