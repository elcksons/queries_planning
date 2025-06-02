import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Ler o arquivo Excel
# (Ajuste o nome do arquivo se necessário)
df = pd.read_excel('Temp Query 4_20250515-215533.xlsx')

# Converter colunas de data/hora
if 'datetime_received' in df.columns:
    df['datetime_received'] = pd.to_datetime(df['datetime_received'])
if 'data_received' in df.columns:
    df['data_received'] = pd.to_datetime(df['data_received'])

# Estatísticas básicas
total_recebimentos = len(df)
remessas_unicas = df['shipment_id'].nunique()
estacoes = df['station_id'].nunique()
print("\n=== Estatísticas Básicas ===")
print(f"Total de recebimentos registrados: {total_recebimentos}")
print(f"Remessas únicas (shipment_id): {remessas_unicas}")
print(f"Número de estações: {estacoes}")

# Análise de janelas de recebimento
print("\n=== Análise de Janelas de Recebimento ===")
if 'janela_num' in df.columns:
    janela_counts = df['janela_num'].value_counts().sort_index()
    print("\nRecebimentos por janela:")
    print(janela_counts)
else:
    janela_counts = None

# Status dos recebimentos
print("\n=== Status dos Recebimentos ===")
status_col = 'statua_recebimento' if 'statua_recebimento' in df.columns else 'status_entrega'
status_counts = df[status_col].value_counts()
print("\nRecebimentos por status:")
print(status_counts)

# Criar tabela com dados e percentual por status de recebimento
status_percent = (status_counts / total_recebimentos) * 100
status_table = pd.DataFrame({
    'Status': status_counts.index,
    'Quantidade': status_counts.values,
    'Percentual (%)': status_percent.values
})
print("\nTabela de Recebimentos por Status:")
print(status_table)

# Taxa de recebimentos no prazo (dentro da janela)
no_prazo = df[df[status_col].str.contains('Dentro', case=False, na=False)].shape[0]
taxa_no_prazo = (no_prazo / total_recebimentos) * 100
print(f"\nTaxa de recebimentos no prazo: {taxa_no_prazo:.2f}%")

# Análise por horário
print("\n=== Análise por Horário ===")
if 'datetime_received' in df.columns:
    df['hora'] = df['datetime_received'].dt.hour
    hourly_counts = df['hora'].value_counts().sort_index()
    print("\nRecebimentos por hora:")
    print(hourly_counts)
else:
    hourly_counts = None

# Visualizações
plt.figure(figsize=(15, 10))

# Gráfico 1: Distribuição dos Status dos Recebimentos
plt.subplot(2, 2, 1)
status_counts.plot(kind='bar', color='skyblue')
plt.title('Distribuição dos Status dos Recebimentos')
plt.xticks(rotation=45)
plt.tight_layout()

# Gráfico 2: Recebimentos por Hora
if hourly_counts is not None:
    plt.subplot(2, 2, 2)
    hourly_counts.plot(kind='line', marker='o', color='orange')
    plt.title('Recebimentos por Hora do Dia')
    plt.xlabel('Hora do Dia')
    plt.ylabel('Quantidade de Recebimentos')
    plt.grid(True)

# Gráfico 3: Distribuição por Janela de Recebimento
if janela_counts is not None:
    plt.subplot(2, 2, 3)
    janela_counts.plot(kind='pie', autopct='%1.1f%%')
    plt.title('Percentual de Recebimentos por Janela (em relação ao total do dia)')

# Gráfico 4: Tabela de Recebimentos por Status
plt.subplot(2, 2, 4)
status_table.plot(kind='bar', x='Status', y='Quantidade', color='green', ax=plt.gca())
plt.title('Quantidade de Recebimentos por Status')
plt.xticks(rotation=45)
plt.tight_layout()

# Salvar os gráficos
plt.tight_layout()
plt.savefig('recebimento_insights.png')
plt.close()

# Insights adicionais
print("\n=== Insights Adicionais ===")
if 'datetime_received' in df.columns:
    print(f"Primeiro recebimento registrado: {df['datetime_received'].min()}")
    print(f"Último recebimento registrado: {df['datetime_received'].max()}")
    print(f"Média de recebimentos por hora: {total_recebimentos / 24:.2f}")

# Percentual de recebimentos fora da janela
fora_janela = df[df[status_col].str.contains('Fora', case=False, na=False)].shape[0]
taxa_fora_janela = (fora_janela / total_recebimentos) * 100
print(f"Percentual de recebimentos fora da janela: {taxa_fora_janela:.2f}%") 