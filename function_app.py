import logging
import azure.functions as func
import pandas as pd
import pyodbc
import requests
from io import StringIO
from datetime import datetime
import os

app = func.FunctionApp()

@app.schedule(
    schedule="0 0 19 * * *",  # todo dia às 19:00
    arg_name="myTimer",
    run_on_startup=False,     # True apenas se quiser testar na inicialização
    use_monitor=False
)
def IngestaoDiariaNVDA(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("A execução atrasou!")

    logging.info("Iniciando ingestão de dados da NVDA")

    try:
        # 1️⃣ Requisição da API
        url = (
            "https://www.alphavantage.co/query?"
            "function=TIME_SERIES_DAILY&symbol=NVDA&outputsize=compact&datatype=csv&apikey=TS6CPXKQF3EZ5BYM"
        )
        r = requests.get(url)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text))

        # 2️⃣ Tratamento de dados
        df.drop_duplicates(inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['time'] = df['timestamp'].dt.time
        df['ticket'] = "NVDA"

        # Último dia disponível
        ultimo_dia = df['timestamp'].max().date()

        # Ignorar fim de semana
        if ultimo_dia.weekday() >= 5:  # 5 = sábado, 6 = domingo
            logging.info("Hoje é fim de semana, não há dados de mercado.")
            return

        df = df[df['timestamp'].dt.date == ultimo_dia]
        df = df.drop(columns=['timestamp'])

        # 3️⃣ Conexão com SQL Server usando variáveis de ambiente
        server = os.environ['SQL_SERVER']
        database = os.environ['SQL_DATABASE']
        username = os.environ['SQL_USER']
        password = os.environ['SQL_PASSWORD']
        driver = '{ODBC Driver 18 for SQL Server}'

        conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};'
            f'UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        )
        cursor = conn.cursor()

        # 4️⃣ Inserção rápida no banco
        valores = [
            (
                row['ticket'],
                int(row['year']),
                int(row['month']),
                int(row['day']),
                row['timestamp'].strftime("%H:%M:%S"),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                float(row['volume'])
            )
            for _, row in df.iterrows()
        ]

        sql = """
        INSERT INTO NVDA (ticket, year, month, day, time, [open], [high], [low], [close], volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.fast_executemany = True
        cursor.executemany(sql, valores)
        conn.commit()

        logging.info(f"Ingestão concluída com sucesso! {len(valores)} linhas inseridas.")

    except Exception as e:
        logging.error(f"Erro durante a ingestão: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
