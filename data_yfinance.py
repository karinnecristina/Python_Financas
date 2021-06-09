## Bibliotecas
import datetime
import os
import sqlalchemy
import time
import yfinance as yf
from dotenv import load_dotenv

## Parâmetros para conectar ao banco de dados:
load_dotenv()
connection = os.getenv('connection')

## Coletando dados diários da API do yahoo e armazenando em um dataframe pandas

# Parâmetros
ticker = ['PG','AAPL'] # Procter & Gamble e Apple
start_time = datetime.datetime(1995,1,1) # Data inicial
end_time = datetime.datetime.now().date().isoformat() # Data atual

connected = False
while not connected:
    try:
        df = yf.download(ticker, start_time, end_time) # Passando os parâmetros definidos anteriormente
        connected = True
        print(f'Conectado ao yahoo com sucesso.','\n')
    except Exception as error:
        print(f'Erro: ' + str(error))
        time.sleep(5)
        pass

def processing(df):
    '''Remodelando o dataframe'''
    df = df.stack().reset_index().rename(columns={'level_1':'Symbol','Adj Close':'Adj_Close'}) # tratando o problema de multi-index e padronizando o nome das colunas 
    df = df[['Date','Symbol','Open','High','Low','Close','Adj_Close','Volume']] # Organizando as colunas
    df.iloc[:,2:8] = df.iloc[:,2:8].applymap('{0:,.2f}'.format) # Formatando os valores (adicionando o separador de milhar e a quantidade de casas decimais)
    df['Date'] = df['Date'].apply(lambda x: x.strftime('%d-%m-%Y')) # Convertendo o formato da data de (ano-mês-dia) para (dia-mês-ano)
    return df

def save_database(df):
    '''Salvando os dados no banco'''
    engine = sqlalchemy.create_engine(connection) # Estabelecendo conexão com o banco
    return df.to_sql('tb_stock',
                      engine,
                      schema='tickers',
                      if_exists='replace',
                      index=False)

# Chamando as funcões    
df = processing(df)
database = save_database(df)