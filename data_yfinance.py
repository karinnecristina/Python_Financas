# %%
# Bibliotecas
import datetime
import time
import yfinance as yf
# %%
# Coletando dados diários da API do yahoo e armazenando em um dataframe pandas

## Parâmetros
ticker = ['PG'] # Procter & Gamble
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

## Padronizando o nome das colunas (não é indicado ter espaços nos nomes) 
df = df.rename(columns={'Adj Close':'Adj_Close'}).reset_index()

## Visualizando o resultado:
df.head()