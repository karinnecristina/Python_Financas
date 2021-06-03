## Bibliotecas
import datetime
import time
import yfinance as yf

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

# Remodelando o dataframe (tratando o problema de multi-index)
# Padronizando o nome das colunas (não é indicado ter espaços nos nomes)    
df = df.stack().reset_index().rename(columns={'level_1':'Symbol','Adj Close':'Adj_Close'})

# Organizando a ordem das coulnas
df = df[['Date','Symbol','Open','High','Low','Close','Adj_Close','Volume']]

# Formatando os valores (adicionando o separador de milhar e a quantidade de casas decimais)
df.iloc[:,2:8] = df.iloc[:,2:8].applymap('{0:,.2f}'.format)

# Convertendo o formato da data de (ano-mês-dia) para (dia-mês-ano)
df['Date'] = df['Date'].apply(lambda x: x.strftime('%d-%m-%Y'))

# Visualizando o resultado (as 5 primeiras linhas):
print(df[:5])