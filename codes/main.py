# Importar as bibliotecas

import pandas as pd
import numpy as np
import sqlite3
import re 

# Gerar os dados de normalização
dados = {
    'Nome' : ['João Silva', 'maria souza', 'CARLOS ALMEIRA', 'AnA PAuLA', '    pedro santos', 'lucas', np.nan, 'Beatriz', 'Marcos#@', ''],
    'Idade' : ['25', 'trinta', '45 anos', '22 anos', '38', 'Cinquenta', '27', '', '40', None],
    'Telefone' : ["(45) 99999-9999", "0988888-8888", "10-77777-7777", "12-66666-6666", "9355555-5555", "(12) 44444-4444", "(01) 33333-3333", "(72) 22222-2222", "(41) 11111-1111", "(12) 00000-0000"],
    'Endereço' : ['Rua A, 123', 'Rua B, 123', 'Rua C, 321', 'Rua Devorador de Almas, 456', 'Rua E. 777', 'Rua F, 886', 'Rua G, 254', 'Rua H, 25', 'Rua Integral, aaaaaaa', 'Sem endereço', ],
    'E-mail' : ['joao.silva@exemplo.com','maria_souza@exemplo.com','carlos@exemplo.com', 'anapaulinha@exemplo.com', 'pedroaaasantos@exemplo.com', 'luquinhasqueroquero@@exemplo.com', np.nan, 'biagamer123@exemplo.com', 'marquin@exemplo.com', 'email_inexistente']
}

#Criação do dataframe
df_desorganizado = pd.DataFrame(dados)
df_desorganizado.to_csv('dados_desorganizados.csv', index=False)

#Carregamento do nosso arquivo csv
df = pd.read_csv('dados_desorganizados.csv')

#Padronização dos nomes
df['Nome'] = df['Nome'].str.strip() #Tirar os espaços extras do começo e do final do nome
df['Nome'] = df['Nome'].str.title() #Primeira letra maiuscula
df['Nome'] = df['Nome'].replace('', np.nan) #Substituir o NaN na tabela



#Padronização das idades 
def extrair_idade(idade):
    if pd.isnull(idade):
        return np.nan
    idade = str(idade)
    numeros = re.findall(r'\d+', idade)
    if numeros:
        return int(numeros[0])
    else:
        return np.nan

df['Idade'] = df['Idade'].apply(extrair_idade)


#Remover e-mails duplicados
df = df.drop_duplicates(subset=["E-mail"])

#Validação dos e-mails
def validar_email(email):
    if pd.isnull(email):
        return np.nan
    padrao = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    if re.match(padrao, email):
        return email
    else:
        return np.nan
    
df["E-mail"] = df['E-mail'].apply(validar_email)

# Padronização do Telefone
def padronizar_telefone(telefone):
    if pd.isnull(telefone): 
        return np.nan
    telefone = re.sub(r'\D', '', telefone) #Remove todos os caracteres não numericos
    if len(telefone) == 11:
        return f'({telefone[:2]}) {telefone[2:7]}-{telefone[7:]}'
    else:
        return np.nan
    
df['Telefone'] = df['Telefone'].apply(padronizar_telefone)

#Padronização do endereço
df['Endereço'] = df['Endereço'].fillna("Não informado").str.strip()

#Remover os registros com Nome ou e-mail Nulos
df = df.dropna(subset=["Nome", "E-mail"])

df.reset_index(drop=True, inplace=True)

#Renomear as colunas (Header)

df.rename(columns= {
    'Nome' : 'nome',
    'Idade' : 'idade',
    'E-mail' : 'email',
    'Telefone' : 'telefone',
    'Endereço' : 'endereco'
}, inplace=True)

#Conectar com o banco / Criar o banco de dados
conn = sqlite3.connect('clientes.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS clientes(
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               nome TEXT NOT NULL,
               idade INTEGER,
               email TEXT NOT NULL,
               telefone TEXT,
               endereco TEXT)

'''
)

df.to_sql('clientes', conn, if_exists='append', index=False)

#Consulta dos dados inseridos
df_bd = pd.read_sql_query("SELECT * FROM clientes", conn)
print(df_bd.head(10))

#Fechar a conexão com o banco de dados 
conn.close()