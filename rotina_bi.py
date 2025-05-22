import psycopg2
import csv
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import datetime
import time
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account

# Caminho para a pasta onde os arquivos CSV serão salvos            
pasta_saida = "C:\\caminho\\arquivos" 
text_email = ''
start_time = time.time()

def print_msg(msg):
    print(msg)
    global text_email
    text_email = f'{text_email}{msg}\n'

def email(text_email,sub):

    titulo = sub
    frase = text_email.replace('\n', '<br>')
    
    me = 'e-mail'
    you = ['e-mail']
    msg = MIMEMultipart('alternative')
    msg['Subject'] = titulo
    msg['From'] = 'e-mail' 
    msg['To'] = 'e-mail'
    text = str(frase)
    html = f"""\
    <html>
    <head></head>
    <body>
        <font face="Courier New, Courier, monospace">{frase}<br>
        </font>
    </body>
    </html>
    """
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    msg.attach(part1)
    msg.attach(part2)
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()
    mail.login('e-mail', 'senha')
    
    for email in you:
        mail.sendmail(me, email, msg.as_string())
    mail.quit()


def send_email(status, mensagem):

    subject = f"{status}"
    body = mensagem

    email(body, subject)

def conectar_banco():
    # Substitua as informações abaixo com os seus próprios detalhes de conexão ao PostgreSQL
    print_msg("--- Realizando a conexão com o banco de dados")
    connection_data = psycopg2.connect(host="ip", database="banco",user="user",password="senha",port = 0000)
    return connection_data

# Substituir os arquivos existentes com base nos novos (pasta origem e saída são as mesmas)
def substituir_arquivos(pasta_saida):
    start_time_replace_files = time.time()
    print_msg('--- Verificando e substituindo os arquivos existentes na pasta')
    
    # Percorre todos os arquivos da pasta de saída
    for arquivo in os.listdir(pasta_saida):
        caminho_arquivo = os.path.join(pasta_saida, arquivo)
        try:
            # Verificar se o arquivo é um arquivo regular (não uma pasta)
            if os.path.isfile(caminho_arquivo):
                # Remover o arquivo antigo para substituição (se necessário)
                os.unlink(caminho_arquivo)
                print_msg(f'>>> Arquivo {caminho_arquivo} substituído \n---> Tempo de execução total: {round(time.time() - start_time_replace_files, 2)} segundos.')
        except Exception as e:
            print_msg(f"\n!!! Erro ao substituir arquivo {caminho_arquivo}: {e}")

substituir_arquivos(pasta_saida)

# Função para acessar Google Sheets e salvar dados em .csv
def acessar_gsheets_e_salvar_csv(sheet_id, pasta_destino):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    json_file = "C:\\caminho\\Credentials\\credentials.json"
    credentials = service_account.Credentials.from_service_account_file(json_file)
    scoped_credentials = credentials.with_scopes(scopes)
    gc = gspread.authorize(scoped_credentials)
    
    # Lista com os nomes ou IDs das planilhas que você deseja acessar
    planilhas = ['gsheet1', 'gsheet2', 'gsheet3', 
                 'gsheet4', 'gsheet5',
                 'gsheet6', 'gsheet7'] 
    
    # Dicionário que mapeia os nomes das planilhas para os novos nomes de arquivos .csv
    nomes_arquivos = {
    'gsheet1': 'arquivo1.csv',
    'gsheet2': 'arquivo2.csv',
    'gsheet3': 'arquivo3.csv',
    'gsheet4': 'arquivo4.csv',
    'gsheet5': 'arquivo5.csv',
    'gsheet6': 'arquivo6.csv',
    'gsheet7': 'arquivo7.csv'
    }    

    # Iterar sobre cada planilha
    for nome_planilha in planilhas:
        try:
            # Abrir a planilha pelo nome
            planilha = gc.open(nome_planilha)
            # Acessar a primeira aba da planilha
            worksheet = planilha.get_worksheet(0)
            # Obter os dados da planilha
            dados = worksheet.get_all_values()
            
            # Definir o nome do arquivo .csv baseado no nome mapeado
            nome_arquivo_csv = os.path.join(pasta_destino, nomes_arquivos.get(nome_planilha, f"{nome_planilha}.csv"))
            
            # Salvar os dados no arquivo .csv
            with open(nome_arquivo_csv, mode='w', newline='', encoding='utf-8') as arquivo_csv:
                escritor_csv = csv.writer(arquivo_csv)
                escritor_csv.writerows(dados)
            
            print(f'Planilha {nome_planilha} processada e salva com sucesso como {nome_arquivo_csv}.')
        
        except Exception as e:
            print(f"Erro ao acessar a planilha {nome_planilha}: {e}")


def executar_consulta(connection_data, query, nome_arquivo):
    cursor = connection_data.cursor()
    start_time_create_csv = time.time()
    
    try:
        # Medir o tempo inicial
        #tempo_inicial = time.time()

        # Executar a consulta
        cursor.execute(query)

        # Obter os resultados da consulta
        resultados = cursor.fetchall()
        
        # Verificar se há resultados antes de salvar em um arquivo CSV
        if resultados:
         

            # Garantir que a pasta de saída exista
            if not os.path.exists(pasta_saida):
                os.makedirs(pasta_saida)

            # Caminho completo do arquivo CSV de saída
            caminho_arquivo = os.path.join(pasta_saida, f"{nome_arquivo}.csv")

            # Escrever os resultados no arquivo CSV
            with open(caminho_arquivo, 'w', newline='', encoding='utf-8') as arquivo_csv:
                escritor_csv = csv.writer(arquivo_csv)
                # Escrever o cabeçalho
                escritor_csv.writerow([desc[0] for desc in cursor.description])
                # Escrever os dados
                escritor_csv.writerows(resultados)

            # Medir o tempo final
            #tempo_final = time.time()

            # Calcular o tempo de execução
            #tempo_execucao = tempo_final - tempo_inicial
        print_msg(f'--- Consulta executada com sucesso!\nO arquivo atualizado foi salvo em: {caminho_arquivo} \n---> Tempo de execução total: {round(time.time() - start_time_create_csv,2)} segundos.')
        

    except Exception as exep:
        print_msg(f"\n!!! Erro ao executar a rotina: {exep}")


    finally:
        # Fechar o cursor, mas manter a conexão aberta para usar em consultas subsequentes
        cursor.close()


if __name__ == "__main__":
    # Substitua as consultas e os nomes dos arquivos conforme necessário
    consultas_e_arquivos = [
        {"consulta_sql": 
         
         """
    # consulta SQL
        """, 

        "nome_do_arquivo": "arquivo_1"},

        {"consulta_sql": 
         
               """
    # consulta SQL
        """, 

         "nome_do_arquivo": "arquivo_2"},

         {"consulta_sql": 
         
                """
    # consulta SQL
        """, 

         "nome_do_arquivo": "arquivo_3"},

         {"consulta_sql": 
         
               """
    # consulta SQL
        """, 

         "nome_do_arquivo": "arquivo_4"},

         {"consulta_sql": 
               """
    # consulta SQL
        """, 

         "nome_do_arquivo": "arquivo_5"},

         {"consulta_sql": 
         
               """
    # consulta SQL
        """, 

         "nome_do_arquivo": "arquivo_6"},

         {"consulta_sql": 
         
              """
    # consulta SQL
        """, 

         "nome_do_arquivo": "arquivo_7"},

        # Adicione mais consultas e nomes de arquivos conforme necessário
        #{"consulta_sql": "SELECT * FROM tabela;","nome_do_arquivo": "resultado_tabela"},
        
    ]

    # Inicializando a variável que irá armazenar as mensagens de erro ou sucesso

erro_tipo = None  # Inicializar erro_tipo para controlar o assunto do e-mail

try:
    print_msg("--- Iniciando Rotina")
    
    # Conectar ao banco de dados
    try:
        conexao = conectar_banco()
    except Exception as e:
        erro_tipo = 'banco'
        text_email += f"Erro ao conectar com o banco de dados: {e}\n"
        raise  # Levanta a exceção para ser capturada no try principal
    
    print_msg(f'\n--- Atualizando os arquivos existentes na pasta: {pasta_saida}')
    substituir_arquivos(pasta_saida)
    
    # Executar as consultas e salvar os resultados em arquivos CSV
    print_msg('\n--- Iniciando Consultas')
    for consulta_info in consultas_e_arquivos:
        consulta_sql = consulta_info["consulta_sql"]
        nome_do_arquivo = consulta_info["nome_do_arquivo"]
        print_msg(f'Arquivo: {nome_do_arquivo}')
        executar_consulta(conexao, consulta_sql, nome_do_arquivo)
    
    # Conexão com Google Sheets e salvar dados em CSV
    try:
        acessar_gsheets_e_salvar_csv('sheet_id', pasta_saida)
    except Exception as e:
        erro_tipo = 'gsheets'
        text_email += f"Erro ao acessar Google Sheets: {e}\n"
        raise  

    end_time = time.time()
    tempo_execucao = end_time - start_time
    print_msg(f'\nRotina executada com sucesso!\n---> Tempo de execução total: {tempo_execucao}')
    text_email += f'Rotina executada com sucesso! Tempo de execução total: {tempo_execucao}\n'

except Exception as e:
    print_msg(f'\nOcorreu um erro durante a execução da rotina: {e}')
    
# Definir o assunto do e-mail com base no tipo de erro
if erro_tipo == 'banco':
    send_email("ERRO --- Rotina BI Performance - Conexão Banco", text_email)
elif erro_tipo == 'gsheets':
    send_email("ERRO --- Rotina BI Performance - Conexão GSheets", text_email)
else:
    send_email("SUCESSO --- Rotina BI Performance", text_email)