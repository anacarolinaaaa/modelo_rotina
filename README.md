## Rotina de Extração e Atualização de Dados

Este script automatiza o processo de extração de dados de um banco PostgreSQL e de planilhas do Google Sheets, salvando os resultados em arquivos .csv. 
Ele também envia relatórios por e-mail informando o sucesso ou falha da rotina.

### Funcionalidades
* Conexão com banco de dados PostgreSQL e execução de múltiplas queries.
* Acesso a várias planilhas no Google Sheets via API.
* Substituição de arquivos antigos por novos .csv em uma pasta destino.
* Envio automático de e-mails com logs de execução (sucesso ou erro).
* Remoção de arquivos antigos antes de salvar novos.

### Pré-requisitos
* Python 3.7+
* Conta no Google Cloud com acesso ao Google Sheets API.
* Credenciais JSON de serviço do Google (Service Account).
* Conta de e-mail com permissão para envio via SMTP.

### Bibliotecas Utilizadas
```
psycopg2
csv
os
smtplib, email.mime
gspread
pandas
google.oauth2.service_account
datetime, time
```

### Estrutura Esperada
```
projeto/
│
├── credentials.json            # Credenciais do Google
├── seu_script.py               # Código principal
└── arquivos/                   # Pasta onde os arquivos CSV serão salvos
```

### Configurações

No script, altere:
* Caminho da pasta de saída (pasta_saida)
* Credenciais do banco de dados (host, database, user, password)
* E-mails de remetente e destinatário
* Nome das planilhas e seus respectivos arquivos .csv
* Caminho para o JSON de credenciais da conta de serviço do Google
* Queries para gerar as bases de dados

### Como Executar
```
python seu_script.py
```

### Personalização
* Adicione quantas queries SQL quiser na lista consultas_e_arquivos.
* Mapeie novas planilhas do Google adicionando no dicionário nomes_arquivos.
