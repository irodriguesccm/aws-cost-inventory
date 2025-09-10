# AWS Cost Inventory Script

Este repositório contém um script Python para coletar um inventário detalhado de recursos AWS, incluindo instâncias EC2, volumes EBS, buckets S3, funções Lambda, entre outros. O script utiliza a biblioteca `boto3` para interagir com os serviços da AWS e gera um arquivo JSON com os dados coletados.

## Pré-requisitos

Antes de executar o script, certifique-se de que você possui:

1. **Credenciais AWS configuradas**:
   - O script utiliza as credenciais configuradas no ambiente para acessar os serviços da AWS. Certifique-se de que as credenciais têm permissões suficientes para listar os recursos.

2. **Ambiente Python configurado**:
   - O script requer Python 3.6 ou superior.
   - As dependências estão listadas no arquivo `requirements.txt`.

3. **Acesso ao AWS CloudShell**:
   - O AWS CloudShell é um terminal baseado em navegador que já vem com o `boto3` e outras ferramentas pré-instaladas.

---

## Instruções de Execução no AWS CloudShell

Siga os passos abaixo para executar o script no AWS CloudShell:

### 1. Acesse o AWS CloudShell
- No Console da AWS, clique no ícone do **CloudShell** no canto superior direito.
- Aguarde a inicialização do terminal.

### 2. Clone o Repositório
No terminal do CloudShell, execute o comando abaixo para clonar este repositório:

```bash
git clone https://github.com/irodriguesccm/aws-cost-inventory.git
```

### 3. Acesse o diretório do projeto
Entre no diretório do projeto clonado
```bash
cd aws-cost-inventory
```

### 4. Instale as Dependências
Instale as dependências necessárias utilizando o pip:
```bash
pip install -r requirements.txt
```

### 5. Execute o Script
Para iniciar a coleta do inventário, execute o seguinte comando:
```bash
python aws_cost_inventory.py
```

### 6. Verifique o Resultado
Após a execução, o script gerará um arquivo chamado aws_cost_inventory.json no diretório atual. Este arquivo contém o inventário completo dos recursos AWS.
Para visualizar o conteúdo do arquivo, utilize o comando:
```bash
cat aws_cost_inventory.json | jq .
```

Nota: O jq já está pré-instalado no AWS CloudShell e facilita a visualização de arquivos JSON.

### 7. Realize o Download do JSON
Para baixar o arquivo JSON, execute o comando abaixo para verificar o diretório atual:
```bash
pwd
```

Será retornado algo como:
/home/cloudshell-user/aws-cost-inventory

Copie o diretório e execute o comando para obter o nome do arquivo JSON:
```bash
ls
```

Copie o nome do arquivo, será algo como:
aws_cost_inventory.json

Junte o ditetório atual + arquivo JSON conforme abaixo:
/home/cloudshell-user/aws-cost-inventory/aws_cost_inventory.json

Clique no botão “Actions” do CloudShell e em “Download file”.

Cole o nome completo do repositório + arquvo JSON e realize o download:
/home/cloudshell-user/aws-cost-inventory/aws_cost_inventory.json
