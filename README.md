# dadosgovcnpj

Pipeline em PySpark para montar uma base local do CNPJ focada no Espirito Santo, enriquecida com tabelas auxiliares oficiais e exportada em CSV unico para uso no Power BI.

## O que o projeto faz

- descobre automaticamente a versao mensal mais recente da base aberta do CNPJ na Receita Federal
- baixa apenas os arquivos necessarios para o pipeline
- extrai e valida os arquivos baixados
- filtra os estabelecimentos do Espirito Santo
- junta dados de empresa, estabelecimento, CNAE, natureza juridica, municipio e opcao por Simples/MEI
- enriquece com a base aberta da JUCEES
- gera um CSV final unico
- opcionalmente gera uma base separada de socios do ES
- remove os arquivos pesados ao final para nao deixar o ambiente inchado

## Estrutura do projeto

```text
.
|-- main.py
|-- requirements.txt
|-- scripts
|   `-- bootstrap_ubuntu.sh
`-- src
    `-- dadosgovcnpj
        |-- __init__.py
        |-- config.py
        |-- io_utils.py
        |-- pipeline.py
        `-- schemas.py
```

## Requisitos para Linux

As instrucoes abaixo assumem Ubuntu ou Debian recem-instalado.

### 1. Atualizar o sistema

```bash
sudo apt update
sudo apt upgrade -y
```

### 2. Instalar dependencias de sistema

```bash
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jre-headless unzip curl
```

### 3. Clonar o projeto

```bash
git clone https://github.com/luizcarlospassamani/dadosgovcnpj.git
cd dadosgovcnpj
```

### 4. Criar ambiente virtual e instalar bibliotecas Python

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 5. Configurar Java para o Spark

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

Se quiser persistir isso no usuario atual:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

## Execucao por etapas

### Descobrir a versao mensal mais recente

```bash
python3 main.py discover-release
```

Com token dinamico da Receita:

```bash
python3 main.py discover-release --receita-share-token gn672Ad4CF8N6TK
```

Primeiro teste leve:

```bash
python3 main.py all --state ES --cleanup --test-mode --receita-share-token gn672Ad4CF8N6TK
```

### Baixar os dados

```bash
python3 main.py download --state ES
```

### Extrair os arquivos zip

```bash
python3 main.py extract --state ES
```

### Validar os insumos baixados

```bash
python3 main.py validate --state ES
```

### Construir a base final

```bash
python3 main.py build-final --state ES
```

### Construir tambem a base de socios do ES

```bash
python3 main.py build-final --state ES --include-socios
```

### Limpar arquivos pesados apos o processamento

```bash
python3 main.py cleanup --state ES
```

## Execucao completa

Sem socios:

```bash
python3 main.py all --state ES --cleanup
```

Com socios:

```bash
python3 main.py all --state ES --include-socios --cleanup
```

Usando o compartilhamento atual da Receita com token dinamico:

```bash
python3 main.py all --state ES --cleanup --receita-share-token gn672Ad4CF8N6TK
```

Usando modo de teste para baixar so 1 arquivo de Empresas e 1 de Estabelecimentos:

```bash
python3 main.py all --state ES --cleanup --test-mode --receita-share-token gn672Ad4CF8N6TK
```

## Saidas esperadas

Os arquivos finais ficam em `data/output/`.

- `base_cnpj_es_enriquecida.csv`
- `socios_es.csv` quando `--include-socios` for usado

## Como o pipeline funciona

### `discover-release`

Le o indice oficial da Receita e identifica a pasta mensal mais recente.

Quando `--receita-share-token` e informado, o projeto usa o compartilhamento publico da Receita via WebDAV para descobrir a release mais recente e listar os arquivos `.zip`.

### `download`

Baixa:

- `Empresas*.zip`
- `Estabelecimentos*.zip`
- `Simples.zip`
- `Cnaes.zip`
- `Naturezas.zip`
- `Municipios.zip`
- CSV da JUCEES
- opcionalmente `Socios*.zip`

### `extract`

Extrai os zips para `data/extracted/`.

### `validate`

Verifica:

- se os arquivos esperados existem
- se os zips estao integros
- se os CSVs principais conseguem ser lidos pelo Spark
- se o filtro do estado alvo retorna registros

### `build-final`

Monta a base final do ES com:

- dados da empresa
- dados do estabelecimento
- CNAE principal com descricao
- natureza juridica com descricao
- municipio com descricao
- flags e datas de Simples e MEI
- enriquecimento JUCEES

### `cleanup`

Remove `data/raw/` e `data/extracted/`, preservando apenas `data/output/`.

## Observacoes importantes

- O volume da Receita e grande. O pipeline foi desenhado para ser simples e local, mas ainda assim vai exigir espaco em disco e tempo de processamento.
- A exportacao final gera um CSV unico para facilitar a carga no Power BI.
- A base da JUCEES nao cobre todo o universo do CNPJ; ela entra como enriquecimento estadual adicional.
- Se o token mudar no futuro, basta executar novamente com um novo valor em `--receita-share-token`.
- Se a Receita mudar o diretorio compartilhado, voce tambem pode sobrescrever o caminho com `--receita-share-dir`.
- Para a primeira execucao, `--test-mode` reduz o volume e baixa apenas 1 arquivo de Empresas e 1 de Estabelecimentos, mantendo os arquivos auxiliares.

## Comando recomendado em maquina Linux nova

Depois do clone e da instalacao:

```bash
python3 main.py all --state ES --cleanup
```

Se quiser incluir socios:

```bash
python3 main.py all --state ES --include-socios --cleanup
```

Se a Receita estiver usando o compartilhamento SERPRO+ com token publico:

```bash
python3 main.py all --state ES --cleanup --receita-share-token gn672Ad4CF8N6TK
```

Para um primeiro teste mais rapido:

```bash
python3 main.py all --state ES --cleanup --test-mode --receita-share-token gn672Ad4CF8N6TK
```
