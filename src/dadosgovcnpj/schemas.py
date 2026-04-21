from pyspark.sql.types import StringType, StructField, StructType


EMPRESAS_COLUMNS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo_responsavel",
]

ESTABELECIMENTOS_COLUMNS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

SIMPLES_COLUMNS = [
    "cnpj_basico",
    "opcao_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
]

SOCIOS_COLUMNS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio_razao_social",
    "cpf_cnpj_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante_legal",
    "faixa_etaria",
]

LOOKUP_COLUMNS = ["codigo", "descricao"]
MUNICIPIOS_COLUMNS = ["codigo", "descricao"]

JUCEES_COLUMNS = [
    "nome_empresa",
    "nome_fantasia",
    "cnpj",
    "nire",
    "constituicao",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "municipio",
    "cep",
    "cod_natureza_juridica",
    "natureza_juridica",
    "atividade_principal",
]


def make_schema(columns: list[str]) -> StructType:
    return StructType([StructField(column, StringType(), True) for column in columns])
