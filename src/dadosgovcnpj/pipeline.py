from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dadosgovcnpj.config import PipelineConfig
from dadosgovcnpj.io_utils import (
    cleanup_intermediate_data,
    configure_logging,
    discover_latest_release,
    download_inputs,
    extract_archives,
    flatten_files,
    persist_release,
    resolve_release,
    validate_zip_integrity,
)
from dadosgovcnpj.schemas import (
    EMPRESAS_COLUMNS,
    ESTABELECIMENTOS_COLUMNS,
    JUCEES_COLUMNS,
    LOOKUP_COLUMNS,
    MUNICIPIOS_COLUMNS,
    SIMPLES_COLUMNS,
    SOCIOS_COLUMNS,
    make_schema,
)


LOGGER = logging.getLogger(__name__)


def build_spark_session(app_name: str) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def _read_csv(
    spark: SparkSession,
    paths: list[str],
    schema_columns: list[str],
    delimiter: str = ";",
    encoding: str = "ISO-8859-1",
    header: bool = False,
) -> DataFrame:
    if not paths:
        raise RuntimeError("Nenhum arquivo encontrado para leitura.")
    return (
        spark.read.option("delimiter", delimiter)
        .option("header", str(header).lower())
        .option("encoding", encoding)
        .schema(make_schema(schema_columns))
        .csv(paths)
    )


def _read_jucees_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("delimiter", ",")
        .option("header", "true")
        .option("encoding", "utf-8")
        .schema(make_schema(JUCEES_COLUMNS))
        .csv(path)
    )


def _format_cnpj() -> F.Column:
    return F.concat_ws("", F.col("cnpj_basico"), F.col("cnpj_ordem"), F.col("cnpj_dv"))


def run_discover_release(config: PipelineConfig) -> None:
    release = discover_latest_release(config)
    persist_release(config, release)
    LOGGER.info("Release mais recente identificada: %s", release)


def run_download(config: PipelineConfig) -> None:
    release = resolve_release(config)
    LOGGER.info("Usando release %s", release)
    downloaded = download_inputs(config)
    LOGGER.info("Download concluido com %s arquivo(s).", len(downloaded))


def run_extract(config: PipelineConfig) -> None:
    extracted = extract_archives(config)
    LOGGER.info("Extracao concluida com %s arquivo(s).", len(extracted))


def run_validate(config: PipelineConfig) -> None:
    validate_zip_integrity(config)
    spark = build_spark_session("dadosgovcnpj-validate")
    try:
        establishments = read_establishments(spark, config).filter(F.col("uf") == config.state)
        empresas = read_empresas(spark, config)
        cnaes = read_cnaes(spark, config)

        es_count = establishments.limit(1000).count()
        emp_count = empresas.limit(1000).count()
        cnaes_count = cnaes.count()
        if es_count == 0:
            raise RuntimeError(f"Nenhum estabelecimento encontrado para a UF {config.state}.")
        LOGGER.info(
            "Validacao concluida. Amostra estabelecimentos %s: %s | empresas(amostra): %s | cnaes: %s",
            config.state,
            es_count,
            emp_count,
            cnaes_count,
        )
    finally:
        spark.stop()


def read_empresas(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    paths = [str(path) for path in flatten_files(config.extracted_dir, "Empresas")]
    return _read_csv(spark, paths, EMPRESAS_COLUMNS)


def read_establishments(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    paths = [str(path) for path in flatten_files(config.extracted_dir, "Estabelecimentos")]
    return _read_csv(spark, paths, ESTABELECIMENTOS_COLUMNS)


def read_simples(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    paths = [str(path) for path in flatten_files(config.extracted_dir, "Simples")]
    return _read_csv(spark, paths, SIMPLES_COLUMNS)


def read_socios(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    paths = [str(path) for path in flatten_files(config.extracted_dir, "Socios")]
    return _read_csv(spark, paths, SOCIOS_COLUMNS)


def read_cnaes(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    paths = [str(path) for path in flatten_files(config.extracted_dir, "Cnaes")]
    return _read_csv(spark, paths, LOOKUP_COLUMNS)


def read_naturezas(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    paths = [str(path) for path in flatten_files(config.extracted_dir, "Naturezas")]
    return _read_csv(spark, paths, LOOKUP_COLUMNS)


def read_municipios(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    paths = [str(path) for path in flatten_files(config.extracted_dir, "Municipios")]
    return _read_csv(spark, paths, MUNICIPIOS_COLUMNS)


def read_jucees(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    path = str(config.raw_dir / "jucees_empresas_es.csv")
    return _read_jucees_csv(spark, path)


def build_final_dataset(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    establishments = (
        read_establishments(spark, config)
        .filter(F.col("uf") == config.state)
        .withColumn("cnpj", _format_cnpj())
    )

    empresas = read_empresas(spark, config)
    simples = read_simples(spark, config)
    cnaes = read_cnaes(spark, config).withColumnRenamed("codigo", "cnae_codigo").withColumnRenamed(
        "descricao", "cnae_descricao"
    )
    naturezas = read_naturezas(spark, config).withColumnRenamed(
        "codigo", "natureza_juridica_codigo"
    ).withColumnRenamed("descricao", "natureza_juridica_descricao")
    municipios = read_municipios(spark, config).withColumnRenamed("codigo", "municipio_codigo").withColumnRenamed(
        "descricao", "municipio_descricao"
    )
    jucees = (
        read_jucees(spark, config)
        .withColumn("cnpj", F.regexp_replace(F.col("cnpj"), r"\D", ""))
        .withColumnRenamed("nome_empresa", "jucees_nome_empresa")
        .withColumnRenamed("nome_fantasia", "jucees_nome_fantasia")
        .withColumnRenamed("atividade_principal", "jucees_atividade_principal")
        .withColumnRenamed("constituicao", "jucees_constituicao")
        .withColumnRenamed("nire", "jucees_nire")
        .withColumnRenamed("municipio", "jucees_municipio")
        .withColumnRenamed("natureza_juridica", "jucees_natureza_juridica")
    )

    final_df = (
        establishments.join(empresas, on="cnpj_basico", how="left")
        .join(simples, on="cnpj_basico", how="left")
        .join(
            cnaes,
            establishments["cnae_fiscal_principal"] == cnaes["cnae_codigo"],
            how="left",
        )
        .join(
            naturezas,
            empresas["natureza_juridica"] == naturezas["natureza_juridica_codigo"],
            how="left",
        )
        .join(
            municipios,
            establishments["municipio"] == municipios["municipio_codigo"],
            how="left",
        )
        .join(jucees, on="cnpj", how="left")
        .select(
            "cnpj",
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "razao_social",
            "nome_fantasia",
            "identificador_matriz_filial",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_descricao",
            "cnae_fiscal_secundaria",
            "natureza_juridica",
            "natureza_juridica_descricao",
            "porte_empresa",
            "capital_social",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio",
            "municipio_descricao",
            "ddd_1",
            "telefone_1",
            "ddd_2",
            "telefone_2",
            "ddd_fax",
            "fax",
            "correio_eletronico",
            "opcao_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
            "jucees_nire",
            "jucees_constituicao",
            "jucees_nome_empresa",
            "jucees_nome_fantasia",
            "jucees_municipio",
            "jucees_natureza_juridica",
            "jucees_atividade_principal",
        )
    )
    return final_df


def build_socios_dataset(spark: SparkSession, config: PipelineConfig, es_cnpjs: DataFrame) -> DataFrame:
    socios = read_socios(spark, config)
    return socios.join(es_cnpjs.select("cnpj_basico").distinct(), on="cnpj_basico", how="inner")


def write_single_csv(df: DataFrame, target_file: Path, tmp_dir: Path) -> None:
    temp_output = tmp_dir / f"{target_file.stem}_tmp"
    if temp_output.exists():
        for path in temp_output.iterdir():
            path.unlink()
        temp_output.rmdir()

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("delimiter", ",")
        .csv(str(temp_output))
    )

    part_files = list(temp_output.glob("part-*.csv"))
    if not part_files:
        raise RuntimeError(f"Nao foi encontrado arquivo CSV em {temp_output}.")
    part_files[0].replace(target_file)
    for path in temp_output.iterdir():
        path.unlink(missing_ok=True)
    temp_output.rmdir()


def run_build_final(config: PipelineConfig) -> None:
    spark = build_spark_session("dadosgovcnpj-build-final")
    try:
        final_df = build_final_dataset(spark, config)
        final_path = config.output_dir / "base_cnpj_es_enriquecida.csv"
        write_single_csv(final_df, final_path, config.tmp_dir)
        LOGGER.info("Base final gerada em %s", final_path)

        if config.include_socios:
            es_cnpjs = read_establishments(spark, config).filter(F.col("uf") == config.state)
            socios_df = build_socios_dataset(spark, config, es_cnpjs)
            socios_path = config.output_dir / "socios_es.csv"
            write_single_csv(socios_df, socios_path, config.tmp_dir)
            LOGGER.info("Base de socios gerada em %s", socios_path)
    finally:
        spark.stop()


def run_cleanup(config: PipelineConfig) -> None:
    cleanup_intermediate_data(config)
    LOGGER.info("Limpeza concluida.")


def run_all(config: PipelineConfig) -> None:
    run_discover_release(config)
    run_download(config)
    run_extract(config)
    run_validate(config)
    run_build_final(config)
    if config.cleanup:
        run_cleanup(config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline local em PySpark para base CNPJ do ES.")
    parser.add_argument(
        "command",
        choices=["discover-release", "download", "extract", "validate", "build-final", "cleanup", "all"],
    )
    parser.add_argument("--state", default="ES", help="UF alvo para o recorte da base. Padrao: ES.")
    parser.add_argument("--release", default=None, help="Release mensal da Receita, ex.: 2026-01.")
    parser.add_argument(
        "--include-socios",
        action="store_true",
        help="Baixa e gera tambem uma base separada de socios do estado alvo.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove arquivos intermediarios pesados ao final.",
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    config = PipelineConfig(
        project_root=project_root,
        state=args.state.upper(),
        release=args.release,
        include_socios=args.include_socios,
        cleanup=args.cleanup,
    )
    config.ensure_directories()

    if args.command == "discover-release":
        run_discover_release(config)
    elif args.command == "download":
        run_download(config)
    elif args.command == "extract":
        run_extract(config)
    elif args.command == "validate":
        run_validate(config)
    elif args.command == "build-final":
        run_build_final(config)
    elif args.command == "cleanup":
        run_cleanup(config)
    elif args.command == "all":
        run_all(config)
    else:
        raise RuntimeError(f"Comando invalido: {args.command}")
