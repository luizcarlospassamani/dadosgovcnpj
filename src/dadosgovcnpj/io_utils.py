from __future__ import annotations

import logging
import re
import shutil
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests

from dadosgovcnpj.config import JUCEES_CSV_URL, PipelineConfig, RECEITA_INDEX_URL


LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def fetch_text(url: str, timeout: int = 120) -> str:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def discover_latest_release() -> str:
    html = fetch_text(RECEITA_INDEX_URL)
    matches = re.findall(r'href="(\d{4}-\d{2})/"', html)
    if not matches:
        raise RuntimeError("Nao foi possivel identificar a release mais recente da Receita.")
    return sorted(matches)[-1]


def persist_release(config: PipelineConfig, release: str) -> None:
    config.ensure_directories()
    config.release_file.write_text(release, encoding="utf-8")


def resolve_release(config: PipelineConfig) -> str:
    if config.release:
        return config.release
    if config.release_file.exists():
        return config.release_file.read_text(encoding="utf-8").strip()
    release = discover_latest_release()
    persist_release(config, release)
    return release


def receita_release_url(release: str) -> str:
    return urljoin(RECEITA_INDEX_URL, f"{release}/")


def list_remote_files(release: str) -> list[str]:
    html = fetch_text(receita_release_url(release))
    files = re.findall(r'href="([^"/][^"]+\.zip)"', html, flags=re.IGNORECASE)
    if not files:
        raise RuntimeError(f"Nenhum arquivo zip encontrado para a release {release}.")
    return sorted(set(files))


def select_files(remote_files: list[str], include_socios: bool) -> list[str]:
    selected: list[str] = []
    for name in remote_files:
        if re.match(r"Empresas\d+\.zip$", name):
            selected.append(name)
        elif re.match(r"Estabelecimentos\d+\.zip$", name):
            selected.append(name)
        elif name in {"Simples.zip", "Cnaes.zip", "Naturezas.zip", "Municipios.zip"}:
            selected.append(name)
        elif include_socios and re.match(r"Socios\d+\.zip$", name):
            selected.append(name)
    return sorted(selected)


def download_file(url: str, destination: Path) -> None:
    LOGGER.info("Baixando %s", url)
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def download_inputs(config: PipelineConfig) -> list[Path]:
    config.ensure_directories()
    release = resolve_release(config)
    remote_files = list_remote_files(release)
    selected_files = select_files(remote_files, include_socios=config.include_socios)
    base_url = receita_release_url(release)
    downloaded: list[Path] = []

    for file_name in selected_files:
        destination = config.raw_dir / file_name
        if not destination.exists():
            download_file(urljoin(base_url, file_name), destination)
        else:
            LOGGER.info("Arquivo ja existe, pulando download: %s", destination.name)
        downloaded.append(destination)

    jucees_destination = config.raw_dir / "jucees_empresas_es.csv"
    if not jucees_destination.exists():
        download_file(JUCEES_CSV_URL, jucees_destination)
    else:
        LOGGER.info("Arquivo ja existe, pulando download: %s", jucees_destination.name)
    downloaded.append(jucees_destination)
    persist_release(config, release)
    return downloaded


def extract_archives(config: PipelineConfig) -> list[Path]:
    config.ensure_directories()
    extracted_paths: list[Path] = []
    for zip_path in sorted(config.raw_dir.glob("*.zip")):
        target_dir = config.extracted_dir / zip_path.stem
        if target_dir.exists() and any(target_dir.iterdir()):
            LOGGER.info("Extracao ja existe, pulando: %s", zip_path.name)
            extracted_paths.extend(path for path in target_dir.iterdir() if path.is_file())
            continue

        target_dir.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Extraindo %s", zip_path.name)
        with zipfile.ZipFile(zip_path, "r") as zip_file:
            zip_file.extractall(target_dir)
        extracted_paths.extend(path for path in target_dir.iterdir() if path.is_file())
    return extracted_paths


def validate_zip_integrity(config: PipelineConfig) -> None:
    for zip_path in sorted(config.raw_dir.glob("*.zip")):
        with zipfile.ZipFile(zip_path, "r") as zip_file:
            bad_file = zip_file.testzip()
            if bad_file is not None:
                raise RuntimeError(f"Zip corrompido: {zip_path} / entrada: {bad_file}")
    LOGGER.info("Todos os arquivos zip passaram na validacao de integridade.")


def flatten_files(root: Path, prefix: str) -> list[Path]:
    files = []
    for directory in sorted(root.glob(f"{prefix}*")):
        if directory.is_dir():
            files.extend(sorted(path for path in directory.iterdir() if path.is_file()))
    return files


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink(missing_ok=True)


def cleanup_intermediate_data(config: PipelineConfig) -> None:
    LOGGER.info("Removendo arquivos intermediarios pesados.")
    remove_path(config.raw_dir)
    remove_path(config.extracted_dir)
    remove_path(config.tmp_dir)
