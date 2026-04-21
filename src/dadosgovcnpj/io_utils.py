from __future__ import annotations

import logging
import re
import shutil
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote, urljoin

import requests

from dadosgovcnpj.config import (
    DEFAULT_RECEITA_SHARE_DIR,
    JUCEES_CSV_URL,
    PipelineConfig,
    RECEITA_INDEX_URLS,
    RECEITA_SHARE_BASE_URL,
)


LOGGER = logging.getLogger(__name__)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def fetch_text(url: str, timeout: int = 120) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _normalize_share_dir(path: str) -> str:
    normalized = path.strip() or DEFAULT_RECEITA_SHARE_DIR
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    return normalized.rstrip("/")


def _share_resource_url(config: PipelineConfig, *parts: str) -> str:
    if not config.receita_share_token:
        raise RuntimeError("Token da Receita nao informado para acesso via compartilhamento.")
    clean_parts = [part.strip("/") for part in parts if part and part.strip("/")]
    encoded_parts = "/".join(quote(part) for part in clean_parts)
    base = f"{RECEITA_SHARE_BASE_URL}/{quote(config.receita_share_token)}"
    return f"{base}/{encoded_parts}" if encoded_parts else base


def _propfind(url: str, depth: int = 1, timeout: int = 120) -> ET.Element:
    headers = {
        **DEFAULT_HEADERS,
        "Depth": str(depth),
        "Content-Type": "application/xml; charset=utf-8",
    }
    body = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<d:propfind xmlns:d=\"DAV:\">
  <d:prop>
    <d:displayname/>
    <d:resourcetype/>
  </d:prop>
</d:propfind>
"""
    response = requests.request("PROPFIND", url, headers=headers, data=body, timeout=timeout)
    response.raise_for_status()
    return ET.fromstring(response.text)


def _iter_dav_entries(root: ET.Element) -> list[dict[str, str | bool]]:
    namespace = {"d": "DAV:"}
    entries: list[dict[str, str | bool]] = []
    for response in root.findall("d:response", namespace):
        href = response.findtext("d:href", default="", namespaces=namespace)
        prop = response.find("d:propstat/d:prop", namespace)
        if prop is None:
            continue
        displayname = prop.findtext("d:displayname", default="", namespaces=namespace)
        is_collection = prop.find("d:resourcetype/d:collection", namespace) is not None
        entries.append({"href": href, "name": displayname, "is_collection": is_collection})
    return entries


def resolve_receita_base_url(config: PipelineConfig) -> str:
    if config.receita_share_token:
        share_root = _share_resource_url(config, _normalize_share_dir(config.receita_share_dir))
        config.base_url_file.write_text(share_root, encoding="utf-8")
        return share_root

    if config.base_url_file.exists():
        cached_url = config.base_url_file.read_text(encoding="utf-8").strip()
        if cached_url:
            return cached_url

    errors: list[str] = []
    for candidate_url in RECEITA_INDEX_URLS:
        try:
            fetch_text(candidate_url)
            config.base_url_file.write_text(candidate_url, encoding="utf-8")
            return candidate_url
        except requests.RequestException as exc:
            errors.append(f"{candidate_url} -> {exc}")

    raise RuntimeError(
        "Nenhum endpoint oficial da Receita respondeu corretamente. Tentativas: " + " | ".join(errors)
    )


def discover_latest_release(config: PipelineConfig) -> str:
    if config.receita_share_token:
        root = _propfind(_share_resource_url(config, _normalize_share_dir(config.receita_share_dir)))
        releases = [
            str(entry["name"])
            for entry in _iter_dav_entries(root)
            if entry["is_collection"] and re.fullmatch(r"\d{4}-\d{2}", str(entry["name"]))
        ]
        if not releases:
            raise RuntimeError("Nao foi possivel identificar a release mais recente no compartilhamento da Receita.")
        return sorted(releases)[-1]

    html = fetch_text(resolve_receita_base_url(config))
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
    release = discover_latest_release(config)
    persist_release(config, release)
    return release


def receita_release_url(config: PipelineConfig, release: str) -> str:
    if config.receita_share_token:
        return _share_resource_url(config, _normalize_share_dir(config.receita_share_dir), release)
    return urljoin(resolve_receita_base_url(config), f"{release}/")


def list_remote_files(config: PipelineConfig, release: str) -> list[str]:
    if config.receita_share_token:
        root = _propfind(receita_release_url(config, release))
        files = [
            str(entry["name"])
            for entry in _iter_dav_entries(root)
            if not entry["is_collection"] and str(entry["name"]).lower().endswith(".zip")
        ]
        if not files:
            raise RuntimeError(f"Nenhum arquivo zip encontrado para a release {release}.")
        return sorted(set(files))

    html = fetch_text(receita_release_url(config, release))
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


def apply_test_mode(selected_files: list[str], enabled: bool) -> list[str]:
    if not enabled:
        return selected_files

    empresas = [name for name in selected_files if re.match(r"Empresas\d+\.zip$", name)]
    estabelecimentos = [name for name in selected_files if re.match(r"Estabelecimentos\d+\.zip$", name)]
    outros = [
        name
        for name in selected_files
        if name not in empresas and name not in estabelecimentos and not re.match(r"Socios\d+\.zip$", name)
    ]

    limited: list[str] = []
    if empresas:
        limited.append(empresas[0])
    if estabelecimentos:
        limited.append(estabelecimentos[0])
    limited.extend(sorted(outros))
    return sorted(limited)


def clear_existing_archives(config: PipelineConfig) -> None:
    for zip_path in sorted(config.raw_dir.glob("*.zip")):
        LOGGER.info("Removendo arquivo zip antigo antes do novo download: %s", zip_path.name)
        zip_path.unlink(missing_ok=True)


def download_file(url: str, destination: Path) -> None:
    LOGGER.info("Baixando %s", url)
    with requests.get(url, headers=DEFAULT_HEADERS, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def download_inputs(config: PipelineConfig) -> list[Path]:
    config.ensure_directories()
    clear_existing_archives(config)
    release = resolve_release(config)
    remote_files = list_remote_files(config, release)
    selected_files = select_files(remote_files, include_socios=config.include_socios)
    selected_files = apply_test_mode(selected_files, enabled=config.test_mode)
    downloaded: list[Path] = []

    for file_name in selected_files:
        destination = config.raw_dir / file_name
        download_file(f"{receita_release_url(config, release).rstrip('/')}/{quote(file_name)}", destination)
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
        LOGGER.info("Removendo zip apos extracao bem-sucedida: %s", zip_path.name)
        zip_path.unlink(missing_ok=True)
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
