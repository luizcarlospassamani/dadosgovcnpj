from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


RECEITA_INDEX_URLS = [
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/",
    "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/",
]
RECEITA_SHARE_BASE_URL = "https://arquivos.receitafederal.gov.br/public.php/dav/files"
DEFAULT_RECEITA_SHARE_DIR = "/Dados/Cadastros/CNPJ"
JUCEES_CSV_URL = (
    "https://dados.es.gov.br/dataset/9bf62349-634b-4e87-93ee-d7ee521bb00f/"
    "resource/f3f7fed7-9d67-4616-962e-d3084146eab9/download/"
    "_relatorio_mensal_site_transparencia_com_atividade_principal_sel_202604011926.csv"
)


@dataclass(slots=True)
class PipelineConfig:
    project_root: Path
    state: str = "ES"
    release: str | None = None
    include_socios: bool = False
    cleanup: bool = False
    receita_share_token: str | None = None
    receita_share_dir: str = DEFAULT_RECEITA_SHARE_DIR

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def extracted_dir(self) -> Path:
        return self.data_dir / "extracted"

    @property
    def tmp_dir(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def output_dir(self) -> Path:
        return self.data_dir / "output"

    @property
    def metadata_dir(self) -> Path:
        return self.data_dir / "metadata"

    @property
    def release_file(self) -> Path:
        return self.metadata_dir / "latest_release.txt"

    @property
    def base_url_file(self) -> Path:
        return self.metadata_dir / "base_url.txt"

    def ensure_directories(self) -> None:
        for path in (
            self.data_dir,
            self.raw_dir,
            self.extracted_dir,
            self.tmp_dir,
            self.output_dir,
            self.metadata_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
