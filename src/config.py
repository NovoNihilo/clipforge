"""Central config loaded from .env"""
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    # Twitch
    twitch_client_id: str = ""
    twitch_client_secret: str = ""

    # Paths
    database_path: str = "clipforge.db"
    assets_dir: str = "assets"
    outputs_dir: str = "outputs"

    # Rate limits
    max_concurrency: int = 2
    request_delay_sec: float = 1.5
    max_retries: int = 3

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def assets_path(self) -> Path:
        p = Path(self.assets_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def outputs_path(self) -> Path:
        p = Path(self.outputs_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p


settings = Settings()
