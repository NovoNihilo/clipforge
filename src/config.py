"""Central config loaded from .env"""
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    # Twitch
    twitch_client_id: str = ""
    twitch_client_secret: str = ""

    # LLM Provider: set ONE of these in .env (leave the other blank/commented)
    # The decider will auto-detect which to use based on which key is present.
    openai_api_key: str = ""
    xai_api_key: str = ""

    # HuggingFace (for pyannote speaker diarization)
    hf_token: str = ""

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

    @property
    def llm_provider(self) -> str:
        """Auto-detect LLM provider from which API key is set."""
        if self.xai_api_key:
            return "xai"
        if self.openai_api_key:
            return "openai"
        return "none"

    @property
    def llm_api_key(self) -> str:
        if self.xai_api_key:
            return self.xai_api_key
        return self.openai_api_key

    @property
    def llm_base_url(self) -> str:
        if self.llm_provider == "xai":
            return "https://api.x.ai/v1"
        return "https://api.openai.com/v1"

    @property
    def llm_model(self) -> str:
        if self.llm_provider == "xai":
            return "grok-3-fast"
        return "gpt-4.1"


settings = Settings()