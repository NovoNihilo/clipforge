"""Shared HTTP client with retry + exponential backoff + jitter."""
import httpx
import asyncio
import random
from rich import print as rprint
from src.config import settings


async def fetch_json(
    url: str,
    headers: dict | None = None,
    params: dict | None = None,
    max_retries: int | None = None,
) -> dict | list | None:
    """GET JSON with retries + exponential backoff + jitter."""
    retries = max_retries or settings.max_retries
    for attempt in range(retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url, headers=headers, params=params)
                if resp.status_code == 429:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    rprint(f"[yellow]Rate limited, waiting {wait:.1f}s (attempt {attempt+1})[/yellow]")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500 and attempt < retries:
                wait = (2 ** attempt) + random.uniform(0, 1)
                rprint(f"[yellow]Server error {e.response.status_code}, retry in {wait:.1f}s[/yellow]")
                await asyncio.sleep(wait)
                continue
            rprint(f"[red]HTTP error: {e}[/red]")
            return None
        except (httpx.RequestError, Exception) as e:
            if attempt < retries:
                wait = (2 ** attempt) + random.uniform(0, 1)
                rprint(f"[yellow]Request error: {e}, retry in {wait:.1f}s[/yellow]")
                await asyncio.sleep(wait)
                continue
            rprint(f"[red]Request failed after {retries} retries: {e}[/red]")
            return None
    return None


async def download_file(url: str, dest_path: str, headers: dict | None = None) -> bool:
    """Download a file with streaming + retries."""
    for attempt in range(settings.max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
                async with client.stream("GET", url, headers=headers) as resp:
                    resp.raise_for_status()
                    with open(dest_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            f.write(chunk)
            return True
        except Exception as e:
            if attempt < settings.max_retries:
                wait = (2 ** attempt) + random.uniform(0, 1)
                rprint(f"[yellow]Download retry {attempt+1}: {e}[/yellow]")
                await asyncio.sleep(wait)
            else:
                rprint(f"[red]Download failed: {e}[/red]")
                return False
    return False
