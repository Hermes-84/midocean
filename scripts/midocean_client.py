from __future__ import annotations
import os
from typing import Dict, Any
import requests
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
from requests import Response

BASE_URL = os.getenv("MIDOCEAN_BASE_URL", "https://api.midocean.com").rstrip("/")
API_KEY = os.getenv("MIDOCEAN_API_KEY", "").strip()
TIMEOUT = float(os.getenv("HTTP_TIMEOUT", 60))

class HttpError(Exception):
    pass

class MidoceanClient:
    def __init__(self, base_url: str | None = None, api_key: str | None = None):
        self.base_url = (base_url or BASE_URL).rstrip("/")
        self.api_key = (api_key or API_KEY).strip()
        if not self.api_key:
            raise ValueError("Missing MIDOCEAN_API_KEY")

    def _auth_headers(self) -> list[dict[str, str]]:
        return [
            {"X-API-Key": self.api_key},
            {"x-api-key": self.api_key},
            {"apikey": self.api_key},
        ]

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((HttpError, requests.RequestException)),
    )
    def get(self, path: str, accept: str = "text/json", params: Dict[str, Any] | None = None) -> dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_exc: Exception | None = None
        for auth_hdr in self._auth_headers():
            headers = {"Accept": accept}
            headers.update(auth_hdr)
            try:
                resp: Response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            except requests.RequestException as e:
                last_exc = e
                continue
            if resp.status_code == 200:
                if accept == "text/json":
                    return resp.json()
                return {"raw": resp.text}
            elif resp.status_code in (401, 403):
                last_exc = HttpError(f"Auth failed with header {list(auth_hdr.keys())[0]} status={resp.status_code}")
                continue
            elif resp.status_code in (429, 500, 502, 503, 504):
                raise HttpError(f"Server busy {resp.status_code}: {resp.text[:200]}")
            else:
                raise HttpError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        raise last_exc or HttpError("Authentication failed with all header variants")
