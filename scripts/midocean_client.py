from __future__ import annotations
import os
from typing import Any, Dict, Optional, List
import requests
from requests import Response
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

BASE_URL = os.getenv("MIDOCEAN_BASE_URL", "https://api.midocean.com").rstrip("/")
API_KEY = os.getenv("MIDOCEAN_API_KEY", "").strip()
TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))

class HttpError(Exception):
    pass

class MidoceanClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None) -> None:
        self.base_url = (base_url or BASE_URL).rstrip("/")
        self.api_key = (api_key or API_KEY).strip()
        if not self.api_key:
            raise ValueError("Missing MIDOCEAN_API_KEY")

    def _auth_headers(self) -> List[Dict[str, str]]:
        # Header ufficiale per le API midocean
        return [
            {"x-Gateway-APIKey": self.api_key},
            {"X-Gateway-APIKey": self.api_key},  # fallback case-variant
        ]

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=20),
        retry=retry_if_exception_type((requests.RequestException, HttpError)),
    )
    def get(self, path: str, accept: str = "text/json", params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_err: Optional[Exception] = None

        for auth in self._auth_headers():
            headers = {"Accept": accept}
            headers.update(auth)
            try:
                resp: Response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            except requests.RequestException as e:
                last_err = e
                continue

            status = resp.status_code
            if status == 200:
                # prova JSON, altrimenti restituisci testo grezzo
                try:
                    return resp.json()
                except ValueError:
                    return {"raw": resp.text}

            if status in (401, 403):
                last_err = HttpError(f"Auth failed with {list(auth.keys())[0]} (status={status})")
                # prova header alternativo
                continue

            if status in (429, 500, 502, 503, 504):
                # errori transitori → retry
                raise HttpError(f"Transient HTTP {status}: {resp.text[:200]}")

            # errori non transitori
            raise HttpError(f"HTTP {status}: {resp.text[:200]}")

        # esaurite le varianti di header
        raise last_err or HttpError("Authentication failed with all header variants")

