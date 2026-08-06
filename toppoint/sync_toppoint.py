from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable

import boto3
import requests
from botocore.config import Config

S3_BUCKET = os.getenv("TOPPOINT_S3_BUCKET", "toppoint-xml")
S3_PREFIX = os.getenv("TOPPOINT_S3_PREFIX", "EUR/V4").strip("/")
S3_REGION = os.getenv("TOPPOINT_S3_REGION", "eu-north-1")
S3_ENDPOINT = os.getenv("TOPPOINT_S3_ENDPOINT", "https://s3-eu-north-1.amazonaws.com")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "out/toppoint"))
DROPBOX_BASE_PATH = os.getenv("DROPBOX_BASE_PATH_TOPPOINT", "/Toppoint").rstrip("/")

EXPECTED_STEMS = {
    "stock": {"stock"},
    "products": {"product", "products"},
    "print": {"print"},
    "printprices": {"printprice", "printprices", "print_price", "print_prices"},
}

TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"
SESSION_START_URL = "https://content.dropboxapi.com/2/files/upload_session/start"
SESSION_APPEND_URL = "https://content.dropboxapi.com/2/files/upload_session/append_v2"
SESSION_FINISH_URL = "https://content.dropboxapi.com/2/files/upload_session/finish"
CHUNK_SIZE = 8 * 1024 * 1024


def normalized_stem(key: str) -> str:
    name = PurePosixPath(key).name.lower()
    while "." in name:
        name = name.rsplit(".", 1)[0]
    return "".join(ch for ch in name if ch.isalnum() or ch == "_")


def normalized_segment(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def s3_client():
    access_key = os.environ["TOPPOINT_AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["TOPPOINT_AWS_SECRET_ACCESS_KEY"]
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=S3_REGION,
        endpoint_url=S3_ENDPOINT,
        config=Config(
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


def list_keys_for_prefix(client, prefix: str) -> list[str]:
    clean_prefix = prefix.strip("/")
    request_prefix = f"{clean_prefix}/" if clean_prefix else ""
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=request_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)

    return keys


def is_v4_key(key: str) -> bool:
    segments = [normalized_segment(part) for part in PurePosixPath(key).parts[:-1]]
    return any(segment in {"v4", "version4", "xmlv4", "feedv4"} for segment in segments)


def list_keys(client) -> list[str]:
    keys = list_keys_for_prefix(client, S3_PREFIX)
    if keys:
        print(f"Percorso S3 utilizzato: s3://{S3_BUCKET}/{S3_PREFIX}/")
        return keys

    base_prefix = S3_PREFIX.split("/", 1)[0] if S3_PREFIX else "EUR"
    broader_keys = list_keys_for_prefix(client, base_prefix)

    if not broader_keys:
        broader_keys = list_keys_for_prefix(client, "")

    if not broader_keys:
        raise RuntimeError(
            f"Nessun file visibile nel bucket s3://{S3_BUCKET}/. "
            "Le credenziali sono state accettate, ma non risultano oggetti accessibili."
        )

    v4_keys = [key for key in broader_keys if is_v4_key(key)]
    if v4_keys:
        detected_roots = sorted(
            {"/".join(PurePosixPath(key).parts[:-1]) for key in v4_keys}
        )
        print("Percorso V4 rilevato automaticamente:")
        for root in detected_roots[:20]:
            print(f"  - s3://{S3_BUCKET}/{root}/")
        return v4_keys

    sample = ", ".join(sorted(broader_keys)[:80])
    raise RuntimeError(
        "Nessuna cartella V4 riconosciuta. "
        f"Primi file visibili nel bucket: {sample}"
    )


def select_key(keys: Iterable[str], logical_name: str) -> str:
    allowed = EXPECTED_STEMS[logical_name]
    matches = [key for key in keys if normalized_stem(key) in allowed]

    if not matches:
        available = ", ".join(sorted(PurePosixPath(k).name for k in keys)[:100])
        raise RuntimeError(
            f"File '{logical_name}' non trovato nel feed V4. "
            f"File disponibili: {available}"
        )

    if len(matches) > 1:
        xml_matches = [
            key for key in matches if PurePosixPath(key).suffix.lower() == ".xml"
        ]
        if len(xml_matches) == 1:
            return xml_matches[0]
        raise RuntimeError(f"Più file compatibili per '{logical_name}': {matches}")

    return matches[0]


def validate_download(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        raise RuntimeError(f"Download vuoto o mancante: {path}")
    with path.open("rb") as handle:
        head = handle.read(256).lstrip()
    if not head.startswith((b"<?xml", b"<")):
        raise RuntimeError(f"Il file non sembra XML: {path}")


def dropbox_access_token() -> str:
    direct = os.getenv("DROPBOX_ACCESS_TOKEN")
    if direct:
        return direct
    app_key = os.environ["DROPBOX_APP_KEY"]
    app_secret = os.environ["DROPBOX_APP_SECRET"]
    refresh_token = os.environ["DROPBOX_REFRESH_TOKEN"]
    response = requests.post(
        TOKEN_URL,
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        auth=(app_key, app_secret),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def upload_small(path: Path, destination: str, token: str) -> None:
    with path.open("rb") as handle:
        response = requests.post(
            UPLOAD_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream",
                "Dropbox-API-Arg": json.dumps(
                    {"path": destination, "mode": "overwrite", "autorename": False}
                ),
            },
            data=handle,
            timeout=600,
        )
    response.raise_for_status()


def upload_chunked(path: Path, destination: str, token: str) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
    }
    with path.open("rb") as handle:
        first = handle.read(CHUNK_SIZE)
        response = requests.post(
            SESSION_START_URL,
            headers={**headers, "Dropbox-API-Arg": json.dumps({"close": False})},
            data=first,
            timeout=600,
        )
        response.raise_for_status()
        session_id = response.json()["session_id"]
        offset = len(first)

        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            next_chunk = handle.read(CHUNK_SIZE)
            is_last = not next_chunk
            cursor = {"session_id": session_id, "offset": offset}

            if is_last:
                response = requests.post(
                    SESSION_FINISH_URL,
                    headers={
                        **headers,
                        "Dropbox-API-Arg": json.dumps(
                            {
                                "cursor": cursor,
                                "commit": {
                                    "path": destination,
                                    "mode": "overwrite",
                                    "autorename": False,
                                    "mute": True,
                                },
                            }
                        ),
                    },
                    data=chunk,
                    timeout=600,
                )
                response.raise_for_status()
                return

            response = requests.post(
                SESSION_APPEND_URL,
                headers={
                    **headers,
                    "Dropbox-API-Arg": json.dumps(
                        {"cursor": cursor, "close": False}
                    ),
                },
                data=chunk,
                timeout=600,
            )
            response.raise_for_status()
            offset += len(chunk)
            handle.seek(-len(next_chunk), 1)

    raise RuntimeError(f"Upload chunked non completato: {path}")


def upload_to_dropbox(path: Path) -> str:
    token = dropbox_access_token()
    destination = f"{DROPBOX_BASE_PATH}/{path.name}"
    if path.stat().st_size <= 140 * 1024 * 1024:
        upload_small(path, destination, token)
    else:
        upload_chunked(path, destination, token)
    return destination


def sync(logical_names: list[str]) -> None:
    client = s3_client()
    keys = list_keys(client)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for logical_name in logical_names:
        key = select_key(keys, logical_name)
        suffixes = "".join(PurePosixPath(key).suffixes) or ".xml"
        local_path = OUTPUT_DIR / f"{logical_name}{suffixes}"
        print(f"Download s3://{S3_BUCKET}/{key} -> {local_path}")
        client.download_file(S3_BUCKET, key, str(local_path))
        validate_download(local_path)
        destination = upload_to_dropbox(local_path)
        print(
            f"Caricato su Dropbox: {destination} "
            f"({local_path.stat().st_size} byte)"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincronizza feed XML Toppoint V4 verso Dropbox"
    )
    parser.add_argument(
        "mode",
        choices=("stock", "weekly", "all"),
        help="stock; weekly=products, print, printprices; all=tutti",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    modes = {
        "stock": ["stock"],
        "weekly": ["products", "print", "printprices"],
        "all": ["stock", "products", "print", "printprices"],
    }
    try:
        sync(modes[args.mode])
    except Exception as exc:
        print(f"ERRORE: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
