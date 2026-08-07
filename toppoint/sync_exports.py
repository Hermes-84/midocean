from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path

import boto3
import requests
from botocore.config import Config

from build_exports import build_from_source_root

S3_BUCKET = os.getenv("TOPPOINT_S3_BUCKET", "toppoint-xml")
S3_REGION = os.getenv("TOPPOINT_S3_REGION", "eu-north-1")
S3_ENDPOINT = os.getenv("TOPPOINT_S3_ENDPOINT", "https://s3-eu-north-1.amazonaws.com")
DROPBOX_BASE_PATH = os.getenv("DROPBOX_BASE_PATH_TOPPOINT", "/Toppoint").rstrip("/")

TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"
SESSION_START_URL = "https://content.dropboxapi.com/2/files/upload_session/start"
SESSION_APPEND_URL = "https://content.dropboxapi.com/2/files/upload_session/append_v2"
SESSION_FINISH_URL = "https://content.dropboxapi.com/2/files/upload_session/finish"
CHUNK_SIZE = 8 * 1024 * 1024

WEEKLY_SOURCES = [
    "EUR/feed-v4/Products_v4.xml",
    "EUR/feed-v4/ProductTranslations_v4.xml",
    "EUR/feed-v4/ProductPrices_v4.xml",
    "EUR/feed-v4/Colors_v4.xml",
    "EUR/feed-v4/Categories_v4.xml",
    "EUR/feed-v4/Print_v4.xml",
    "EUR/feed-v4/PositionTranslations_v4.xml",
    "EUR/product_images.xml",
    "EUR/ProductionTimes.xml",
    "EUR/printprices.xml",
]


def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["TOPPOINT_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["TOPPOINT_AWS_SECRET_ACCESS_KEY"],
        region_name=S3_REGION,
        endpoint_url=S3_ENDPOINT,
        config=Config(
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


def download_key(client, key: str, root: Path) -> Path:
    destination = root / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Download s3://{S3_BUCKET}/{key} -> {destination}")
    client.download_file(S3_BUCKET, key, str(destination))
    if not destination.exists() or destination.stat().st_size == 0:
        raise RuntimeError(f"Download vuoto: {key}")
    return destination


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
                    {"path": destination, "mode": "overwrite", "autorename": False, "mute": True}
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
                    "Dropbox-API-Arg": json.dumps({"cursor": cursor, "close": False}),
                },
                data=chunk,
                timeout=600,
            )
            response.raise_for_status()
            offset += len(chunk)
            handle.seek(-len(next_chunk), 1)

    raise RuntimeError(f"Upload chunked non completato: {path}")


def upload(path: Path, filename: str, token: str) -> str:
    destination = f"{DROPBOX_BASE_PATH}/{filename}"
    if path.stat().st_size <= 140 * 1024 * 1024:
        upload_small(path, destination, token)
    else:
        upload_chunked(path, destination, token)
    print(f"Dropbox: {destination} ({path.stat().st_size} byte)")
    return destination


def sync_stock() -> None:
    client = s3_client()
    token = dropbox_access_token()
    with tempfile.TemporaryDirectory(prefix="toppoint-stock-") as tmp:
        root = Path(tmp)
        stock = download_key(client, "EUR/stock.xml", root)
        upload(stock, "stock.xml", token)


def sync_weekly() -> None:
    client = s3_client()
    token = dropbox_access_token()
    with tempfile.TemporaryDirectory(prefix="toppoint-weekly-") as tmp:
        root = Path(tmp)
        for key in WEEKLY_SOURCES:
            download_key(client, key, root)

        source_root = root / "EUR"
        export_dir = root / "exports"
        result = build_from_source_root(source_root, export_dir)

        upload(export_dir / "Products.csv", "Products.csv", token)
        upload(source_root / "feed-v4" / "Products_v4.xml", "Products_v4.xml", token)
        upload(source_root / "feed-v4" / "Print_v4.xml", "Print.xml", token)
        upload(export_dir / "DPO PRINT.csv", "DPO PRINT.csv", token)
        upload(source_root / "printprices.xml", "printprices.xml", token)

        print(
            "Weekly Toppoint completato: "
            f"Products={result['products']['rows']} righe, "
            f"DPO={result['dpo_print']['rows']} righe"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("stock", "weekly", "all"))
    args = parser.parse_args()

    if args.mode in {"stock", "all"}:
        sync_stock()
    if args.mode in {"weekly", "all"}:
        sync_weekly()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
