from __future__ import annotations
import os, json, requests

APP_KEY = os.getenv("DROPBOX_APP_KEY")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
BASE_PATH = os.getenv("DROPBOX_BASE_PATH", "/Public/midocean").rstrip("/")

TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
CONTENT_URL = "https://content.dropboxapi.com/2/files"

class DropboxAuthError(Exception): pass
class DropboxUploadError(Exception): pass

def _get_access_token() -> str:
    if not (APP_KEY and APP_SECRET and REFRESH_TOKEN):
        raise DropboxAuthError("Missing Dropbox OAuth envs")
    resp = requests.post(
        TOKEN_URL,
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN},
        auth=(APP_KEY, APP_SECRET),
        timeout=30,
    )
    if resp.status_code != 200:
        raise DropboxAuthError(f"Token error {resp.status_code}: {resp.text}")
    return resp.json()["access_token"]

def upload_file(local_path: str, dropbox_filename: str) -> str:
    token = _get_access_token()
    dest_path = f"{BASE_PATH}/{dropbox_filename}"
    with open(local_path, "rb") as f:
        r = requests.post(
            f"{CONTENT_URL}/upload",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream",
                "Dropbox-API-Arg": json.dumps({"path": dest_path, "mode": {".tag": "overwrite"}}),
            },
            data=f,
            timeout=120,
        )
        if r.status_code != 200:
            raise DropboxUploadError(f"Upload failed: {r.status_code}: {r.text}")
    return dest_path
