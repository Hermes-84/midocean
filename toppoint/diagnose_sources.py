from __future__ import annotations

import json
import os
import re
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
import xml.etree.ElementTree as ET

import boto3
from botocore.config import Config

from build_exports_clean import build_from_source_root

S3_BUCKET = os.getenv("TOPPOINT_S3_BUCKET", "toppoint-xml")
S3_PREFIX = os.getenv("TOPPOINT_S3_BASE_PREFIX", "EUR").strip("/")
S3_REGION = os.getenv("TOPPOINT_S3_REGION", "eu-north-1")
S3_ENDPOINT = os.getenv("TOPPOINT_S3_ENDPOINT", "https://s3-eu-north-1.amazonaws.com")
OUT_DIR = Path(os.getenv("TOPPOINT_DIAGNOSTIC_DIR", "out/toppoint-diagnostic"))
SOURCE_DIR = OUT_DIR / "source"
SCHEMA_DIR = OUT_DIR / "schema"
EXPORT_DIR = OUT_DIR / "exports"

SUPPORT_XML_NAMES = {
    "productimages",
    "productiontimes",
    "productiontimesfl",
    "productprices",
}


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def canonical_name(filename: str) -> str:
    stem = PurePosixPath(filename).stem.lower()
    return re.sub(r"[^a-z0-9]", "", stem)


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


def list_objects(client) -> list[dict]:
    paginator = client.get_paginator("list_objects_v2")
    result: list[dict] = []
    prefix = f"{S3_PREFIX}/" if S3_PREFIX else ""
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith("/"):
                continue
            result.append(
                {
                    "key": key,
                    "size": int(item.get("Size", 0)),
                    "last_modified": item.get("LastModified").isoformat()
                    if item.get("LastModified")
                    else None,
                    "etag": str(item.get("ETag", "")).strip('"'),
                }
            )
    if not result:
        raise RuntimeError(f"Nessun oggetto visibile in s3://{S3_BUCKET}/{prefix}")
    return result


def classify_key(key: str) -> set[str]:
    path = key.lower()
    canonical = canonical_name(PurePosixPath(key).name)
    classes: set[str] = set()

    if path.endswith(".xml") and ("/feed-v4/" in path or canonical.endswith("v4")):
        classes.add("v4_xml")
    if path.endswith(".xml") and "/feed-v3/" in path:
        classes.add("v3_xml")
    if path.endswith(".xml") and re.fullmatch(r"stock(?:v\d+)?", canonical):
        classes.add("stock")
    if path.endswith(".xml") and "print" in canonical and "price" in canonical:
        classes.add("printprices")
    if path.endswith(".xml") and "price" in canonical:
        classes.add("price_xml")
    if path.endswith(".xml") and canonical in SUPPORT_XML_NAMES:
        classes.add("support_xml")
    if path.endswith(".pdf") and "manual" in canonical:
        classes.add("manual")
    return classes


def unique_destination(key: str) -> Path:
    safe_parts = [
        re.sub(r"[^A-Za-z0-9._-]+", "_", part)
        for part in PurePosixPath(key).parts
    ]
    return SOURCE_DIR.joinpath(*safe_parts)


def download_selected(client, manifest: list[dict]) -> list[dict]:
    selected: list[dict] = []
    wanted = {"v4_xml", "v3_xml", "stock", "printprices", "support_xml"}
    for item in manifest:
        classes = classify_key(item["key"])
        if not classes.intersection(wanted):
            continue
        destination = unique_destination(item["key"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        print(f"Download s3://{S3_BUCKET}/{item['key']} -> {destination}")
        client.download_file(S3_BUCKET, item["key"], str(destination))
        selected.append({**item, "classes": sorted(classes), "local_path": str(destination)})
    if not selected:
        raise RuntimeError("Nessun XML Toppoint selezionato")
    return selected


def schema_summary(path: Path) -> dict:
    path_counts: Counter[str] = Counter()
    attribute_counts: Counter[str] = Counter()
    samples: dict[str, list[str]] = defaultdict(list)
    stack: list[str] = []
    root_tag = None
    element_count = 0

    for event, elem in ET.iterparse(path, events=("start", "end")):
        tag = local_name(elem.tag)
        if event == "start":
            if root_tag is None:
                root_tag = tag
            stack.append(tag)
            continue
        element_count += 1
        current_path = "/".join(stack)
        path_counts[current_path] += 1
        for attr_name in elem.attrib:
            attribute_counts[f"{current_path}/@{local_name(attr_name)}"] += 1
        value = (elem.text or "").strip()
        if value and len(samples[current_path]) < 8:
            samples[current_path].append(re.sub(r"\s+", " ", value)[:500])
        stack.pop()
        elem.clear()

    return {
        "file": path.name,
        "size": path.stat().st_size,
        "root_tag": root_tag,
        "element_count": element_count,
        "paths": [
            {"path": key, "count": count, "samples": samples.get(key, [])}
            for key, count in path_counts.most_common()
        ],
        "attributes": [
            {"path": key, "count": count}
            for key, count in attribute_counts.most_common()
        ],
    }


def write_text_report(manifest, selected, schemas, exports) -> None:
    lines = [
        "TOPPOINT S3 DIAGNOSTIC",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Bucket: {S3_BUCKET}",
        f"Prefix: {S3_PREFIX}",
        "",
        "ALL VISIBLE OBJECTS",
    ]
    for item in sorted(manifest, key=lambda x: x["key"].lower()):
        classes = ",".join(sorted(classify_key(item["key"]))) or "-"
        lines.append(f"{item['size']:>12}  [{classes}]  {item['key']}")
    lines.extend(["", "DOWNLOADED FILES"])
    for item in selected:
        lines.append(f"{item['size']:>12}  {item['key']}")
    lines.extend(["", "XML ROOTS"])
    for schema in schemas:
        lines.append(
            f"{schema['file']}: root={schema['root_tag']} "
            f"elements={schema['element_count']} size={schema['size']}"
        )
    lines.extend([
        "",
        "GENERATED EXPORTS",
        f"Products.csv: {exports['products']['rows']} rows, {exports['products']['columns']} columns",
        f"DPO PRINT.csv: {exports['dpo_print']['rows']} rows",
        f"Exact duplicate columns removed: {len(exports['products'].get('dropped_duplicate_columns', []))}",
    ])
    (OUT_DIR / "diagnostic.txt").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    client = s3_client()
    manifest = list_objects(client)
    for item in manifest:
        item["classes"] = sorted(classify_key(item["key"]))
    (OUT_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    selected = download_selected(client, manifest)
    schemas: list[dict] = []
    for item in selected:
        path = Path(item["local_path"])
        if path.suffix.lower() != ".xml":
            continue
        print(f"Analisi schema XML: {path}")
        schema = schema_summary(path)
        schemas.append(schema)
        (SCHEMA_DIR / f"{path.name}.schema.json").write_text(
            json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    exports = build_from_source_root(SOURCE_DIR / "EUR", EXPORT_DIR)
    write_text_report(manifest, selected, schemas, exports)

    # I feed completi non devono finire nell'artifact del repository pubblico.
    shutil.rmtree(SOURCE_DIR, ignore_errors=True)
    print(f"Diagnostic + export preview completato: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
