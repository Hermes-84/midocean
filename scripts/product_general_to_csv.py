from __future__ import annotations
import os, re, pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import write_csv, log
from scripts.dropbox_uploader import upload_file

OUT = os.getenv("OUT_DIR", "out")
LANG = os.getenv("MIDOCEAN_LANGUAGE", "it")
INFILE = os.path.join(OUT, "general.csv")
OUTFILE = INFILE  # sovrascrive lo stesso file

# ---- helpers decimali / parsing ------------------------------------------------

def _eu(val):
    """Converte in stringa con virgola decimale (UE)."""
    if val in (None, ""): return ""
    try:
        f = float(str(val).replace(",", "."))
        s = f"{f:.3f}".rstrip("0").rstrip(".")
        return s.replace(".", ",")
    except:
        return str(val)

def _eu_clean_numeric(val):
    """Prende un valore che può contenere unità (es. '303 KG'), estrae il numero e lo formatta EU."""
    if val in (None, ""): return ""
    s = str(val).strip()
    s = re.sub(r"[^0-9,.\-]", "", s)   # tieni solo cifre e separatori
    if s == "": return ""
    return _eu(s)

def _safe_i(x) -> int:
    try:
        return int(float(str(x).replace(",", ".")))
    except:
        return 0

def _areas_from_positions(positions: list) -> tuple[list[int], list[str]]:
    """(aree_mm2, aree_cm2_string). Dedup/ordina. Niente stringhe vuote."""
    mm2: list[int] = []
    for p in positions or []:
        h = _safe_i(p.get("max_print_size_height") or p.get("height") or p.get("print_height"))
        w = _safe_i(p.get("max_print_size_width")  or p.get("width")  or p.get("print_width"))
        if h > 0 and w > 0:
            mm2.append(h * w)
    mm2 = sorted(set(mm2), reverse=True)
    cm2 = [f"{int(round(a / 100.0))}cm2" for a in mm2]
    return mm2, cm2

def _bucket_cm2_list(cm2_csv: str) -> str:
    """
    Riceve '86cm2,30cm2,24cm2,1cm2' -> bucket (49/149/299/749) deduplicati, desc.
    Esempio: -> '149cm2,49cm2'
    """
    if not cm2_csv: return ""
    out: list[int] = []
    for part in str(cm2_csv).split(","):
        part = part.strip()
        if not part: continue
        m = re.search(r"(\d+)", part)
        if not m: continue
        n = int(m.group(1))
        # bucket
        if n <= 49: b

