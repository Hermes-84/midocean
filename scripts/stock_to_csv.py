from __future__ import annotations
import os
import pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import write_csv, now_local, time_hms, log, SUPPLIER as SUPPLIER_DEFAULT
from scripts.dropbox_uploader import upload_file

OUT = os.getenv("OUT_DIR", "out")
FILENAME = "stock.csv"

COLUMNS = [
    "supplier",
    "date",
    "time",
    "sku",
    "qty",
    "first_arrival_date",
    "first_arrival_qty",
    "next_arrival_date",
    "next_arrival_qty",
]

def _to_int(v) -> int:
    try:
        if v is None or v == "":
            return 0
        return int(float(str(v).replace(",", ".").strip()))
    except Exception:
        return 0

def main():
    client = MidoceanClient()
    data = client.get("gateway/stock/2.0", accept="text/json")

    # La risposta ufficiale è { "modified_at": "...", "stock": [ {...} ] }
    if isinstance(data, dict):
        items = data.get("stock") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []

    supplier = os.getenv("SUPPLIER_NAME", SUPPLIER_DEFAULT)
    today = now_local().strftime("%Y%m%d")
    hhmmss = time_hms()

    rows = []
    non_zero, zeroes = 0, 0

    for s in items:
        sku = (s.get("sku") or s.get("product_number") or s.get("item") or "").strip()

        # Prendi SEMPRE il qty esposto dall'API (stock corrente aggregato su tutti i magazzini)
        # Nessuna “regola speciale” che lo sovrascrive.
        qty = _to_int(s.get("qty"))

        fa_date = s.get("first_arrival_date") or ""
        fa_qty  = _to_int(s.get("first_arrival_qty"))
        na_date = s.get("next_arrival_date") or ""
        na_qty  = _to_int(s.get("next_arrival_qty"))

        rows.append({
            "supplier": supplier,
            "date": today,
            "time": hhmmss,
            "sku": sku,
            "qty": qty,
            "first_arrival_date": fa_date,
            "first_arrival_qty": fa_qty,
            "next_arrival_date": na_date,
            "next_arrival_qty": na_qty,
        })

        if qty > 0:
            non_zero += 1
        else:
            zeroes += 1

    df = pd.DataFrame(rows, columns=COLUMNS)
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, FILENAME)
    write_csv(df, out_path)

    log.info("Stock: %d non-zero, %d zero", non_zero, zeroes)
    dest = upload_file(out_path, FILENAME)
    log.info("Uploaded to Dropbox → %s", dest)

if __name__ == "__main__":
    main()

