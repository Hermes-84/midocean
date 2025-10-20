from __future__ import annotations
import os
import pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import now_local, time_hms, write_csv, log, SUPPLIER
from scripts.dropbox_uploader import upload_file

OUT = os.getenv("OUT_DIR", "out")
FILENAME = "stock.csv"

def compute_qty(row) -> int:
    try:
        q = int(row.get("qty") or 0)
    except Exception:
        q = 0
    fad = str(row.get("first_arrival_date") or "")
    today = now_local().strftime("%Y-%m-%d")
    if q == 0 and fad == today:
        try:
            return int(row.get("first_arrival_qty") or 0)
        except Exception:
            return 0
    return q

def main():
    client = MidoceanClient()
    data = client.get("gateway/gateway/stock/2.0", accept="text/json")
    stock_items = data.get("stock", [])
    if not isinstance(stock_items, list):
        raise RuntimeError("Unexpected stock payload")

    rows = []
    for it in stock_items:
        row = {
            "supplier": SUPPLIER,
            "date": now_local().strftime("%Y%m%d"),
            "time": time_hms(),
            "sku": it.get("sku", ""),
            "qty": None,
            "first_arrival_date": it.get("first_arrival_date"),
            "first_arrival_qty": it.get("first_arrival_qty"),
            "next_arrival_date": it.get("next_arrival_date"),
            "next_arrival_qty": it.get("next_arrival_qty"),
        }
        # normalize date format to YYYYMMDD if needed
        for k in ("first_arrival_date", "next_arrival_date"):
            if isinstance(row[k], str) and "-" in row[k]:
                row[k] = row[k].replace("-", "")
        rows.append(row)

    # Compute effective qty with ISO dates for rule
    def iso(d):
        if not d: return ""
        if "-" in d and len(d) == 10: return d
        if len(d) == 8: return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
        return ""

    tmp = []
    for r in rows:
        it = dict(r)
        it["first_arrival_date"] = iso(it.get("first_arrival_date"))
        tmp.append(it)
    q = [compute_qty(r) for r in tmp]
    df = pd.DataFrame(rows)
    df["qty"] = q

    out_path = os.path.join(OUT, FILENAME)
    write_csv(df, out_path)
    dest = upload_file(out_path, FILENAME)
    log.info("Uploaded to Dropbox → %s", dest)

if __name__ == "__main__":
    main()
