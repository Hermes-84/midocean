# scripts/stock_to_csv.py
from __future__ import annotations
import os, pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import write_csv, log
from scripts.dropbox_uploader import upload_file

OUT = os.getenv("OUT_DIR", "out")
FILENAME = "stock.csv"

COLUMNS = [
    "supplier","date","time","sku","qty",
    "first_arrival_date","first_arrival_qty",
    "next_arrival_date","next_arrival_qty",
]

def _i(x):
    try:
        if x is None or x == "": return 0
        if isinstance(x, (int, float)): return int(x)
        s = str(x).strip()
        # gestisci "4,22" o "4.22"
        s = s.replace(",", ".")
        return int(float(s))
    except Exception:
        return 0

def main():
    client = MidoceanClient()
    # JSON esplicito
    data = client.get("gateway/stock/2.0", accept="text/json")

    stock = []
    items = data.get("stock") if isinstance(data, dict) else data
    if not isinstance(items, list):
        items = []

    for s in items:
        # qty robusto: qty | quantity | available | stock | QTY
        qty = s.get("qty")
        if qty in (None, "", 0, "0"):
            for k in ("quantity","available","stock","QTY","Quantity","AVAILABLE","STOCK"):
                if s.get(k) not in (None, ""):
                    qty = s.get(k); break

        row = {
            "supplier": os.getenv("SUPPLIER_NAME","Mid Ocean Brands"),
            "date": data.get("modified_at","")[:10] if isinstance(data,dict) else "",
            "time": data.get("modified_at","")[11:19] if isinstance(data,dict) else "",
            "sku": s.get("sku") or s.get("id") or s.get("product") or "",
            "qty": _i(qty),
            "first_arrival_date": s.get("first_arrival_date") or "",
            "first_arrival_qty": _i(s.get("first_arrival_qty")),
            "next_arrival_date": s.get("next_arrival_date") or "",
            "next_arrival_qty": _i(s.get("next_arrival_qty")),
        }
        stock.append(row)

    df = pd.DataFrame(stock, columns=COLUMNS)
    out_path = os.path.join(OUT, FILENAME)
    write_csv(df, out_path)
    dest = upload_file(out_path, FILENAME)
    log.info("Uploaded to Dropbox → %s", dest)

if __name__ == "__main__":
    main()

