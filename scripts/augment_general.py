from __future__ import annotations
import os, pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import write_csv, log
from scripts.dropbox_uploader import upload_file

OUT = os.getenv("OUT_DIR", "out")
LANG = os.getenv("MIDOCEAN_LANGUAGE", "it")
INFILE = os.path.join(OUT, "general.csv")
OUTFILE = INFILE  # sovrascrive lo stesso file

def _eu(val):
    if val in (None, ""): return ""
    try:
        f = float(str(val).replace(",", "."))
        s = f"{f:.3f}".rstrip("0").rstrip(".")
        return s.replace(".", ",")
    except:
        return str(val)

def _safe_i(x) -> int:
    try:
        return int(float(str(x).replace(",", ".")))
    except:
        return 0

def _areas_from_positions(positions: list) -> tuple[list[int], list[str]]:
    mm2 = []
    cm2 = []
    for p in positions or []:
        h = _safe_i(p.get("max_print_size_height"))
        w = _safe_i(p.get("max_print_size_width"))
        if h and w:
            a = h * w
            mm2.append(a)
            cm2.append(f"{a/100:.0f}cm2")
    mm2.sort(reverse=True)
    # ordina cm2 in base al numero (stringa tipo "299cm2")
    cm2.sort(key=lambda s: int(s[:-4]), reverse=True) if cm2 else None
    return mm2, cm2

def main():
    if not os.path.exists(INFILE):
        raise SystemExit(f"Missing input {INFILE} (run product_general_to_csv.py first)")

    df = pd.read_csv(INFILE, dtype=str).fillna("")
    client = MidoceanClient()

    # === PRICE (Product Pricelist 2.0) ===
    price_payload = client.get("gateway/pricelist/2.0", accept="text/json")
    price_rows = price_payload.get("price") if isinstance(price_payload, dict) else []
    sku_to_price = {}
    for r in price_rows or []:
        base = r.get("price")
        for sc in r.get("scale") or []:
            if str(sc.get("minimum_quantity")) == "1":
                base = sc.get("price") or base
                break
        sku = str(r.get("sku"))
        if sku:
            sku_to_price[sku] = base or ""

    # === PRINT DATA (Print Data 1.0) ===
    pd_payload = client.get("gateway/printdata/1.0", accept="text/json")
    if isinstance(pd_payload, dict) and isinstance(pd_payload.get("products"), list):
        pd_products = pd_payload["products"]
    elif isinstance(pd_payload, list):
        pd_products = pd_payload
    else:
        pd_products = []
        if isinstance(pd_payload, dict):
            for v in pd_payload.values():
                if isinstance(v, list) and v and isinstance(v[0], dict) and (
                    "master_code" in v[0] or "printing_positions" in v[0]
                ):
                    pd_products = v; break

    # aggrega per master_id (preferito) o master_code
    agg = {}
    def uniq(seq):
        seen=set(); out=[]
        for x in seq:
            if x not in seen:
                seen.add(x); out.append(x)
        return out

    for m in pd_products:
        master_code = m.get("master_code") or ""
        master_id   = m.get("master_id") or ""
        key = master_id or master_code
        positions = m.get("printing_positions") or m.get("print_positions") or []
        mm2, cm2 = _areas_from_positions(positions)
        techs = []
        maxcols = []
        for p in positions:
            for t in (p.get("printing_techniques") or []):
                if t.get("id"): techs.append(str(t["id"]))
                mx = t.get("max_colours") or t.get("maximum_colors") or t.get("max_colors")
                if mx not in (None, ""): maxcols.append(str(mx))
        agg[key] = {
            "manipulation": m.get("print_manipulation") or "",
            "techniques": ",".join(uniq(techs)),
            "max_colors": ",".join(uniq(maxcols)),
            "template": m.get("print_template") or "",
            "mm2": ",".join(str(a) for a in mm2),
            "cm2": ",".join(cm2),
        }

    # === PRODUCTS (per pesi net/gross) ===
    prod_payload = client.get("gateway/products/2.0", accept="text/json", params={"language": LANG})
    products_list = []
    if isinstance(prod_payload, dict):
        for v in prod_payload.values():
            if isinstance(v, list): products_list = v; break
    elif isinstance(prod_payload, list):
        products_list = prod_payload

    sku_to_weights = {}
    master_to_weights = {}
    for m in products_list:
        mid = m.get("master_id") or ""
        nw = m.get("net_weight")
        gw = m.get("gross_weight")
        gwu = m.get("gross_weight_unit") or "KG"
        if (nw or gw) and mid:
            master_to_weights[mid] = (nw, gw, gwu)
        for var in (m.get("variants") or []):
            sku = var.get("sku") or ""
            nv = var.get("net_weight") or nw
            gv = var.get("gross_weight") or gw
            gu = var.get("gross_weight_unit") or gwu
            if sku:
                sku_to_weights[sku] = (nv, gv, gu)

    # colonne del tuo CSV
    col_mid  = "products__product__product_print_id_2"
    col_mno  = "products__product__product_base_number"
    col_sku  = "products__product__product_number"
    cw_col   = "products__product__packaging_carton__weight"
    cq_col   = "products__product__packaging_carton__carton_quantity"

    # riempi colonne stampa
    df["products__product__manipulation"] = df.apply(
        lambda r: agg.get(r.get(col_mid) or r.get(col_mno),{}).get("manipulation",""), axis=1)
    df["Printing technique"] = df.apply(
        lambda r: agg.get(r.get(col_mid) or r.get(col_mno),{}).get("techniques",""), axis=1)
    df["Max colors"] = df.apply(
        lambda r: agg.get(r.get(col_mid) or r.get(col_mno),{}).get("max_colors",""), axis=1)
    df["Print position document"] = df.apply(
        lambda r: agg.get(r.get(col_mid) or r.get(col_mno),{}).get("template",""), axis=1)
    df["measuresprintrange"] = df.apply(
        lambda r: agg.get(r.get(col_mid) or r.get(col_mno),{}).get("mm2",""), axis=1)
    df["max_print_area"] = df.apply(
        lambda r: agg.get(r.get(col_mid) or r.get(col_mno),{}).get("cm2",""), axis=1)

    # price per SKU (copiato 1:1, di solito già con virgola)
    df["price"] = df[col_sku].map(lambda s: sku_to_price.get(str(s), ""))

    # pesi (EU decimal) + unit
    def _fill_weights(row):
        sku = row.get(col_sku,"")
        mid = row.get(col_mid,"")
        nw, gw, gwu = "", "", "KG"
        if sku in sku_to_weights:
            nw, gw, gwu = sku_to_weights[sku]
        elif mid in master_to_weights:
            nw, gw, gwu = master_to_weights[mid]
        # fallback gross = carton_weight / carton_qty
        try:
            cw = float(str(row.get(cw_col,"")).replace(",", ".")) if row.get(cw_col) else None
            cq = float(str(row.get(cq_col,"")).replace(",", ".")) if row.get(cq_col) else None
        except:
            cw, cq = None, None
        if (not gw) and cw and cq and cq>0:
            gw = cw / cq
            gwu = "KG"
        row["products__product__net_weight"] = _eu(nw) if nw else (_eu(gw) if gw else "")
        row["products__product__gross_weight"] = _eu(gw) if gw else ""
        row["products__product__gross_weight_unit"] = gwu if (gw or nw) else ""
        return row

    df = df.apply(_fill_weights, axis=1)

    # salva e ricarica su Dropbox (overwrite)
    write_csv(df, OUTFILE)
    dest = upload_file(OUTFILE, "general.csv")
    log.info("Augmented and uploaded general.csv → %s", dest)

if __name__ == "__main__":
    main()
