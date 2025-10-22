from __future__ import annotations
import os, re, pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import write_csv, log
from scripts.dropbox_uploader import upload_file

OUT = os.getenv("OUT_DIR", "out")
LANG = os.getenv("MIDOCEAN_LANGUAGE", "it")
INFILE = os.path.join(OUT, "general.csv")
OUTFILE = INFILE  # sovrascrive lo stesso file

# ---------- helpers -------------------------------------------------------------

def _eu(val):
    if val in (None, ""): return ""
    try:
        f = float(str(val).replace(",", "."))
        s = f"{f:.3f}".rstrip("0").rstrip(".")
        return s.replace(".", ",")
    except:
        return str(val)

def _eu_clean_numeric(val):
    """Es. '303 KG' -> '303' (eu-decimal)."""
    if val in (None, ""): return ""
    s = str(val).strip()
    s = re.sub(r"[^0-9,.\-]", "", s)
    if s == "": return ""
    return _eu(s)

def _to_float(val):
    try:
        return float(str(val).replace(",", "."))
    except:
        return None

def _safe_i(x) -> int:
    try:
        return int(float(str(x).replace(",", ".")))
    except:
        return 0

def _areas_from_positions(positions: list) -> tuple[list[int], list[str]]:
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
    if not cm2_csv: return ""
    out: list[int] = []
    for part in str(cm2_csv).split(","):
        part = part.strip()
        if not part: continue
        m = re.search(r"(\d+)", part)
        if not m: continue
        n = int(m.group(1))
        if n <= 49: b = 49
        elif n <= 149: b = 149
        elif n <= 299: b = 299
        else: b = 749
        out.append(b)
    out = sorted(set(out), reverse=True)
    return ",".join(f"{v}cm2" for v in out)

def _uniq(seq):
    seen=set(); out=[]
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# ---------- main ----------------------------------------------------------------

def main():
    if not os.path.exists(INFILE):
        raise SystemExit(f"Missing input {INFILE} (run product_general_to_csv.py first)")

    df = pd.read_csv(INFILE, dtype=str).fillna("")
    client = MidoceanClient()

    # === PRICE ==================================================================
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

    # === PRINT DATA ==============================================================
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

    agg = {}
    for m in pd_products:
        master_code = m.get("master_code") or ""
        master_id   = m.get("master_id") or ""
        key = master_id or master_code
        positions = m.get("printing_positions") or m.get("print_positions") or []
        mm2, cm2 = _areas_from_positions(positions)
        techs, maxcols = [], []
        for p in positions:
            for t in (p.get("printing_techniques") or []):
                if t.get("id"): techs.append(str(t["id"]))
                mx = t.get("max_colours") or t.get("maximum_colors") or t.get("max_colors")
                if mx not in (None, ""): maxcols.append(str(mx))
        agg[key] = {
            "manipulation": m.get("print_manipulation") or "",
            "techniques": ",".join(_uniq(techs)),
            "max_colors": ",".join(_uniq(maxcols)),
            "template": m.get("print_template") or "",
            "mm2": ",".join(str(a) for a in mm2),
            "cm2": ",".join(cm2),
        }

    # === PRODUCTS (pesi + EAN/GTIN + pms/green/polybag) =========================
    prod_payload = client.get("gateway/products/2.0", accept="text/json", params={"language": LANG})
    products_list = []
    if isinstance(prod_payload, dict):
        for v in prod_payload.values():
            if isinstance(v, list): products_list = v; break
    elif isinstance(prod_payload, list):
        products_list = prod_payload

    sku_to_weights = {}
    master_to_weights = {}
    sku_to_ean = {}
    sku_to_gtin = {}
    sku_to_pms = {}
    sku_to_green = {}
    sku_to_polybag = {}

    for m in products_list:
        mid = m.get("master_id") or ""
        nw = m.get("net_weight")
        gw = m.get("gross_weight")
        gwu = m.get("gross_weight_unit") or "KG"
        if (nw or gw) and mid:
            master_to_weights[mid] = (nw, gw, gwu)

        # fallback a livello master per meta-dati aggiuntivi
        master_pms     = m.get("pms_color") or m.get("pms") or m.get("pantone") or ""
        master_green   = m.get("green") or m.get("is_green") or m.get("sustainable") or ""
        master_polybag = m.get("polybag") or m.get("is_polybag") or m.get("packed_in_polybag") or ""

        for var in (m.get("variants") or []):
            sku = var.get("sku") or ""
            if not sku:
                continue
            nv = var.get("net_weight") or nw
            gv = var.get("gross_weight") or gw
            gu = var.get("gross_weight_unit") or gwu
            sku_to_weights[sku] = (nv, gv, gu)

            # EAN + GTIN
            ean = (
                var.get("ean") or var.get("ean_code") or var.get("ean13") or
                var.get("barcode") or var.get("bar_code") or ""
            )
            gtin = var.get("gtin") or var.get("gtin13") or var.get("gtin14") or ""
            if ean:  sku_to_ean[sku]  = str(ean)
            if gtin: sku_to_gtin[sku] = str(gtin)

            # PMS / GREEN / POLYBAG (variante > master)
            pms     = var.get("pms_color") or var.get("pms") or var.get("pantone") or master_pms
            green   = var.get("green") or var.get("is_green") or var.get("sustainable") or master_green
            polybag = var.get("polybag") or var.get("is_polybag") or var.get("packed_in_polybag") or master_polybag
            if pms   : sku_to_pms[sku]     = str(pms)
            if green : sku_to_green[sku]   = str(green)
            if polybag: sku_to_polybag[sku]= str(polybag)

    # === enrich dataframe ========================================================
    col_mid  = "products__product__product_print_id_2"
    col_mno  = "products__product__product_base_number"
    col_sku  = "products__product__product_number"
    cw_col   = "products__product__packaging_carton__weight"
    cq_col   = "products__product__packaging_carton__carton_quantity"

    # stampa / template / aree
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

    # nuove colonne calcolate
    df["max_print_area_rounded"] = df["max_print_area"].map(_bucket_cm2_list)
    df["products__product__manipulation_man"] = df["products__product__manipulation"].map(
        lambda s: (s.strip() + " man") if str(s).strip() else ""
    )
    df["price"] = df[col_sku].map(lambda s: sku_to_price.get(str(s), ""))

    # EAN/GTIN + pms/green/polybag
    df["products__product__ean"] = df[col_sku].map(lambda s: sku_to_ean.get(str(s), ""))
    df["gtin"]      = df[col_sku].map(lambda s: sku_to_gtin.get(str(s), ""))
    df["pms_color"] = df[col_sku].map(lambda s: sku_to_pms.get(str(s), ""))
    df["green"]     = df[col_sku].map(lambda s: sku_to_green.get(str(s), ""))
    df["polybag"]   = df[col_sku].map(lambda s: sku_to_polybag.get(str(s), ""))

    # pesi per variante e fallback
    def _fill_weights(row):
        sku = row.get(col_sku,"")
        mid = row.get(col_mid,"")
        nw, gw, gwu = "", "", "KG"
        if sku in sku_to_weights:
            nw, gw, gwu = sku_to_weights[sku]
        elif mid in master_to_weights:
            nw, gw, gwu = master_to_weights[mid]
        # forza EU per net/gross
        row["products__product__net_weight"]   = _eu(nw) if nw else ""
        row["products__product__gross_weight"] = _eu(gw) if gw else ""
        row["products__product__gross_weight_unit"] = gwu if (gw or nw) else ""
        return row

    df = df.apply(_fill_weights, axis=1)

    # decimali UE anche per carton cols
    carton_cols_eu = [
        "products__product__packaging_carton__length",
        "products__product__packaging_carton__width",
        "products__product__packaging_carton__height",
        "products__product__packaging_carton__weight",
        "products__product__packaging_carton__volume",
    ]
    for c in carton_cols_eu:
        if c in df.columns:
            df[c] = df[c].map(_eu_clean_numeric)

    # --- CORREZIONE PACKAGING CARTON WEIGHT (se era per pezzo) ------------------
    def _fix_carton_weight(row):
        cw_s = row.get(cw_col, "")
        cq_s = row.get(cq_col, "")
        cw = _to_float(cw_s) if cw_s != "" else None
        try:
            cq = int(float(str(cq_s).replace(",", "."))) if cq_s not in ("", None) else None
        except:
            cq = None

        # prendi gross weight per pezzo preferendo dizionario (più affidabile)
        sku = row.get(col_sku,"")
        mid = row.get(col_mid,"")
        gw = None
        if sku in sku_to_weights:
            gw = _to_float(sku_to_weights[sku][1])
        elif mid in master_to_weights:
            gw = _to_float(master_to_weights[mid][1])
        if gw is None:
            gw = _to_float(row.get("products__product__gross_weight",""))

        if gw and cq:
            computed_cw = gw * cq  # kg
            # Se cartone mancante o evidentemente troppo piccolo/grande, sostituisci
            if (cw is None) or (cw == 0) or (cw < computed_cw * 0.2) or (cw > computed_cw * 5):
                row[cw_col] = _eu(computed_cw)
                row["products__product__packaging_carton__weight_unit"] = "KG"
        return row

    df = df.apply(_fix_carton_weight, axis=1)

    # reorder “gentile” (vicinanze utili)
    cols = list(df.columns)
    for pair in [
        ("products__product__product_number","products__product__ean"),
        ("products__product__ean","gtin"),
        ("products__product__color_description","pms_color"),
        ("products__product__manipulation","products__product__manipulation_man"),
        ("max_print_area","max_print_area_rounded"),
    ]:
        left,right = pair
        if right in cols and left in cols:
            cols.insert(cols.index(left)+1, cols.pop(cols.index(right)))
    df = df[cols]

    # log di controllo
    try:
        sample = df.iloc[0]
        log.info(
            "SAMPLE: ean=%s gtin=%s pms=%s green=%s polybag=%s carton_w=%s qty=%s",
            sample.get("products__product__ean",""),
            sample.get("gtin",""),
            sample.get("pms_color",""),
            sample.get("green",""),
            sample.get("polybag",""),
            sample.get("products__product__packaging_carton__weight",""),
            sample.get("products__product__packaging_carton__carton_quantity",""),
        )
    except Exception as e:
        log.info("SAMPLE unavailable: %s", e)

    # salva + upload
    write_csv(df, OUTFILE)
    dest = upload_file(OUTFILE, "general.csv")
    log.info("Augmented and uploaded general.csv → %s", dest)

if __name__ == "__main__":
    main()

