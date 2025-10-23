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
    """float -> stringa con virgola come separatore EU."""
    if val in (None, ""): return ""
    try:
        f = float(str(val).replace(",", "."))
        s = f"{f:.3f}".rstrip("0").rstrip(".")
        return s.replace(".", ",")
    except:
        return str(val)

def _eu_clean_numeric(val):
    """Es. '303 KG' -> '303' con formattazione EU."""
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

    # === PRODUCTS (pesi + GTIN/EAN + PMS/GREEN/POLYBAG) =========================
    prod_payload = client.get("gateway/products/2.0", accept="text/json", params={"language": LANG})
    products_list = []
    if isinstance(prod_payload, dict):
        for v in prod_payload.values():
            if isinstance(v, list): products_list = v; break
    elif isinstance(prod_payload, list):
        products_list = prod_payload

    sku_to_weights = {}         # sku -> (net, gross, unit)
    master_to_weights = {}      # master_id -> (net, gross, unit)
    sku_to_ean = {}
    sku_to_gtin = {}
    sku_to_pms = {}
    sku_to_green = {}
    sku_to_polybag = {}

    for m in products_list:
        mid = m.get("master_id") or ""
        # pesi a livello master (outer carton pesi NON qui)
        nw = m.get("net_weight")
        gw = m.get("gross_weight")
        gwu = m.get("gross_weight_unit") or "KG"
        if (nw or gw) and mid:
            master_to_weights[mid] = (nw, gw, gwu)

        # meta master per fallback
        master_pack = m.get("packaging") or {}
        master_pms     = m.get("pms_color") or m.get("pms") or m.get("pantone") or ""
        master_green   = m.get("green") or m.get("is_green") or m.get("sustainable") or ""
        master_polybag = (
            m.get("polybag") or m.get("is_polybag") or m.get("packed_in_polybag")
            or master_pack.get("polybag") or ""
        )

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
            pack = var.get("packaging") or {}
            pms     = var.get("pms_color") or var.get("pms") or var.get("pantone") or master_pms
            green   = var.get("green") or var.get("is_green") or var.get("sustainable") or master_green
            polybag = (
                var.get("polybag") or var.get("is_polybag") or var.get("packed_in_polybag")
                or pack.get("polybag") or master_polybag
            )
            if pms   : sku_to_pms[sku]     = str(pms)
            if green : sku_to_green[sku]   = str(green)
            if polybag: sku_to_polybag[sku]= str(polybag)

    # === enrich dataframe ========================================================
    col_mid  = "products__product__product_print_id_2"
    col_mno  = "products__product__product_base_number"
    col_sku  = "products__product__product_number"
    cl_col   = "products__product__packaging_carton__length"
    cw_col   = "products__product__packaging_carton__width"
    ch_col   = "products__product__packaging_carton__height"
    csize_u  = "products__product__packaging_carton__size_unit"
    cwei_col = "products__product__packaging_carton__weight"
    cwei_u   = "products__product__packaging_carton__weight_unit"
    cvol_col = "products__product__packaging_carton__volume"
    cvol_u   = "products__product__packaging_carton__volume_unit"
    cq_col   = "products__product__packaging_carton__carton_quantity"

    # *** stampa / template / aree
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

    # *** nuove colonne
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

    # *** pesi net/gross per variante (formato EU)
    def _fill_weights(row):
        sku = row.get(col_sku,"")
        mid = row.get(col_mid,"")
        nw, gw, gwu = "", "", "KG"
        if sku in sku_to_weights:
            nw, gw, gwu = sku_to_weights[sku]
        elif mid in master_to_weights:
            nw, gw, gwu = master_to_weights[mid]
        row["products__product__net_weight"]   = _eu(nw) if nw else ""
        row["products__product__gross_weight"] = _eu(gw) if gw else ""
        row["products__product__gross_weight_unit"] = gwu if (gw or nw) else ""
        return row

    df = df.apply(_fill_weights, axis=1)

    # *** decimali EU anche per carton raw cols
    for c in [cl_col, cw_col, ch_col, cwei_col, cvol_col]:
        if c in df.columns:
            df[c] = df[c].map(_eu_clean_numeric)

    # *** calcolo volume da dimensioni, unità M/M3
    def _fill_carton_volume_and_units(row):
        L = _to_float(row.get(cl_col, ""))
        W = _to_float(row.get(cw_col, ""))
        H = _to_float(row.get(ch_col, ""))
        if L and W and H:
            vol = L * W * H  # m3 se le misure sono in metri (specifica midocean)
            row[cvol_col] = _eu(vol)
            row[cvol_u]   = "M3"
            # imposta unità dimensionale a "M"
            row[csize_u] = "M"
        return row

    df = df.apply(_fill_carton_volume_and_units, axis=1)

    # *** correzione carton weight: usa quello API se presente; altrimenti fallback gw_per_item * carton_qty
    def _fix_carton_weight(row):
        cw_val = _to_float(row.get(cwei_col, ""))  # NB: cwei_col è il valore, cwei_u è l'unità
        # (attenzione: sopra abbiamo mappato _eu_clean_numeric su cwei_col)
        cq = None
        try:
            cq = int(float(str(row.get(cq_col, "")).replace(",", "."))) if row.get(cq_col, "") not in ("", None) else None
        except:
            cq = None

        if cw_val and cw_val > 0:
            # Mantieni il peso cartone fornito dall'API
            row[cwei_u] = "KG" if not row.get(cwei_u) else row[cwei_u]
            return row

        # Fallback → gross_weight_per_item * carton_quantity
        sku = row.get(col_sku, "")
        mid = row.get(col_mid, "")
        gw = None
        if sku in sku_to_weights:
            gw = _to_float(sku_to_weights[sku][1])
        elif mid in master_to_weights:
            gw = _to_float(master_to_weights[mid][1])
        if gw and cq:
            row[cwei_col] = _eu(gw * cq)
            row[cwei_u]   = "KG"
        return row

    df = df.apply(_fix_carton_weight, axis=1)

    # --- reorder “gentile” (vicinanze utili)
    cols = list(df.columns)
    for pair in [
        ("products__product__product_number","products__product__ean"),
        ("products__product__ean","gtin"),
        ("products__product__color_description","pms_color"),
        ("products__product__manipulation","products__product__manipulation_man"),
        ("max_print_area","max_print_area_rounded"),
        (cvol_col, cvol_u),
    ]:
        left,right = pair
        if right in cols and left in cols:
            cols.insert(cols.index(left)+1, cols.pop(cols.index(right)))
    # assicura size_unit vicino alle dimensioni
    try:
        i = cols.index(ch_col)
        if csize_u in cols:
            cols.insert(i+1, cols.pop(cols.index(csize_u)))
    except ValueError:
        pass

    df = df[cols]

    # log di controllo
    try:
        sample = df.iloc[0]
        log.info(
            "SAMPLE: gtin=%s polybag=%s carton(LxWxH=%s×%s×%s %s) vol=%s %s weight=%s %s",
            sample.get("gtin",""),
            sample.get("polybag",""),
            sample.get(cl_col,""), sample.get(cw_col,""), sample.get(ch_col,""),
            sample.get(csize_u,""),
            sample.get(cvol_col,""), sample.get(cvol_u,""),
            sample.get(cwei_col,""), sample.get(cwei_u,""),
        )
    except Exception as e:
        log.info("SAMPLE unavailable: %s", e)

    # salva + upload
    write_csv(df, OUTFILE)
    dest = upload_file(OUTFILE, "general.csv")
    log.info("Augmented and uploaded general.csv → %s", dest)

if __name__ == "__main__":
    main()

