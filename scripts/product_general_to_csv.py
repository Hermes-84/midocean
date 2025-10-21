from __future__ import annotations
import os
import pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import now_local, time_hms, write_csv, to_it_decimal, to_upper, log, SUPPLIER

OUT = os.getenv("OUT_DIR", "out")
FILENAME = "generale.csv"

COLUMNS = [
 "midocean_product_info_id","supplier","date","time","language",
 "products__product__product_number","products__product__product_base_number","products__product__product_id",
 "products__product__product_print_id","products__product__product_name","products__product__plcstatus",
 "products__product__short_description","products__product__long_description","products__product__dimensions",
 "products__product__net_weight","products__product__gross_weight","products__product__gross_weight_unit",
 "products__product__color_code","products__product__color_description","products__product__size",
 "products__product__gender","products__product__material_type","products__product__category_code",
 "products__product__category_level_1","products__product__category_level_2","products__product__category_level_3",
 "products__product__category_level_4","products__product__image_url","products__product__thumbnail_url",
 "products__product__commodity_code","products__product__country_of_origin",
 "products__product__packaging_carton__length","products__product__packaging_carton__width",
 "products__product__packaging_carton__height","products__product__packaging_carton__size_unit",
 "products__product__packaging_carton__weight","products__product__packaging_carton__weight_unit",
 "products__product__packaging_carton__volume","products__product__packaging_carton__volume_unit",
 "products__product__packaging_carton__inner_carton_quantity","products__product__packaging_carton__carton_quantity",
 "products__product__digital_assets","products__product__digital_assets__digital_asset__url",
 "products__product__digital_assets__digital_asset__type","products__product__digital_assets__digital_asset__subtype",
 "products__product__digital_assets__digital_asset__001__url","products__product__digital_assets__digital_asset__001__type",
 "products__product__digital_assets__digital_asset__001__subtype","products__product__digital_assets__digital_asset__002__url",
 "products__product__digital_assets__digital_asset__002__type","products__product__digital_assets__digital_asset__002__subtype",
 "products__product__digital_assets__digital_asset__003__url","products__product__digital_assets__digital_asset__003__type",
 "products__product__digital_assets__digital_asset__003__subtype","products__product__digital_assets__digital_asset__004__url",
 "products__product__digital_assets__digital_asset__004__type","products__product__digital_assets__digital_asset__004__subtype",
 "products__product__digital_assets__digital_asset__005__url","products__product__digital_assets__digital_asset__005__type",
 "products__product__digital_assets__digital_asset__005__subtype","products__product__digital_assets__digital_asset__006__url",
 "products__product__digital_assets__digital_asset__006__type","products__product__digital_assets__digital_asset__006__subtype",
 "products__product__digital_assets__digital_asset__007__url","products__product__digital_assets__digital_asset__007__type",
 "products__product__digital_assets__digital_asset__007__subtype","products__product__digital_assets__digital_asset__008__url",
 "products__product__digital_assets__digital_asset__008__type","products__product__digital_assets__digital_asset__008__subtype",
 "products__product__digital_assets__digital_asset__009__url","products__product__digital_assets__digital_asset__009__type",
 "products__product__digital_assets__digital_asset__009__subtype",
 "price","products__product__quantity","products__product__product_print_id_2","products__product__item_color_number",
 "products__product__manipulation","Printing technique","Max colors","Print position document","max_print_area","measuresprintrange","test","translation__of__products__product__color_description"
]

def _pick(d: dict, *keys, default: str = "") -> str:
    for k in keys:
        if k in d and d[k] not in (None, "", "null"):
            return str(d[k])
    return default

def _assets_list(master: dict, variant: dict) -> list[dict]:
    return (variant.get("digital_assets") or master.get("digital_assets") or [])

def _first_asset_by_subtype(assets: list[dict], subtype: str) -> str:
    for a in assets:
        if str(a.get("subtype") or "").lower() == subtype.lower():
            return a.get("url") or ""
    return ""

def _pad_assets(assets: list[dict], n: int = 9):
    urls, types, subs = [], [], []
    for a in (assets or [])[:n]:
        urls.append(a.get("url", ""))
        types.append(a.get("type", ""))
        subs.append(a.get("subtype", ""))
    while len(urls) < n:
        urls.append(""); types.append(""); subs.append("")
    return urls, types, subs

def main():
    client = MidoceanClient()
    lang = os.getenv("MIDOCEAN_LANGUAGE", "it")
    data = client.get("gateway/products/2.0", accept="text/json", params={"language": lang})

    # data può essere dict o list → normalizza
    if isinstance(data, dict):
        products = data.get("products") or data.get("data") or []
    elif isinstance(data, list):
        products = data
    else:
        products = []

    rows = []
    rid = 0
    for master in products:
        master_code = _pick(master, "master_code", "product_base_number", "product_code", "product_number")
        master_id   = _pick(master, "master_id", "product_print_id")
        product_name = _pick(master, "product_name", "name")
        short_desc   = _pick(master, "short_description", "shortDescription")
        long_desc    = _pick(master, "long_description", "longDescription")
        dimensions   = _pick(master, "dimensions", "dimension")
        material     = _pick(master, "material", "material_group", "material_type")
        commodity    = _pick(master, "commodity_code", "hs_code")
        origin       = _pick(master, "country_of_origin", "origin_country")
        cat_code     = _pick(master, "category_code", "category")
        carton_len   = _pick(master, "carton_length", "outer_carton_length")
        carton_wid   = _pick(master, "carton_width", "outer_carton_width")
        carton_hei   = _pick(master, "carton_height", "outer_carton_height")
        carton_unit  = _pick(master, "carton_size_unit", "outer_carton_size_unit")
        carton_wgt   = _pick(master, "gross_weight", "outer_carton_weight")
        carton_wgt_u = _pick(master, "gross_weight_unit", "outer_carton_weight_unit")
        carton_vol   = _pick(master, "volume", "outer_carton_volume")
        carton_vol_u = _pick(master, "volume_unit", "outer_carton_volume_unit")
        inner_qty    = _pick(master, "inner_carton_quantity", "inner_ctn_qty")
        outer_qty    = _pick(master, "outer_carton_quantity", "outer_ctn_qty")

        # categorie (preferisci quelle della variante, se presenti)
        variants = master.get("variants") or master.get("items") or []
        if not variants:
            # fallback: crea una pseudo-variante per non perdere il master
            variants = [dict(sku=master_code)]

        for v in variants:
            rid += 1
            sku = _pick(v, "sku", "product_number", "product_code", default=master_code)
            color_code = _pick(v, "color_code", "colour_code", "item_color_number")
            color_desc = _pick(v, "color_description", "colour_description")
            size_text  = _pick(v, "size_textile", "size")
            gender     = _pick(v, "gender")
            plcstatus  = _pick(v, "plc_status_description", "plcstatus")
            cl1 = _pick(v, "category_level1", "category_level_1", default=_pick(master, "category_level1","category_level_1"))
            cl2 = _pick(v, "category_level2", "category_level_2", default=_pick(master, "category_level2","category_level_2"))
            cl3 = _pick(v, "category_level3", "category_level_3", default=_pick(master, "category_level3","category_level_3"))
            cl4 = _pick(v, "category_level4", "category_level_4", default=_pick(master, "category_level4","category_level_4"))

            assets = _assets_list(master, v)
            front_url = _first_asset_by_subtype(assets, "item_picture_front") or (assets[0]["url"] if assets else "")
            thumb_url = front_url.replace("700X700", "240X240") if "700X700" in front_url else front_url
            urls, types, subs = _pad_assets(assets, 9)

            row = {
                "midocean_product_info_id": rid,
                "supplier": SUPPLIER,
                "date": now_local().strftime("%Y%m%d"),
                "time": time_hms(),
                "language": lang.upper(),
                "products__product__product_number": sku,
                "products__product__product_base_number": master_code,
                "products__product__product_id": _pick(v, "variant_id", "id"),
                "products__product__product_print_id": master_id,
                "products__product__product_name": product_name,
                "products__product__plcstatus": plcstatus,
                "products__product__short_description": short_desc,
                "products__product__long_description": long_desc,
                "products__product__dimensions": dimensions,
                "products__product__net_weight": "",  # non affidabile in API, lo lasciamo vuoto
                "products__product__gross_weight": "", # idem
                "products__product__gross_weight_unit": "",
                "products__product__color_code": color_code,
                "products__product__color_description": color_desc,
                "products__product__size": size_text,
                "products__product__gender": gender,
                "products__product__material_type": material,
                "products__product__category_code": cat_code,
                "products__product__category_level_1": cl1,
                "products__product__category_level_2": cl2,
                "products__product__category_level_3": cl3,
                "products__product__category_level_4": cl4,
                "products__product__image_url": front_url,
                "products__product__thumbnail_url": thumb_url,
                "products__product__commodity_code": commodity,
                "products__product__country_of_origin": origin,
                "products__product__packaging_carton__length": carton_len,
                "products__product__packaging_carton__width": carton_wid,
                "products__product__packaging_carton__height": carton_hei,
                "products__product__packaging_carton__size_unit": to_upper(carton_unit),
                "products__product__packaging_carton__weight": carton_wgt,
                "products__product__packaging_carton__weight_unit": to_upper(carton_wgt_u),
                "products__product__packaging_carton__volume": carton_vol,
                "products__product__packaging_carton__volume_unit": to_upper(carton_vol_u),
                "products__product__packaging_carton__inner_carton_quantity": inner_qty,
                "products__product__packaging_carton__carton_quantity": outer_qty,
                "products__product__digital_assets": "",
                "products__product__digital_assets__digital_asset__url": urls[0],
                "products__product__digital_assets__digital_asset__type": types[0],
                "products__product__digital_assets__digital_asset__subtype": subs[0],
                "products__product__digital_assets__digital_asset__001__url": urls[0],
                "products__product__digital_assets__digital_asset__001__type": types[0],
                "products__product__digital_assets__digital_asset__001__subtype": subs[0],
                "products__product__digital_assets__digital_asset__002__url": urls[1],
                "products__product__digital_assets__digital_asset__002__type": types[1],
                "products__product__digital_assets__digital_asset__002__subtype": subs[1],
                "products__product__digital_assets__digital_asset__003__url": urls[2],
                "products__product__digital_assets__digital_asset__003__type": types[2],
                "products__product__digital_assets__digital_asset__003__subtype": subs[2],
                "products__product__digital_assets__digital_asset__004__url": urls[3],
                "products__product__digital_assets__digital_asset__004__type": types[3],
                "products__product__digital_assets__digital_asset__004__subtype": subs[3],
                "products__product__digital_assets__digital_asset__005__url": urls[4],
                "products__product__digital_assets__digital_asset__005__type": types[4],
                "products__product__digital_assets__digital_asset__005__subtype": subs[4],
                "products__product__digital_assets__digital_asset__006__url": urls[5],
                "products__product__digital_assets__digital_asset__006__type": types[5],
                "products__product__digital_assets__digital_asset__006__subtype": subs[5],
                "products__product__digital_assets__digital_asset__007__url": urls[6],
                "products__product__digital_assets__digital_asset__007__type": types[6],
                "products__product__digital_assets__digital_asset__007__subtype": subs[6],
                "products__product__digital_assets__digital_asset__008__url": urls[7],
                "products__product__digital_assets__digital_asset__008__type": types[7],
                "products__product__digital_assets__digital_asset__008__subtype": subs[7],
                "products__product__digital_assets__digital_asset__009__url": urls[8],
                "products__product__digital_assets__digital_asset__009__type": types[8],
                "products__product__digital_assets__digital_asset__009__subtype": subs[8],
                "price": "",
                "products__product__quantity": "",
                "products__product__product_print_id_2": master_id,
                "products__product__item_color_number": color_code,
                "products__product__manipulation": "",
                "Printing technique": "",
                "Max colors": "",
                "Print position document": "",
                "max_print_area": "",
                "measuresprintrange": "",
                "test": "",
                "translation__of__products__product__color_description": color_desc,
            }
            rows.append(row)

    df = pd.DataFrame(rows, columns=COLUMNS)
    out_path = os.path.join(OUT, FILENAME)
    write_csv(df, out_path)
    from scripts.dropbox_uploader import upload_file
    dest = upload_file(out_path, FILENAME)
    log.info("Uploaded to Dropbox → %s", dest)

if __name__ == "__main__":
    main()

# scripts/augment_general.py
from __future__ import annotations
import os, math, pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import write_csv, log

OUT = os.getenv("OUT_DIR", "out")
INFILE = os.path.join(OUT, "general.csv")
OUTFILE = os.path.join(OUT, "general.csv")  # sovrascrive

def _eu(val: str|int|float|None) -> str:
    if val is None or val == "": return ""
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
    # ordina desc per coerenza
    mm2.sort(reverse=True)
    cm2.sort(key=lambda s: int(s[:-4]), reverse=True)
    return mm2, cm2

def main():
    if not os.path.exists(INFILE):
        raise SystemExit(f"Missing input {INFILE} (run product_general_to_csv.py first)")
    df = pd.read_csv(INFILE, dtype=str).fillna("")

    client = MidoceanClient()

    # --- PRICE (Product Pricelist 2.0) ---
    price_payload = client.get("gateway/pricelist/2.0", accept="text/json")  # EUR, price:[{sku, price, scale:[...]}]
    price_rows = price_payload.get("price") if isinstance(price_payload, dict) else []
    sku_to_price = {}
    for r in price_rows or []:
        # prendi il prezzo con minimum_quantity == 1 se presente, altrimenti 'price'
        base = r.get("price")
        for sc in r.get("scale") or []:
            if str(sc.get("minimum_quantity")) == "1":
                base = sc.get("price") or base
                break
        # la API ti dà già la virgola decimale
        sku_to_price[str(r.get("sku"))] = base or ""

    # --- PRINT DATA (Print Data 1.0) ---
    pd_payload = client.get("gateway/printdata/1.0", accept="text/json")
    # trova lista prodotti
    if isinstance(pd_payload, dict) and isinstance(pd_payload.get("products"), list):
        pd_products = pd_payload["products"]
    elif isinstance(pd_payload, list):
        pd_products = pd_payload
    else:
        # cerca una lista plausibile
        pd_products = []
        if isinstance(pd_payload, dict):
            for v in pd_payload.values():
                if isinstance(v, list) and v and isinstance(v[0], dict) and (
                    "master_code" in v[0] or "printing_positions" in v[0]
                ):
                    pd_products = v; break

    # aggrega per master_code e master_id
    agg = {}
    for m in pd_products:
        master_code = m.get("master_code") or ""
        master_id   = m.get("master_id") or ""
        key = master_id or master_code
        pos = m.get("printing_positions") or m.get("print_positions") or []
        mm2, cm2 = _areas_from_positions(pos)
        techs = []
        colors = []
        for p in pos:
            for t in (p.get("printing_techniques") or []):
                if t.get("id"): techs.append(str(t["id"]))
                if t.get("max_colours") or t.get("maximum_colors") or t.get("max_colors"):
                    colors.append(str(t.get("max_colours") or t.get("maximum_colors") or t.get("max_colors")))
        # unici ordinati
        def uniq(seq): 
            seen=set(); out=[]
            for x in seq:
                if x not in seen: seen.add(x); out.append(x)
            return out
        agg[key] = {
            "manipulation": (m.get("print_manipulation") or ""),
            "techniques": ",".join(uniq(techs)),
            "max_colors": ",".join(uniq(colors)),
            "template": m.get("print_template") or "",
            "mm2": ",".join(str(a) for a in mm2),
            "cm2": ",".join(cm2),
        }

    # --- PRODUCTS (per net/gross weight fallback) ---
    prod_payload = client.get(f"gateway/products/2.0?language={os.getenv('MIDOCEAN_LANGUAGE','en')}", accept="text/json")
    # indicizza per sku e per master_id
    sku_to_weights = {}
    master_to_weights = {}
    products_list = []
    if isinstance(prod_payload, dict):
        for v in prod_payload.values():
            if isinstance(v, list): products_list = v; break
    elif isinstance(prod_payload, list):
        products_list = prod_payload

    for m in products_list:
        master_id = m.get("master_id") or ""
        nw = m.get("net_weight")
        gw = m.get("gross_weight")
        gwu = m.get("gross_weight_unit") or "KG"
        if nw or gw:
            master_to_weights[master_id] = (nw, gw, gwu)
        for var in (m.get("variants") or []):
            sku = var.get("sku") or ""
            nv = var.get("net_weight") or nw
            gv = var.get("gross_weight") or gw
            gu = var.get("gross_weight_unit") or gwu
            if sku:
                sku_to_weights[sku] = (nv, gv, gu)

    # --- Enrichment riga per riga ---
    def to_float_or_none(s):
        try: return float(str(s).replace(",", "."))
        except: return None

    # colonne presenti nel tuo CSV
    col_master_id   = "products__product__product_print_id_2"
    col_master_code = "products__product__product_base_number"
    col_sku         = "products__product__product_number"

    # per carton fallback gross = carton_weight / carton_qty
    cw_col = "products__product__packaging_carton__weight"
    cq_col = "products__product__packaging_carton__carton_quantity"

    # riempi colonne richieste
    df["products__product__manipulation"] = df.apply(
        lambda r: agg.get(r.get(col_master_id) or r.get(col_master_code),{}).get("manipulation",""), axis=1)

    df["Printing technique"] = df.apply(
        lambda r: agg.get(r.get(col_master_id) or r.get(col_master_code),{}).get("techniques",""), axis=1)

    df["Max colors"] = df.apply(
        lambda r: agg.get(r.get(col_master_id) or r.get(col_master_code),{}).get("max_colors",""), axis=1)

    df["Print position document"] = df.apply(
        lambda r: agg.get(r.get(col_master_id) or r.get(col_master_code),{}).get("template",""), axis=1)

    df["measuresprintrange"] = df.apply(
        lambda r: agg.get(r.get(col_master_id) or r.get(col_master_code),{}).get("mm2",""), axis=1)

    df["max_print_area"] = df.apply(
        lambda r: agg.get(r.get(col_master_id) or r.get(col_master_code),{}).get("cm2",""), axis=1)

    # PRICE per SKU
    df["price"] = df[col_sku].map(lambda s: sku_to_price.get(str(s), ""))

    # WEIGHTS
    def _fill_weights(row):
        sku = row.get(col_sku,"")
        mid = row.get(col_master_id,"")
        nw, gw, gwu = "", "", "KG"
        if sku in sku_to_weights:
            nw, gw, gwu = sku_to_weights[sku]
        elif mid in master_to_weights:
            nw, gw, gwu = master_to_weights[mid]
        # fallback gross = carton_weight/carton_qty
        if not gw:
            cw = to_float_or_none(row.get(cw_col))
            cq = to_float_or_none(row.get(cq_col))
            if cw and cq and cq>0:
                gw = cw / cq
                gwu = "KG"
        # format EU
        row["products__product__net_weight"] = _eu(nw) if nw else ( _eu(gw) if gw else "" )
        row["products__product__gross_weight"] = _eu(gw) if gw else ""
        row["products__product__gross_weight_unit"] = gwu if (gw or nw) else ""
        return row

    df = df.apply(_fill_weights, axis=1)

    # salva sovrascrivendo
    write_csv(df, OUTFILE)
    log.info("Augmented general.csv → %s", OUTFILE)

if __name__ == "__main__":
    main()

