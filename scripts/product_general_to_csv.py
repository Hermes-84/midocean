from __future__ import annotations
import os
import pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import now_local, time_hms, write_csv, to_upper, log
from scripts.dropbox_uploader import upload_file

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
    payload = client.get("gateway/products/2.0", accept="text/json", params={"language": lang})

    # normalizza in lista prodotti
    if isinstance(payload, dict):
        products = payload.get("products") or payload.get("data") or []
    elif isinstance(payload, list):
        products = payload
    else:
        products = []

    rows = []
    rid = 0
    supplier = os.getenv("SUPPLIER_NAME", "Mid Ocean Brands")
    today = now_local().strftime("%Y%m%d")
    hhmmss = time_hms()

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
        carton_wgt   = _pick(master, "outer_carton_weight", "gross_weight")
        carton_wgt_u = _pick(master, "outer_carton_weight_unit", "gross_weight_unit")
        carton_vol   = _pick(master, "outer_carton_volume", "volume")
        carton_vol_u = _pick(master, "outer_carton_volume_unit", "volume_unit")
        inner_qty    = _pick(master, "inner_carton_quantity", "inner_ctn_qty")
        outer_qty    = _pick(master, "outer_carton_quantity", "outer_ctn_qty")

        variants = master.get("variants") or master.get("items") or []
        if not variants:
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
                "supplier": supplier,
                "date": today,
                "time": hhmmss,
                "language": lang.upper(),
                "products__product__product_number": sku,
                "products__product__product_base_number": master_code,
                "products__product__product_id": _pick(v, "variant_id", "id"),
                "products__product__product_print_id": master_id,
                "products__product__product

