from __future__ import annotations
import os
import pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import now_local, write_csv, to_it_decimal, to_upper, log, SUPPLIER
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

def first_asset(assets: list, subtype: str) -> str:
    for a in (assets or []):
        if a.get("subtype") == subtype:
            return a.get("url") or ""
    return ""

def pad_assets(assets: list, n: int = 9):
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
    data = client.get("gateway/products/2.0", params={"language": lang})
    products = data.get("products") or data.get("data") or []
    if not products:
        products = data if isinstance(data, list) else []
    rows = []
    rid = 0
    for master in products:
        master_code = master.get("master_code")
        master_id = master.get("master_id")
        for v in master.get("variants", []):
            rid += 1
            sku = v.get("sku")
            assets_master = master.get("digital_assets", [])
            assets_variant = v.get("digital_assets", [])
            assets = assets_variant or assets_master
            url_front = first_asset(assets, "item_picture_front")
            url_thumb = url_front.replace("700X700", "240X240") if url_front else ""
            urls, types, subs = pad_assets(assets, 9)
            cat_code = master.get("category_code")
            cl1 = v.get("category_level1") or master.get("category_level1")
            cl2 = v.get("category_level2") or master.get("category_level2")
            cl3 = v.get("category_level3") or master.get("category_level3")
            cl4 = v.get("category_level4") or master.get("category_level4")

            row = {
                "midocean_product_info_id": rid,
                "supplier": os.getenv("SUPPLIER_NAME", "Mid Ocean Brands"),
                "date": now_local().strftime("%Y%m%d"),
                "time": now_local().strftime("%H:%M:%S"),
                "language": lang.upper(),
                "products__product__product_number": sku,
                "products__product__product_base_number": master_code,
                "products__product__product_id": v.get("variant_id"),
                "products__product__product_print_id": master_id,
                "products__product__product_name": master.get("product_name"),
                "products__product__plcstatus": v.get("plc_status_description"),
                "products__product__short_description": master.get("short_description"),
                "products__product__long_description": master.get("long_description"),
                "products__product__dimensions": master.get("dimensions"),
                "products__product__net_weight": "",  # to_it_decimal(master.get("net_weight")) if present
                "products__product__gross_weight": "", # to_it_decimal(master.get("gross_weight"))
                "products__product__gross_weight_unit": "", # to_upper(master.get("gross_weight_unit"))
                "products__product__color_code": v.get("color_code"),
                "products__product__color_description": v.get("color_description"),
                "products__product__size": v.get("size_textile") or v.get("size") or "",
                "products__product__gender": v.get("gender") or "",
                "products__product__material_type": master.get("material") or master.get("material_group") or "",
                "products__product__category_code": cat_code,
                "products__product__category_level_1": cl1,
                "products__product__category_level_2": cl2,
                "products__product__category_level_3": cl3,
                "products__product__category_level_4": cl4,
                "products__product__image_url": url_front,
                "products__product__thumbnail_url": url_thumb,
                "products__product__commodity_code": master.get("commodity_code"),
                "products__product__country_of_origin": master.get("country_of_origin"),
                "products__product__packaging_carton__length": "",  # to_it_decimal(master.get("carton_length"))
                "products__product__packaging_carton__width": "",   # to_it_decimal(master.get("carton_width"))
                "products__product__packaging_carton__height": "",  # to_it_decimal(master.get("carton_height"))
                "products__product__packaging_carton__size_unit": "", # to_upper(master.get("carton_length_unit"))
                "products__product__packaging_carton__weight": "",    # to_it_decimal(master.get("gross_weight"))
                "products__product__packaging_carton__weight_unit": "", # to_upper(master.get("gross_weight_unit"))
                "products__product__packaging_carton__volume": "",     # to_it_decimal(master.get("volume"))
                "products__product__packaging_carton__volume_unit": "",# to_upper(master.get("volume_unit"))
                "products__product__packaging_carton__inner_carton_quantity": master.get("inner_carton_quantity"),
                "products__product__packaging_carton__carton_quantity": master.get("outer_carton_quantity"),
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
                "products__product__item_color_number": v.get("color_code"),
                "products__product__manipulation": "",
                "Printing technique": "",
                "Max colors": "",
                "Print position document": "",
                "max_print_area": "",
                "measuresprintrange": "",
                "test": "",
                "translation__of__products__product__color_description": v.get("color_description"),
            }
            rows.append(row)

    df = pd.DataFrame(rows, columns=COLUMNS)
    out_path = os.path.join(OUT, FILENAME)
    write_csv(df, out_path)
    dest = upload_file(out_path, FILENAME)
    log.info("Uploaded to Dropbox → %s", dest)

if __name__ == "__main__":
    main()
