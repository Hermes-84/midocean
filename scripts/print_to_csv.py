from __future__ import annotations
import os
import pandas as pd
from scripts.midocean_client import MidoceanClient
from scripts.utils import write_csv, log
from scripts.dropbox_uploader import upload_file

OUT = os.getenv("OUT_DIR", "out")
FILENAME = "print.csv"

COLUMNS = [
  "products__product__product_base_number",
  "products__product__product_print_id",
  "products__product__print_express_possible",
  "products__product__item_color_number",
  "products__product__manipulation",
  "products__product__pps__pp__id",
  "products__product__pps__pp__printing_size_unit",
  "products__product__pps__pp__max_print_size_height",
  "products__product__pps__pp__max_print_size_width",
  "products__product__pps__pp__print_position_url__color",
  "products__product__pps__pp__print_position_url__text",
  "products__product__pps__pp__printing_technique__id",
  "products__product__pps__pp__printing_technique__max_colors",
  "products__product__print_position_document",
]

def main():
    client = MidoceanClient()
    data = client.get("gateway/printdata/1.0", accept="text/json")
    rows = []
    for master in data.get("products", []):
        master_code = master.get("master_code")
        master_id = master.get("master_id")
        manipulation = master.get("print_manipulation") or ""
        print_template = master.get("print_template") or ""
        colors = master.get("item_color_numbers", [])
        for pos in master.get("print_positions", []):
            pos_id = pos.get("name") or pos.get("id") or ""
            unit = pos.get("max_print_size_unit") or pos.get("printing_size_unit") or "mm"
            h = pos.get("max_print_size_height") or ""
            w = pos.get("max_print_size_width") or ""
            images = { (img.get("variant_color") or img.get("color") or ""): (img.get("print_position_image_with_area") or img.get("print_position_image_blank") or "") for img in pos.get("images", []) }
            for tech in pos.get("printing_techniques", []):
                tech_id = tech.get("id")
                max_colors = tech.get("max_colors") or tech.get("maximum_colors") or ""
                for c in colors:
                    rows.append({
                        "products__product__product_base_number": master_code,
                        "products__product__product_print_id": master_id,
                        "products__product__print_express_possible": "N",
                        "products__product__item_color_number": c,
                        "products__product__manipulation": manipulation,
                        "products__product__pps__pp__id": pos_id,
                        "products__product__pps__pp__printing_size_unit": unit,
                        "products__product__pps__pp__max_print_size_height": h,
                        "products__product__pps__pp__max_print_size_width": w,
                        "products__product__pps__pp__print_position_url__color": c,
                        "products__product__pps__pp__print_position_url__text": images.get(c, ""),
                        "products__product__pps__pp__printing_technique__id": tech_id,
                        "products__product__pps__pp__printing_technique__max_colors": max_colors,
                        "products__product__print_position_document": print_template,
                    })

    df = pd.DataFrame(rows, columns=COLUMNS)
    out_path = os.path.join(OUT, FILENAME)
    write_csv(df, out_path)
    dest = upload_file(out_path, FILENAME)
    log.info("Uploaded to Dropbox → %s", dest)

if __name__ == "__main__":
    main()
