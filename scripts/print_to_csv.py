# scripts/print_to_csv.py
from __future__ import annotations
import os, pandas as pd
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

def _extract_products(payload):
    # payload può essere dict con "products" oppure lista
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get("products"), list):
            return payload["products"]
        # fallback: prima lista di dict con master_code
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict) and (
                "master_code" in v[0] or "printing_positions" in v[0]
            ):
                return v
    return []

def _first_image_url(images: list) -> str:
    if not isinstance(images, list): return ""
    # preferisci con area, poi blank
    for key in ("print_position_image_with_area","image_with_area","with_area"):
        for im in images:
            u = im.get(key) if isinstance(im, dict) else None
            if u: return u
    for key in ("print_position_image_blank","image_blank","blank"):
        for im in images:
            u = im.get(key) if isinstance(im, dict) else None
            if u: return u
    # qualsiasi url
    for im in images:
        u = im.get("url") if isinstance(im, dict) else None
        if u: return u
    return ""

def main():
    client = MidoceanClient()
    data = client.get("gateway/printdata/1.0", accept="text/json")

    rows = []
    for master in _extract_products(data):
        master_code = master.get("master_code") or ""
        master_id   = master.get("master_id") or ""
        manipulation = master.get("print_manipulation") or ""
        print_template = master.get("print_template") or ""
        colors = master.get("item_color_numbers") or []

        positions = master.get("printing_positions") or master.get("print_positions") or []
        for pos in positions:
            pos_id = pos.get("position_id") or pos.get("name") or pos.get("id") or ""
            unit   = pos.get("print_size_unit") or pos.get("max_print_size_unit") or "mm"
            h      = pos.get("max_print_size_height") or ""
            w      = pos.get("max_print_size_width") or ""
            image_url = _first_image_url(pos.get("images") or [])

            techniques = pos.get("printing_techniques") or []
            for tech in techniques:
                tech_id = tech.get("id") or ""
                max_colors = tech.get("max_colours") or tech.get("maximum_colors") or tech.get("max_colors") or ""
                # una riga per ogni colore item, come nel tuo esempio
                for c in (colors or [""]):
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
                        "products__product__pps__pp__print_position_url__text": image_url,
                        "products__product__pps__pp__printing_technique__id": tech_id,
                        "products__product__pps__pp__printing_technique__max_colors": str(max_colors),
                        "products__product__print_position_document": print_template,
                    })

    df = pd.DataFrame(rows, columns=COLUMNS)
    out_path = os.path.join(OUT, FILENAME)
    write_csv(df, out_path)
    dest = upload_file(out_path, FILENAME)
    log.info("Wrote %s rows → %s; Uploaded → %s", len(df), out_path, dest)

if __name__ == "__main__":
    main()

