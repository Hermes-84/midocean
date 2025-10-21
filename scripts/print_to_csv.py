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

def _pick(d: dict, *keys, default: str = "") -> str:
    """Ritorna il primo valore non vuoto tra le chiavi fornite, come stringa."""
    for k in keys:
        if k in d and d[k] is not None and str(d[k]).strip() != "":
            return str(d[k])
    return default

def _normalize_colors(colors) -> list[str]:
    """Accetta lista o stringa ('01;03;85', '01,03,85') → lista di stringhe pulite."""
    if colors is None:
        return []
    if isinstance(colors, list):
        return [str(c).strip() for c in colors if str(c).strip() != ""]
    s = str(colors)
    for sep in [";", ",", "|", " "]:
        s = s.replace(sep, ",")
    return [x.strip() for x in s.split(",") if x.strip() != ""]

def main():
    client = MidoceanClient()
    data = client.get("gateway/printdata/1.0", accept="text/json")

    products = data.get("products") or data.get("data") or (data if isinstance(data, list) else [])
    rows = []

    for master in products:
        master_code = _pick(master, "master_code", "product_base_number", "product_base_no", "product_code", "product_number")
        master_id   = _pick(master, "master_id", "product_print_id")
        manipulation = _pick(master, "print_manipulation", "manipulation")

        # PDF template (varianti chiave note)
        print_template = _pick(
            master,
            "print_template_url",
            "print_template",
            "print_position_document",
            "print_position_document_url",
            "template_pdf_url"
        )

        # colori disponibili per questa master (numero colore variante)
        colors = _normalize_colors(
            master.get("item_color_numbers")
            or master.get("item_colour_numbers")
            or master.get("variant_colors")
            or master.get("variant_colours")
            or master.get("colors")
            or master.get("colours")
        )

        for pos in master.get("print_positions", []):
            # id/nome posizione
            pos_id = _pick(pos, "id", "position_id", "name", "position_name", "position")

            # unità e dimensioni massime
            unit = _pick(pos, "printing_size_unit", "print_size_unit", "max_print_size_unit", default="mm")
            h = _pick(pos, "max_print_size_height", "height", "print_height", "max_height")
            w = _pick(pos, "max_print_size_width",  "width",  "print_width",  "max_width")

            # immagini per posizione/colore (preferisci "with area" se presente)
            imgs_list = pos.get("images") or pos.get("print_position_images") or []
            def _img_url(img: dict) -> str:
                return _pick(
                    img,
                    "print_position_image_with_area",
                    "print_position_image_with_area_url",
                    "print_position_image_blank",
                    "print_position_image_blank_url",
                    "url"
                )
            def _img_key(img: dict) -> str:
                return _pick(img, "variant_color", "variant_colour", "color", "colour")
            images = {}
            default_img = ""
            for img in imgs_list:
                url = _img_url(img)
                if not default_img and url:
                    default_img = url
                key = _img_key(img)
                if key:
                    images[key] = url

            # tecniche di stampa
            for tech in pos.get("printing_techniques", []):
                tech_id = _pick(tech, "id", "technique_id", "printing_technique_id", "code")
                max_colors = _pick(tech, "max_colors", "max_colours", "maximum_colors", "maximum_colours", "max")

                # una riga per ciascun colore
                for c in (colors or [""]):
                    c_str = str(c) if c is not None else ""
                    rows.append({
                        "products__product__product_base_number": master_code,
                        "products__product__product_print_id": master_id,
                        "products__product__print_express_possible": "N",
                        "products__product__item_color_number": c_str,
                        "products__product__manipulation": manipulation,
                        "products__product__pps__pp__id": pos_id,
                        "products__product__pps__pp__printing_size_unit": unit,
                        "products__product__pps__pp__max_print_size_height": h,
                        "products__product__pps__pp__max_print_size_width": w,
                        "products__product__pps__pp__print_position_url__color": c_str,
                        "products__product__pps__pp__print_position_url__text": images.get(c_str, default_img),
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
