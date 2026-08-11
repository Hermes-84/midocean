from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import build_exports as base

LEGACY_HEADERS = base.LEGACY_HEADERS
DPO_HEADERS = base.DPO_HEADERS

LANG_PREFIXES = ("DA_", "DE_", "EN_", "ES_", "FI_", "FR_", "NL_", "NO_", "PL_", "SE_", "PT_")
TRUE_VALUES = {"1", "true", "yes", "y"}
FALSE_VALUES = {"0", "false", "no", "n"}
NUMERIC_HINTS = re.compile(
    r"(?:^|__|_)(?:id|code|ean|hs|rgb|price|start|end|minimum|maximum|quantity|qty|"
    r"number|count|pcs|weight|length|width|height|diameter|volume|capacity|mah|time|day|days|"
    r"volt|voltage|watt|power|size|level|parent|pass|passes|from|till)(?:$|__|_)", re.I
)


def norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


LEGACY_DIRECT = {
    norm(h[len("products__product__"):])
    for h in LEGACY_HEADERS
    if h.startswith("products__product__") and "__" not in h[len("products__product__"):]
}


def direct_dict(elem: ET.Element | None) -> dict[str, str]:
    if elem is None:
        return {}
    return {c.tag: (c.text or "").strip() for c in list(elem) if not list(c)}


def load_print(path: Path):
    root = ET.parse(path).getroot()
    rows, by_product = [], defaultdict(list)
    products = root.find("Products")
    if products is None:
        return rows, by_product
    for product in products.findall("Product"):
        pid = base.text(product, "Product_Code")
        positions = product.find("Positions")
        if positions is None:
            continue
        for pos in positions.findall("Position"):
            row = direct_dict(pos)
            row["Product_Code"] = pid
            rows.append(row)
            by_product[pid].append(row)
    return rows, by_product


def load_indexes(source_root: Path):
    data = base.load_indexes(source_root)
    v3 = source_root / "feed-v3"

    root = ET.parse(v3 / "Products_v3.xml").getroot()
    data["products_v3"], data["v3_rows_by_pid"] = {}, defaultdict(list)
    for node in root.find("Products").findall("Product"):
        pid, color = base.text(node, "Product_Id"), base.text(node, "Color_Code")
        data["products_v3"][(pid, color)] = node
        data["v3_rows_by_pid"][pid].append(node)

    colors_root = ET.parse(v3 / "colors.xml").getroot()
    data["colors_v3"] = {
        (base.text(node, "Product_id"), base.text(node, "Color_Code")): direct_dict(node)
        for node in colors_root.find("Colors").findall("Color")
    }

    cats_root = ET.parse(v3 / "categories.xml").getroot()
    data["categories_v3"] = {
        base.text(node, "Category_Id"): direct_dict(node)
        for node in cats_root.find("Categories").findall("Category")
    }

    trans_root = ET.parse(v3 / "ProductTranslations_v3.xml").getroot()
    data["translations_v3"] = {base.text(n, "code"): n for n in trans_root.findall("product")}

    data["print_v3_rows"], data["print_v3_by_product"] = load_print(v3 / "Print_v3.xml")

    pos_root = ET.parse(v3 / "PositionTranslations_v3.xml").getroot()
    data["position_it_v3"] = {
        base.text(n, "Position"): base.text(n, "IT_Position_Title")
        for n in pos_root.findall("Position")
    }
    return data


def v3_direct_value(node: ET.Element | None, header: str) -> str:
    if node is None or not header.startswith("products__product__"):
        return ""
    suffix = header[len("products__product__"):]
    if "__" in suffix:
        return ""
    wanted = norm(suffix)
    for child in list(node):
        if not list(child) and norm(child.tag) == wanted:
            return (child.text or "").strip()
    return ""


def aggregate_v3_print(rows: list[dict[str, str]]) -> dict[str, str]:
    def vals(name):
        return [row.get(name, "") for row in rows]
    return {
        "sub_article": base.smart_join(vals("Sub_Article")),
        "passes": base.smart_join(vals("Passes")),
    }


def actual_variants(data, product: ET.Element):
    pid = base.text(product, "Product_Id")
    colors = product.find("Colors")
    variants = colors.findall("Color") if colors is not None else []
    if variants:
        return [(n.attrib.get("id", ""), n) for n in variants]
    return [(base.text(n, "Color_Code"), None) for n in data["v3_rows_by_pid"].get(pid, [])]


def strict_legacy_row(data, product: ET.Element, color_code: str, color_node: ET.Element | None):
    pid = base.text(product, "Product_Id")
    row = base.build_legacy_row(data, product, color_node)
    v3 = data["products_v3"].get((pid, color_code))

    # Source identity: color code comes from V4 when present, otherwise actual V3 row.
    row["products__product__color_code"] = color_code
    if color_node is None and v3 is not None:
        row["products__product__image_color"] = base.text(v3, "Image_Color")
        row["products__product__colors"] = base.text(v3, "Colors")

    # Remove mappings that were previously inferred/constructed rather than exact
    # supplier fields. Exact V3 fields are filled below when available.
    exact_v4 = {
        "products__product__refill": base.translation_it(data["translations"], pid, "Refill"),
        "products__product__tip": base.translation_it(data["translations"], pid, "Tip"),
        "products__product__dishwasher_resistant": base.translation_it(data["translations"], pid, "Dishwasher_Resistant"),
        "products__product__microwave_safe": base.translation_it(data["translations"], pid, "Microwave_Safe"),
    }
    reset = {
        "products__product__dangerous_goods_surcharge",
        "products__product__print_possible",
        "products__product__express_delivery",
        "products__product__standard_writing_colors",
        "products__product__batteries",
        "products__product__sheets",
        "products__product__gift_box",
        "products__product__refillable",
        "products__product__suitable_hot_drinks",
        "products__product__leak_free",
        "products__product__cables_included",
        "products__product__input_volt",
        "products__product__capacity_mah",
        "products__product__social_compliance",
        "products__product__letterbox",
        "products__product__brands",
        "products__product__positions__position__sub_article",
        "products__product__positions__position__passes",
    }
    for header in reset:
        row[header] = ""
    row.update(exact_v4)

    # V3 exact-name fallback only. No semantic guessing.
    for header in LEGACY_HEADERS:
        if not row.get(header):
            value = v3_direct_value(v3, header)
            if value:
                row[header] = value

    # Print_v4 removed Passes/Sub_Article; Print_v3 still carries those exact fields.
    p3 = aggregate_v3_print(data["print_v3_by_product"].get(pid, []))
    if not row["products__product__positions__position__sub_article"]:
        row["products__product__positions__position__sub_article"] = p3["sub_article"]
    if not row["products__product__positions__position__passes"]:
        row["products__product__positions__position__passes"] = p3["passes"]

    # Product-specific V3 color fallback only when V4 color table lacks the field.
    c3 = data["colors_v3"].get((pid, color_code), {})
    if not row["products__product__it_color_description"]:
        row["products__product__it_color_description"] = c3.get("IT_Title", "")
    if not row["IT_Color_Base_Title"]:
        row["IT_Color_Base_Title"] = c3.get("IT_Color_Base_Title", "")
    if not row["IT_Color_Base2_Title"]:
        row["IT_Color_Base2_Title"] = c3.get("IT_Color_Base2_Title", "")
    return row


def keep_it_and_neutral(flat: dict[str, str]):
    out = {}
    for key, value in flat.items():
        leaf = key.split("__")[-1]
        if leaf.startswith("IT_") or not leaf.startswith(LANG_PREFIXES):
            out[key] = value
    return out


def v3_new_fields(v3: ET.Element | None):
    if v3 is None:
        return {}
    out = {}
    for child in list(v3):
        value = (child.text or "").strip()
        if list(child) or not value or norm(child.tag) in LEGACY_DIRECT:
            continue
        out[f"v3__{child.tag}"] = value
    return out


def booleanize(rows, headers):
    bool_headers = set()
    for header in headers:
        if NUMERIC_HINTS.search(header):
            continue
        values = {str(r.get(header, "")).strip().lower() for r in rows if str(r.get(header, "")).strip()}
        if values and values.issubset(TRUE_VALUES | FALSE_VALUES):
            bool_headers.add(header)
    for row in rows:
        for header in bool_headers:
            value = str(row.get(header, "")).strip().lower()
            if value in TRUE_VALUES:
                row[header] = "Sì"
            elif value in FALSE_VALUES:
                row[header] = "No"


def dedupe_columns(rows, candidates):
    seen = {tuple(str(r.get(h, "")).strip() for r in rows): h for h in LEGACY_HEADERS}
    kept, dropped = [], []
    for header in candidates:
        sig = tuple(str(r.get(header, "")).strip() for r in rows)
        if sig in seen:
            dropped.append((header, seen[sig]))
        else:
            seen[sig] = header
            kept.append(header)
    return kept, dropped


def build_products(data, destination: Path):
    rows, candidates, candidate_seen = [], [], set()

    product_flat = {}
    it_flat = {}
    for product in data["products"]:
        pid = base.text(product, "Product_Id")
        product_flat[pid] = base.flatten(product, "Product", skip_direct={"Colors"})
        trans = data["translations"].get(pid)
        it_flat[pid] = base.flatten_it_translation(trans) if trans is not None else {}

    def add(row, key, value):
        row[key] = value
        if key not in candidate_seen:
            candidate_seen.add(key)
            candidates.append(key)

    for product in data["products"]:
        pid = base.text(product, "Product_Id")
        category_ids = [base.text(n, ".") for n in product.findall("./Categories/Category_Id")]
        category_agg = defaultdict(list)
        for cid in category_ids:
            for key, value in keep_it_and_neutral(data["category_flat"].get(cid, {})).items():
                category_agg[key].append(value)

        for color_code, color_node in actual_variants(data, product):
            row = strict_legacy_row(data, product, color_code, color_node)
            v3 = data["products_v3"].get((pid, color_code))

            for key, value in product_flat[pid].items():
                add(row, f"v4__{key}", value)
            add(row, "v4__Color__@id", color_code)
            add(row, "v4__Color__EAN_Code", base.text(color_node, "EAN_Code") if color_node is not None else "")
            add(row, "v4__Color__Image_Color", base.text(color_node, "Image_Color") if color_node is not None else "")

            for key, value in keep_it_and_neutral(data["color_flat"].get(color_code, {})).items():
                add(row, f"v4_color__{key}", value)
            for key, values in category_agg.items():
                add(row, f"v4_category__{key}", base.unique_join(values, ", "))
            for key, value in it_flat[pid].items():
                clean = key[9:] if key.startswith("product__") else key
                add(row, f"v4_it__{clean}", value)

            price = data["prices"].get((pid, color_code), {})
            add(row, "v4_price__Excluded_From_Discount", price.get("excluded", ""))
            for i, tier in enumerate(price.get("tiers", []), 1):
                for field in ("Start", "End", "Price"):
                    add(row, f"v4_price__tier_{i}__{field}", tier.get(field, ""))

            for key, value in v3_new_fields(v3).items():
                add(row, key, value)
            rows.append(row)

    booleanize(rows, LEGACY_HEADERS + candidates)
    kept, dropped = dedupe_columns(rows, candidates)
    headers = LEGACY_HEADERS + kept

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return {"rows": len(rows), "columns": len(headers), "path": str(destination), "dropped_duplicate_columns": dropped}


def build_dpo(data, destination: Path):
    rows, seen = [], set()
    for pos in data["print_rows"]:
        pid = pos.get("Product_Code", "")
        height, width, diameter = pos.get("Height", ""), pos.get("Width", ""), pos.get("Diameter", "")
        dimensions = f"diam {diameter} mm" if diameter not in {"", "0"} and height in {"", "0"} and width in {"", "0"} else f"{height}x{width} mm"
        group = pos.get("Print_Group", "")
        original = pos.get("Position", "")
        translated = data["position_it"].get(original) or data["position_it_v3"].get(original) or original
        image = pos.get("Image_Name", "")
        image = "/" + image.rsplit("/", 1)[-1] if image else ""
        if not image or image == "/":
            continue
        key = (pid, dimensions, group, translated, image)
        if key in seen:
            continue
        seen.add(key)
        rows.append({DPO_HEADERS[0]: pid, DPO_HEADERS[1]: dimensions, DPO_HEADERS[2]: group, DPO_HEADERS[3]: translated, DPO_HEADERS[4]: image, DPO_HEADERS[5]: ""})

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=DPO_HEADERS)
        writer.writeheader(); writer.writerows(rows)
    return {"rows": len(rows), "columns": len(DPO_HEADERS), "path": str(destination)}


def build_from_source_root(source_root: Path, output_dir: Path):
    data = load_indexes(source_root)
    products = build_products(data, output_dir / "Products.csv")
    dpo = build_dpo(data, output_dir / "DPO PRINT.csv")
    print(f"Products: {products['rows']} righe, {products['columns']} colonne")
    print(f"DPO PRINT: {dpo['rows']} righe")
    print(f"Doppioni rimossi: {len(products['dropped_duplicate_columns'])}")
    return {"products": products, "dpo_print": dpo}
