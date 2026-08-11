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
    r"volt|voltage|watt|power|size|level|parent|pass|passes|till)(?:$|__|_)", re.I
)


def norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


LEGACY_DIRECT = {
    norm(h[len("products__product__"):]): h
    for h in LEGACY_HEADERS
    if h.startswith("products__product__") and "__" not in h[len("products__product__"):]
}


def direct_dict(elem: ET.Element | None) -> dict[str, str]:
    if elem is None:
        return {}
    return {c.tag: (c.text or "").strip() for c in list(elem) if not list(c)}


def direct_exact(elem: ET.Element | None, legacy_header: str) -> str:
    if elem is None or not legacy_header.startswith("products__product__"):
        return ""
    suffix = legacy_header[len("products__product__"):]
    if "__" in suffix:
        return ""
    wanted = norm(suffix)
    for child in list(elem):
        if not list(child) and norm(child.tag) == wanted:
            return (child.text or "").strip()
    return ""


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


def actual_variants(data, product: ET.Element):
    pid = base.text(product, "Product_Id")
    colors = product.find("Colors")
    variants = colors.findall("Color") if colors is not None else []
    if variants:
        return [(node.attrib.get("id", ""), node) for node in variants]
    # No invented compatibility color: take the actual maintained V3 row(s).
    return [(base.text(node, "Color_Code"), None) for node in data["v3_rows_by_pid"].get(pid, [])]


def v4_direct_override(product: ET.Element, header: str) -> str:
    """Use V4 only when the supplier exposes the same field name directly."""
    if not header.startswith("products__product__"):
        return ""
    suffix = header[len("products__product__"):]
    if "__" in suffix:
        return ""
    wanted = norm(suffix)
    for child in list(product):
        if not list(child) and norm(child.tag) == wanted:
            return (child.text or "").strip()
    return ""


def print_aggregate(rows: list[dict[str, str]]) -> dict[str, str]:
    def vals(name):
        return [r.get(name, "") for r in rows]

    dimensions = []
    method_qty = []
    for row in rows:
        width, height, diameter = row.get("Width", ""), row.get("Height", ""), row.get("Diameter", "")
        if width not in {"", "0"} and height not in {"", "0"}:
            dimensions.append(f"{width}X{height}")
        elif diameter not in {"", "0"}:
            dimensions.append(f"Ø{diameter}")
        if row.get("Print_Method") and row.get("Minimum_Order_Quantity"):
            method_qty.append(f"{row['Print_Method']}-min{row['Minimum_Order_Quantity']}")

    return {
        "print_group": base.smart_join(vals("Print_Group")),
        "print_method": base.smart_join(vals("Print_Method")),
        "handling_code": base.smart_join(vals("Handling_Code")),
        "minimum_order_quantity": base.smart_join(vals("Minimum_Order_Quantity")),
        "sub_article": base.smart_join(vals("Sub_Article")),
        "position": base.smart_join(vals("Position")),
        "print_method_default": base.smart_join(vals("Print_Method_Default")),
        "position_default": base.smart_join(vals("Position_Default")),
        "number_of_colors": base.smart_join(vals("Number_Of_Colors")),
        "height": base.smart_join(vals("Height")),
        "width": base.smart_join(vals("Width")),
        "diameter": base.smart_join(vals("Diameter")),
        "passes": base.smart_join(vals("Passes")),
        "image_name": base.unique_join(vals("Image_Name")),
        "production_time": base.smart_join(vals("Production_Time")),
        "single_name": base.smart_join(vals("Single_Name")),
        "dimensions": base.smart_join(dimensions),
        "method_qty": base.smart_join(method_qty),
    }


def v3_method_time(rows: list[dict[str, str]]) -> str:
    values = []
    for row in rows:
        method, days = row.get("Print_Method", ""), row.get("Production_Time", "")
        if method and days:
            values.append(f"{method}-time{days}")
    return base.smart_join(values)


def first_price(data, pid: str, color: str) -> str:
    tiers = data["prices"].get((pid, color), {}).get("tiers", [])
    if not tiers:
        return ""
    try:
        first = min(tiers, key=lambda x: int(x.get("Start") or 10**9))
    except ValueError:
        first = tiers[0]
    return first.get("Price", "")


def keep_it_and_neutral(flat: dict[str, str]):
    out = {}
    for key, value in flat.items():
        leaf = key.split("__")[-1]
        if leaf.startswith("IT_") or not leaf.startswith(LANG_PREFIXES):
            out[key] = value
    return out


def build_legacy_row(data, product: ET.Element, color_code: str, color_node: ET.Element | None):
    pid = base.text(product, "Product_Id")
    v3 = data["products_v3"].get((pid, color_code))
    row = {header: "" for header in LEGACY_HEADERS}

    # Exact maintained V3 fields are the safe compatibility baseline.
    for header in LEGACY_HEADERS:
        row[header] = direct_exact(v3, header)

    row["created"] = data["created"]
    row["products__product__product_id"] = pid
    row["products__product__color_code"] = color_code

    # V4 is primary only for exact same-named direct fields.
    for header in LEGACY_HEADERS:
        value = v4_direct_override(product, header)
        if value:
            row[header] = value

    # V4 structures that are clearly the same legacy supplier fields.
    log = product.find("Logistic_Information")
    sust = product.find("Sustainability_Compliance")
    pinfo = product.find("Print_Information")
    props = product.find("Product_Properties")

    exact_nested = {
        "products__product__dangerous_goods": base.text(log, "Dangerous_Goods"),
        "products__product__breaking_risk": base.text(log, "Breaking_Risk"),
        "products__product__hs_code": base.text(sust, "Hs_Code"),
        "products__product__country_origin": base.text(sust, "Country_Origin"),
        "products__product__minimum_start": base.text(pinfo, "Minimum_Start"),
        "products__product__print_group": base.text(pinfo, "Print_Group"),
        "products__product__print_method": base.text(pinfo, "Print_Method"),
        "products__product__print_size": base.text(pinfo, "Print_Size"),
        "products__product__print_maximum_colors": base.text(pinfo, "Print_Maximum_Colors"),
        "products__product__print_positions": base.text(pinfo, "Print_Positions"),
        "products__product__handling_code": base.text(pinfo, "Handling_Code"),
        "products__product__mechanical": base.text(props, "Mechanical"),
        "products__product__eraser": base.text(props, "Eraser"),
        "products__product__double_wall": base.text(props, "Double_Wall"),
        "products__product__bluetooth": base.text(props, "Bluetooth"),
        "products__product__child_resistant": base.text(props, "Child_Resistant"),
        "products__product__controllable_flame": base.text(props, "Controllable_Flame"),
    }
    for header, value in exact_nested.items():
        if value:
            row[header] = value

    # V4 dimensions preserve the old compatibility unit where unambiguous.
    dimension_map = {
        "products__product__length": ("ProductLength", "Length"),
        "products__product__width": ("ProductWidth", "Width"),
        "products__product__height": ("ProductHeight", "Height"),
        "products__product__diameter": ("ProductDiameter", "Diameter"),
        "products__product__volume": ("ProductVolume", "Volume"),
    }
    for header, (node_name, attr_name) in dimension_map.items():
        value = base.attr(product, node_name, attr_name)
        if value:
            row[header] = value

    weight = base.attr(product, "ProductWeight", "Weight")
    weight_node = product.find("ProductWeight")
    if weight:
        if weight_node is not None and weight_node.attrib.get("unit", "").lower() == "kg":
            try:
                weight = f"{float(weight) * 1000:g}"
            except ValueError:
                pass
        row["products__product__weight"] = weight

    # Optional print values: direct supplier list, no generated Yes/No.
    if pinfo is not None:
        optional = pinfo.find("Optional_Prints")
        if optional is not None:
            row["products__product__print_group_optional"] = base.smart_join(
                base.text(n, "Optional_Print_Group") for n in optional.findall("Optional_print")
            )
            row["products__product__print_method_optional"] = base.smart_join(
                base.text(n, "Optional_Print_Method") for n in optional.findall("Optional_print")
            )

    # Official Italian V4 translations. Exact V3 values remain fallback when absent.
    it_map = {
        "products__product__it_name": "title",
        "products__product__it_description": "description",
        "products__product__it_keywords": "search_term",
        "products__product__material": "Material",
        "products__product__mechanism": "Mechanism",
        "products__product__standard_writing_colors": "Default_Writing_Colors",
        "products__product__optional_writing_colors": "Optional_Writing_Colors",
        "products__product__refill": "Refill",
        "products__product__tip": "Tip",
        "products__product__paper_color": "Paper_Color",
        "products__product__usb_port": "Usb_Port",
        "products__product__dishwasher_resistant": "Dishwasher_Resistant",
        "products__product__microwave_safe": "Microwave_Safe",
    }
    for header, prop in it_map.items():
        value = base.translation_it(data["translations"], pid, prop)
        if value:
            row[header] = value

    # Italian colour/category supplier tables.
    color_info = data["colors"].get(color_code, {})
    color_v3 = data["colors_v3"].get((pid, color_code), {})
    row["products__product__it_color_description"] = color_info.get("IT_Title", "") or color_v3.get("IT_Title", "")
    row["IT_Color_Base_Title"] = color_info.get("IT_Color_Base_Title", "") or color_v3.get("IT_Color_Base_Title", "")
    row["IT_Color_Base2_Title"] = color_info.get("IT_Color_Base2_Title", "") or color_v3.get("IT_Color_Base2_Title", "")

    category_ids = [base.text(n, ".") for n in product.findall("./Categories/Category_Id")]
    titles = [data["categories"].get(cid, {}).get("IT_Description", "") for cid in category_ids]
    titles = [x for x in titles if x]
    if titles:
        row["categories"] = base.unique_join(titles)

    # Variant/product values that live in dedicated V4 nodes.
    if color_node is not None:
        image = base.text(color_node, "Image_Color")
        if image:
            row["products__product__image_color"] = image
    comp = [base.text(n, ".") for n in product.findall("./Complementary_Products/Product_Id")]
    if comp:
        row["products__product__complementary_products__product_id"] = base.unique_join(comp)
    colors_node = product.find("Colors")
    if colors_node is not None:
        color_ids = [n.attrib.get("id", "") for n in colors_node.findall("Color") if n.attrib.get("id", "")]
        if color_ids:
            row["products__product__colors"] = base.smart_join(color_ids, ", ")

    price = first_price(data, pid, color_code)
    if price:
        row["products__product__prices__price__price"] = price

    # Print V4 is primary; fields removed from V4 come from maintained Print V3.
    p4 = print_aggregate(data["prints_by_product"].get(pid, []))
    p3rows = data["print_v3_by_product"].get(pid, [])
    p3 = print_aggregate(p3rows)
    position_headers = {
        "products__product__positions__position__print_group": p4["print_group"],
        "products__product__positions__position__print_method": p4["print_method"],
        "products__product__positions__position__handling_code": p4["handling_code"],
        "products__product__positions__position__minimum_order_quantity": p4["minimum_order_quantity"],
        "products__product__positions__position__sub_article": p3["sub_article"],
        "products__product__positions__position__position": p4["position"],
        "products__product__positions__position__print_method_default": p4["print_method_default"],
        "products__product__positions__position__position_default": p4["position_default"],
        "products__product__positions__position__number_of_colors": p4["number_of_colors"],
        "products__product__positions__position__height": p4["height"],
        "products__product__positions__position__width": p4["width"],
        "products__product__positions__position__diameter": p4["diameter"],
        "products__product__positions__position__passes": p3["passes"],
        "products__product__positions__position__image_name": p4["image_name"],
        "products__product__positions__position__production_time": p3["production_time"],
        "products__product__positions__position__single_name": p4["single_name"],
    }
    for header, value in position_headers.items():
        if value:
            row[header] = value

    # Helper columns are deterministic formatting of supplier values only.
    row["dimensions"] = p4["dimensions"]
    row["print_method_quantity"] = p4["method_qty"]
    row["print_method_time"] = v3_method_time(p3rows)
    if row.get("products__product__minimum"):
        row["product_minimum"] = f"{row['products__product__minimum']}minimo"
    images = data["images"].get(f"{pid}-{color_code}", [])
    row["additional_images"] = base.unique_join(images)
    return row


def semantic_legacy_for_candidate(candidate: str) -> str | None:
    # Most direct/nested V4 leaves map mechanically to a same-named legacy field.
    leaf = candidate.split("__")[-1]
    if leaf.startswith("@"):
        leaf = leaf[1:]
    mapped = LEGACY_DIRECT.get(norm(leaf))
    if mapped:
        return mapped

    explicit = {
        "v4__Color__@id": "products__product__color_code",
        "v4__Color__Image_Color": "products__product__image_color",
        "v4_it__title": "products__product__it_name",
        "v4_it__description": "products__product__it_description",
        "v4_it__search_term": "products__product__it_keywords",
        "v4_color__IT_Title": "products__product__it_color_description",
        "v4_color__IT_Color_Base_Title": "IT_Color_Base_Title",
        "v4_color__IT_Color_Base2_Title": "IT_Color_Base2_Title",
        "v4_price__tier_1__Price": "products__product__prices__price__price",
    }
    return explicit.get(candidate)


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
    return bool_headers


def build_products(data, destination: Path):
    rows, candidates, candidate_seen = [], [], set()
    product_flat, it_flat = {}, {}

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
            row = build_legacy_row(data, product, color_code, color_node)

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
            rows.append(row)

    boolean_headers = booleanize(rows, LEGACY_HEADERS + candidates)

    # First remove semantic aliases intentionally represented by the legacy
    # compatibility column, which already applies V4->V3 fallback.
    filtered = []
    semantic_dropped = []
    for candidate in candidates:
        legacy = semantic_legacy_for_candidate(candidate)
        if legacy:
            semantic_dropped.append((candidate, legacy))
        else:
            filtered.append(candidate)

    # Then remove remaining exact duplicate value vectors without guessing semantics.
    seen = {tuple(str(r.get(h, "")).strip() for r in rows): h for h in LEGACY_HEADERS}
    kept, exact_dropped = [], []
    for candidate in filtered:
        sig = tuple(str(r.get(candidate, "")).strip() for r in rows)
        if sig in seen:
            exact_dropped.append((candidate, seen[sig]))
        else:
            seen[sig] = candidate
            kept.append(candidate)

    headers = LEGACY_HEADERS + kept
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return {
        "rows": len(rows),
        "columns": len(headers),
        "path": str(destination),
        "boolean_columns": sorted(boolean_headers),
        "semantic_duplicate_columns": semantic_dropped,
        "exact_duplicate_columns": exact_dropped,
    }


def build_dpo(data, destination: Path):
    rows, seen = [], set()
    for pos in data["print_rows"]:
        pid = pos.get("Product_Code", "")
        height, width, diameter = pos.get("Height", ""), pos.get("Width", ""), pos.get("Diameter", "")
        if diameter not in {"", "0"} and height in {"", "0"} and width in {"", "0"}:
            dimensions = f"diam {diameter} mm"
        else:
            dimensions = f"{height}x{width} mm"
        group = pos.get("Print_Group", "")  # exact supplier value; no correction/rewrite
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
        rows.append({
            DPO_HEADERS[0]: pid,
            DPO_HEADERS[1]: dimensions,
            DPO_HEADERS[2]: group,
            DPO_HEADERS[3]: translated,
            DPO_HEADERS[4]: image,
            DPO_HEADERS[5]: "",
        })

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=DPO_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    return {"rows": len(rows), "columns": len(DPO_HEADERS), "path": str(destination)}


def build_from_source_root(source_root: Path, output_dir: Path):
    data = load_indexes(source_root)
    products = build_products(data, output_dir / "Products.csv")
    dpo = build_dpo(data, output_dir / "DPO PRINT.csv")
    print(f"Products: {products['rows']} righe, {products['columns']} colonne")
    print(f"DPO PRINT: {dpo['rows']} righe")
    print(f"Alias semantici rimossi: {len(products['semantic_duplicate_columns'])}")
    print(f"Doppioni esatti rimossi: {len(products['exact_duplicate_columns'])}")
    return {"products": products, "dpo_print": dpo}
