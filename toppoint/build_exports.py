from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

LEGACY_HEADERS = """created
products__product__product_id
products__product__color_code
products__product__size
products__product__length
products__product__width
products__product__height
products__product__diameter
products__product__volume
products__product__weight
products__product__minimum
products__product__minimum_start
products__product__increment_of
products__product__only_blank
products__product__only_print
products__product__small_order_surcharge
products__product__dangerous_goods
products__product__dangerous_goods_surcharge
products__product__breaking_risk
products__product__prices__price__price
products__product__price_on_demand
products__product__print_possible
products__product__print_group
products__product__print_method
products__product__print_group_optional
products__product__print_method_optional
products__product__print_size
products__product__print_maximum_colors
products__product__print_positions
products__product__handling_code
products__product__express_delivery
products__product__express_delivery_days
products__product__image_together
products__product__image_color
categories
products__product__complementary_products__product_id
products__product__complementary_products
products__product__colors
products__product__brand
products__product__extra_info_size
products__product__material
products__product__mechanism
products__product__mechanical
products__product__thickness
products__product__standard_writing_colors
products__product__optional_writing_colors
products__product__batteries
products__product__led
products__product__refill
products__product__point
products__product__eraser
products__product__tip
products__product__quality
products__product__sheets
products__product__lined
products__product__gift_box
products__product__sugar
products__product__paper_color
products__product__orientation
products__product__refillable
products__product__child_resistant
products__product__controllable_flame
products__product__custom
products__product__outer_carton_printed_weight
products__product__outer_carton_printed_length
products__product__outer_carton_printed_width
products__product__outer_carton_printed_height
products__product__outer_carton_unprinted_quantity
products__product__metal_parts
products__product__outer_carton_unprinted_length
products__product__outer_carton_unprinted_width
products__product__outer_carton_unprinted_height
products__product__toppoint_design
products__product__double_wall
products__product__dishwasher_resistant
products__product__microwave_safe
products__product__suitable_hot_drinks
products__product__leak_free
products__product__suitable_devices
products__product__cables_included
products__product__usb_port
products__product__bluetooth
products__product__input_volt
products__product__output_volt
products__product__capacity_mah
products__product__charging_time
products__product__social_compliance
products__product__letterbox
products__product__outer_carton_unprinted_weight
products__product__hs_code
products__product__country_origin
products__product__brands
products__product__it_name
products__product__it_description
products__product__it_keywords
products__product__it_color_description
IT_Color_Base_Title
IT_Color_Base2_Title
products__product__positions__position__print_group
products__product__positions__position__print_method
products__product__positions__position__handling_code
products__product__positions__position__minimum_order_quantity
products__product__positions__position__sub_article
products__product__positions__position__position
products__product__positions__position__print_method_default
products__product__positions__position__position_default
products__product__positions__position__number_of_colors
products__product__positions__position__height
products__product__positions__position__width
products__product__positions__position__diameter
products__product__positions__position__passes
products__product__positions__position__image_name
products__product__positions__position__production_time
products__product__positions__position__single_name
dimensions
print_method_quantity
print_method_time
product_minimum
additional_images""".splitlines()

DPO_HEADERS = [
    "Products__Product__Product_Code",
    "Products__Product__Positions__Position__Passes",
    "Products__Product__Positions__Position__Print_Group",
    "Posizione (Italiano)",
    "Products__Product__Positions__Position__Image_Name",
    "note",
]


def text(elem: ET.Element | None, tag: str) -> str:
    if elem is None:
        return ""
    node = elem.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""


def unique_join(values, sep=",") -> str:
    out, seen = [], set()
    for value in values:
        value = "" if value is None else str(value).strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return sep.join(out)


def smart_join(values, sep=",") -> str:
    vals = list(dict.fromkeys(str(v).strip() for v in values if str(v).strip()))

    def key(v: str):
        try:
            return 0, float(v)
        except ValueError:
            if v.lower() in {"full colour", "full color", "fc"}:
                return 2, v.lower()
            return 1, v.lower()

    return sep.join(sorted(vals, key=key))


def yes_no(value: str) -> str:
    value = (value or "").strip().lower()
    if value in {"1", "true", "yes", "y"}:
        return "Yes"
    if value in {"0", "false", "no", "n"}:
        return "No"
    return value


def flatten(elem: ET.Element, root_strip: str | None = None, skip_direct=None) -> dict[str, str]:
    skip_direct = set(skip_direct or [])
    acc: dict[str, list[str]] = defaultdict(list)

    def walk(node: ET.Element, path: str = ""):
        if path and path.count("__") == 0 and node.tag in skip_direct:
            return
        current = f"{path}__{node.tag}" if path else node.tag
        for name, value in node.attrib.items():
            if value:
                acc[f"{current}__@{name}"].append(value)
        children = list(node)
        if not children:
            value = (node.text or "").strip()
            if value:
                acc[current].append(value)
        else:
            for child in children:
                if current == elem.tag and child.tag in skip_direct:
                    continue
                walk(child, current)

    walk(elem)
    result = {k: unique_join(v, ", ") for k, v in acc.items()}
    if root_strip:
        prefix = root_strip + "__"
        result = {(k[len(prefix):] if k.startswith(prefix) else k): v for k, v in result.items()}
    return result


def flatten_it_translation(elem: ET.Element) -> dict[str, str]:
    acc: dict[str, list[str]] = defaultdict(list)

    def walk(node: ET.Element, path: str = ""):
        current = f"{path}__{node.tag}" if path else node.tag
        children = list(node)
        if not children:
            value = (node.text or "").strip()
            if value and node.tag.endswith("-it_IT"):
                acc[current.replace("-it_IT", "")].append(value)
        else:
            for child in children:
                walk(child, current)

    walk(elem)
    return {k: unique_join(v, ", ") for k, v in acc.items()}


def square_1600(url: str) -> str:
    if not url:
        return ""
    base = url.split("?", 1)[0]
    return (
        base
        + "?w=1600&h=1600&canvas.width=1600&canvas.height=1600"
        + "&canvas.color=ffffff&canvas.position=center&format=original"
    )


def normalize(value: str) -> str:
    return re.sub(r"[\s_-]+", " ", (value or "").strip().lower())


def load_indexes(source_root: Path):
    feed = source_root / "feed-v4"
    products_root = ET.parse(feed / "Products_v4.xml").getroot()
    created = text(products_root, "Created")
    products = products_root.find("Products").findall("Product")

    color_root = ET.parse(feed / "Colors_v4.xml").getroot()
    colors = {}
    color_flat = {}
    for node in color_root.find("Colors").findall("Color"):
        code = text(node, "Color_Code")
        colors[code] = {child.tag: (child.text or "").strip() for child in list(node)}
        color_flat[code] = flatten(node, "Color")

    category_root = ET.parse(feed / "Categories_v4.xml").getroot()
    categories = {}
    category_flat = {}
    for node in category_root.find("Categories").findall("Category"):
        cid = text(node, "Category_Id")
        categories[cid] = {child.tag: (child.text or "").strip() for child in list(node)}
        category_flat[cid] = flatten(node, "Category")

    trans_root = ET.parse(feed / "ProductTranslations_v4.xml").getroot()
    translations = {text(node, "code"): node for node in trans_root.findall("product")}

    price_root = ET.parse(feed / "ProductPrices_v4.xml").getroot()
    prices = {}
    max_tiers = 0
    for node in price_root.find("Products").findall("Product"):
        pid, color = text(node, "Product_Id"), text(node, "Color_Code")
        price_node = node.find("Prices")
        tiers = []
        if price_node is not None:
            for tier in price_node.findall("Price"):
                tiers.append({child.tag: (child.text or "").strip() for child in list(tier)})
        max_tiers = max(max_tiers, len(tiers))
        prices[(pid, color)] = {
            "excluded": text(price_node, "Excluded_From_Discount"),
            "tiers": tiers,
        }

    print_root = ET.parse(feed / "Print_v4.xml").getroot()
    print_rows = []
    prints_by_product: dict[str, list[dict[str, str]]] = defaultdict(list)
    for product in print_root.find("Products").findall("Product"):
        pid = text(product, "Product_Code")
        positions = product.find("Positions")
        if positions is None:
            continue
        for pos in positions.findall("Position"):
            row = {child.tag: (child.text or "").strip() for child in list(pos)}
            row["Product_Code"] = pid
            print_rows.append(row)
            prints_by_product[pid].append(row)

    position_root = ET.parse(feed / "PositionTranslations_v4.xml").getroot()
    position_it = {}
    for node in position_root.findall("Position"):
        key = text(node, "Position")
        position_it[key] = text(node, "IT_Position_Title") or text(node, "EN_Position_Title") or key

    production_root = ET.parse(source_root / "ProductionTimes.xml").getroot()
    production_index: dict[tuple[str, str, str, str], list[dict]] = defaultdict(list)
    for product in production_root.findall("Product"):
        pid = text(product, "Product_Code")
        positions = product.find("Positions")
        if positions is None:
            continue
        for pos in positions.findall("Position"):
            record = {child.tag: (child.text or "").strip() for child in list(pos) if child.tag != "ProductionTimes"}
            tiers = []
            tiers_node = pos.find("ProductionTimes")
            if tiers_node is not None:
                for tier in tiers_node.findall("ProductionTime"):
                    tiers.append({child.tag: (child.text or "").strip() for child in list(tier)})
            record["tiers"] = tiers
            production_index[(pid, record.get("Print_Group", ""), normalize(record.get("Print_Method", "")), normalize(record.get("Position_Name", "")))].append(record)

    image_root = ET.parse(source_root / "product_images.xml").getroot()
    images = {}
    for product in image_root.findall("product"):
        variants = product.find("variants")
        if variants is None:
            continue
        for variant in variants.findall("variant"):
            code = variant.attrib.get("code", "")
            urls = []
            image_nodes = variant.find("images")
            if image_nodes is not None:
                for image in image_nodes.findall("image"):
                    url = text(image, "large") or text(image, "medium") or text(image, "small")
                    if url:
                        urls.append(square_1600(url))
            images[code] = list(dict.fromkeys(urls))

    return {
        "created": created,
        "products": products,
        "colors": colors,
        "color_flat": color_flat,
        "categories": categories,
        "category_flat": category_flat,
        "translations": translations,
        "prices": prices,
        "max_tiers": max_tiers,
        "print_rows": print_rows,
        "prints_by_product": prints_by_product,
        "position_it": position_it,
        "production_index": production_index,
        "images": images,
    }


def production_days(index, pid: str, pos: dict[str, str]) -> str:
    key = (pid, pos.get("Print_Group", ""), normalize(pos.get("Print_Method", "")), normalize(pos.get("Position", "")))
    candidates = index.get(key, [])
    if not candidates:
        return ""
    colors = pos.get("Number_Of_Colors", "")

    def same_color(record):
        value = record.get("Number_Of_Colors", "")
        return value == colors or (value == "FC" and colors.lower().startswith("full"))

    record = next((item for item in candidates if same_color(item)), candidates[0])
    for tier in record["tiers"]:
        if tier.get("Till") == "2500":
            return tier.get("Days", "")
    for tier in record["tiers"]:
        try:
            if int(tier["From"]) <= 2500 <= int(tier["Till"]):
                return tier.get("Days", "")
        except (KeyError, ValueError):
            pass
    return record["tiers"][-1].get("Days", "") if record["tiers"] else ""


def attr(product: ET.Element, path: str, name: str) -> str:
    node = product.find(path)
    return node.attrib.get(name, "") if node is not None else ""


def logistic_attr(product: ET.Element, node_name: str, attr_name: str) -> str:
    logistic = product.find("Logistic_Information")
    node = logistic.find(node_name) if logistic is not None else None
    return node.attrib.get(attr_name, "") if node is not None else ""


def logistic_text(product: ET.Element, name: str) -> str:
    return text(product.find("Logistic_Information"), name)


def sustainability(product: ET.Element, name: str) -> str:
    return text(product.find("Sustainability_Compliance"), name)


def property_text(product: ET.Element, name: str) -> str:
    return text(product.find("Product_Properties"), name)


def property_attr(product: ET.Element, name: str, attr_name="Value") -> str:
    props = product.find("Product_Properties")
    node = props.find(name) if props is not None else None
    return node.attrib.get(attr_name, "") if node is not None else ""


def translation_it(index, pid: str, prop: str) -> str:
    node = index.get(pid)
    if node is None:
        return ""
    if prop in {"title", "description", "search_term"}:
        return text(node, f"{prop}-it_IT")
    return text(node.find("Product_Properties"), f"{prop}-it_IT")


def position_aggregate(data, pid: str) -> dict[str, str]:
    positions = data["prints_by_product"].get(pid, [])
    dimensions, times, method_qty, method_time = [], [], [], []
    for pos in positions:
        width, height, diameter = pos.get("Width", ""), pos.get("Height", ""), pos.get("Diameter", "")
        if width not in {"", "0"} and height not in {"", "0"}:
            dimensions.append(f"{width}X{height}")
        elif diameter not in {"", "0"}:
            dimensions.append(f"Ø{diameter}")
        days = production_days(data["production_index"], pid, pos)
        if days:
            times.append(days)
        if pos.get("Print_Method") and pos.get("Minimum_Order_Quantity"):
            method_qty.append(f"{pos['Print_Method']}-min{pos['Minimum_Order_Quantity']}")
        if pos.get("Print_Method") and days:
            method_time.append(f"{pos['Print_Method']}-time{days}")

    def values(name):
        return [pos.get(name, "") for pos in positions]

    return {
        "groups": smart_join(values("Print_Group")),
        "methods": smart_join(values("Print_Method")),
        "handling": smart_join(values("Handling_Code")),
        "minimums": smart_join(values("Minimum_Order_Quantity")),
        "positions": smart_join(values("Position")),
        "method_default": smart_join(values("Print_Method_Default")),
        "position_default": smart_join(values("Position_Default")),
        "colors": smart_join(values("Number_Of_Colors")),
        "heights": smart_join(values("Height")),
        "widths": smart_join(values("Width")),
        "diameters": smart_join(values("Diameter")),
        "images": unique_join(values("Image_Name")),
        "times": smart_join(times),
        "single_name": smart_join(values("Single_Name")),
        "dimensions": smart_join(dimensions),
        "method_qty": smart_join(method_qty),
        "method_time": smart_join(method_time),
    }


def optional_prints(product: ET.Element, name: str) -> str:
    print_info = product.find("Print_Information")
    optional = print_info.find("Optional_Prints") if print_info is not None else None
    if optional is None:
        return ""
    return smart_join(text(node, name) for node in optional.findall("Optional_print"))


def build_legacy_row(data, product: ET.Element, color_node: ET.Element | None) -> dict[str, str]:
    pid = text(product, "Product_Id")
    color = color_node.attrib.get("id", "") if color_node is not None else ""
    compatibility_color = color or "N0999"
    print_info = product.find("Print_Information")
    color_info = data["colors"].get(compatibility_color, {})
    aggregate = position_aggregate(data, pid)
    price_record = data["prices"].get((pid, compatibility_color), {})
    tiers = price_record.get("tiers", [])
    first_price = min(tiers, key=lambda x: int(x.get("Start") or 999999)).get("Price", "") if tiers else ""
    variants = product.find("Colors")
    color_codes = [node.attrib.get("id", "") for node in variants.findall("Color")] if variants is not None else []
    category_ids = [text(node, ".") for node in product.findall("./Categories/Category_Id")]
    category_titles = [data["categories"].get(cid, {}).get("IT_Description", "") for cid in category_ids]
    complementary = [text(node, ".") for node in product.findall("./Complementary_Products/Product_Id")]

    weight = attr(product, "ProductWeight", "Weight")
    weight_node = product.find("ProductWeight")
    if weight and weight_node is not None and weight_node.attrib.get("unit", "").lower() == "kg":
        try:
            weight = f"{float(weight) * 1000:g}"
        except ValueError:
            pass

    image_color = text(color_node, "Image_Color") if color_node is not None else ""
    additional_images = data["images"].get(f"{pid}-{compatibility_color}", [])
    minimum = text(product, "Minimum")

    row = {header: "" for header in LEGACY_HEADERS}
    row.update({
        "created": data["created"],
        "products__product__product_id": pid,
        "products__product__color_code": compatibility_color,
        "products__product__length": attr(product, "ProductLength", "Length"),
        "products__product__width": attr(product, "ProductWidth", "Width"),
        "products__product__height": attr(product, "ProductHeight", "Height"),
        "products__product__diameter": attr(product, "ProductDiameter", "Diameter"),
        "products__product__volume": attr(product, "ProductVolume", "Volume"),
        "products__product__weight": weight,
        "products__product__minimum": minimum,
        "products__product__minimum_start": text(print_info, "Minimum_Start"),
        "products__product__only_blank": text(product, "Only_Blank"),
        "products__product__only_print": text(product, "Only_Print"),
        "products__product__dangerous_goods": yes_no(logistic_text(product, "Dangerous_Goods")),
        "products__product__dangerous_goods_surcharge": "0",
        "products__product__breaking_risk": yes_no(logistic_text(product, "Breaking_Risk")),
        "products__product__prices__price__price": first_price,
        "products__product__print_possible": "Yes" if text(print_info, "Print_Group") else "No",
        "products__product__print_group": text(print_info, "Print_Group"),
        "products__product__print_method": text(print_info, "Print_Method"),
        "products__product__print_group_optional": optional_prints(product, "Optional_Print_Group"),
        "products__product__print_method_optional": optional_prints(product, "Optional_Print_Method"),
        "products__product__print_size": text(print_info, "Print_Size"),
        "products__product__print_maximum_colors": text(print_info, "Print_Maximum_Colors"),
        "products__product__print_positions": text(print_info, "Print_Positions"),
        "products__product__handling_code": text(print_info, "Handling_Code"),
        "products__product__express_delivery": "No" if text(print_info, "Print_Group") else "",
        "products__product__image_together": text(product, "Image_Together"),
        "products__product__image_color": image_color,
        "categories": unique_join(category_titles),
        "products__product__complementary_products__product_id": unique_join(complementary),
        "products__product__colors": smart_join([code or "N0999" for code in color_codes] or ["N0999"], ", "),
        "products__product__brand": text(product, "Brand"),
        "products__product__material": translation_it(data["translations"], pid, "Material"),
        "products__product__mechanism": translation_it(data["translations"], pid, "Mechanism"),
        "products__product__mechanical": property_text(product, "Mechanical"),
        "products__product__standard_writing_colors": translation_it(data["translations"], pid, "Default_Writing_Colors"),
        "products__product__optional_writing_colors": translation_it(data["translations"], pid, "Optional_Writing_Colors"),
        "products__product__batteries": translation_it(data["translations"], pid, "Battery") or translation_it(data["translations"], pid, "Battery_Type"),
        "products__product__refill": translation_it(data["translations"], pid, "Refill") or translation_it(data["translations"], pid, "Type_Of_Refill"),
        "products__product__eraser": property_text(product, "Eraser"),
        "products__product__tip": translation_it(data["translations"], pid, "Tip") or translation_it(data["translations"], pid, "Tip_Type"),
        "products__product__sheets": property_text(product, "Number_Of_Sheets"),
        "products__product__gift_box": text(product, "Giftbox"),
        "products__product__paper_color": translation_it(data["translations"], pid, "Paper_Color"),
        "products__product__refillable": property_text(product, "Replaceable_Refill"),
        "products__product__child_resistant": property_text(product, "Child_Resistant"),
        "products__product__controllable_flame": property_text(product, "Controllable_Flame"),
        "products__product__custom": text(product, "Custom"),
        "products__product__outer_carton_printed_weight": logistic_attr(product, "outbound_printed_carton_weight", "Outbound_Printed_Carton_Weight") or logistic_text(product, "Outbound_Printed_Carton_Weight"),
        "products__product__outer_carton_printed_length": logistic_attr(product, "outbound_printed_carton_length", "Outbound_Printed_Carton_Length"),
        "products__product__outer_carton_printed_width": logistic_attr(product, "outbound_printed_carton_width", "Outbound_Printed_Carton_Width"),
        "products__product__outer_carton_printed_height": logistic_attr(product, "outbound_printed_carton_height", "Outbound_Printed_Carton_Height"),
        "products__product__outer_carton_unprinted_quantity": logistic_text(product, "Outbound_Unprinted_Pcs_Per_Carton"),
        "products__product__metal_parts": text(product, "Metal_Parts"),
        "products__product__outer_carton_unprinted_length": logistic_attr(product, "outbound_unprinted_carton_length", "Outbound_Unprinted_Carton_Length"),
        "products__product__outer_carton_unprinted_width": logistic_attr(product, "outbound_unprinted_carton_width", "Outbound_Unprinted_Carton_Width"),
        "products__product__outer_carton_unprinted_height": logistic_attr(product, "outbound_unprinted_carton_height", "Outbound_Unprinted_Carton_Height"),
        "products__product__toppoint_design": text(product, "Toppoint_Design"),
        "products__product__double_wall": property_text(product, "Double_Wall"),
        "products__product__dishwasher_resistant": translation_it(data["translations"], pid, "Dishwasher_Resistant") or yes_no(property_text(product, "Dishwasher_Safe")),
        "products__product__microwave_safe": translation_it(data["translations"], pid, "Microwave_Safe") or translation_it(data["translations"], pid, "Microwave_Safe_New"),
        "products__product__suitable_hot_drinks": property_text(product, "Hot_Drinks_Suitable"),
        "products__product__leak_free": property_text(product, "Leakfree"),
        "products__product__cables_included": property_text(product, "Cable_Included"),
        "products__product__usb_port": translation_it(data["translations"], pid, "Usb_Port"),
        "products__product__bluetooth": property_text(product, "Bluetooth"),
        "products__product__input_volt": property_attr(product, "Input_Voltage"),
        "products__product__capacity_mah": property_attr(product, "Battery_Capacity"),
        "products__product__charging_time": property_attr(product, "Charging_Time"),
        "products__product__social_compliance": yes_no(sustainability(product, "From_Social_Audited_Factory")),
        "products__product__letterbox": yes_no(logistic_text(product, "Mailbox")),
        "products__product__outer_carton_unprinted_weight": logistic_attr(product, "outbound_unprinted_carton_weight", "Outbound_Unprinted_Carton_Weight") or logistic_text(product, "Outbound_Unprinted_Carton_Weight"),
        "products__product__hs_code": sustainability(product, "Hs_Code"),
        "products__product__country_origin": sustainability(product, "Country_Origin"),
        "products__product__brands": text(product, "Brand"),
        "products__product__it_name": translation_it(data["translations"], pid, "title"),
        "products__product__it_description": translation_it(data["translations"], pid, "description"),
        "products__product__it_keywords": translation_it(data["translations"], pid, "search_term"),
        "products__product__it_color_description": color_info.get("IT_Title", ""),
        "IT_Color_Base_Title": color_info.get("IT_Color_Base_Title", ""),
        "IT_Color_Base2_Title": color_info.get("IT_Color_Base2_Title", ""),
        "products__product__positions__position__print_group": aggregate["groups"],
        "products__product__positions__position__print_method": aggregate["methods"],
        "products__product__positions__position__handling_code": aggregate["handling"],
        "products__product__positions__position__minimum_order_quantity": aggregate["minimums"],
        "products__product__positions__position__sub_article": "<null>" if aggregate["groups"] else "",
        "products__product__positions__position__position": aggregate["positions"],
        "products__product__positions__position__print_method_default": aggregate["method_default"],
        "products__product__positions__position__position_default": aggregate["position_default"],
        "products__product__positions__position__number_of_colors": aggregate["colors"],
        "products__product__positions__position__height": aggregate["heights"],
        "products__product__positions__position__width": aggregate["widths"],
        "products__product__positions__position__diameter": aggregate["diameters"],
        "products__product__positions__position__passes": "1" if aggregate["groups"] else "",
        "products__product__positions__position__image_name": aggregate["images"],
        "products__product__positions__position__production_time": aggregate["times"],
        "products__product__positions__position__single_name": aggregate["single_name"],
        "dimensions": aggregate["dimensions"],
        "print_method_quantity": aggregate["method_qty"],
        "print_method_time": aggregate["method_time"],
        "product_minimum": f"{minimum}minimo" if minimum else "",
        "additional_images": unique_join(additional_images),
    })
    return row


def build_products(data, destination: Path) -> dict:
    product_flat_cache, translation_flat_cache = {}, {}
    v4_columns, it_columns = set(), set()
    color_columns = set()
    category_columns = set()

    for code, flat in data["color_flat"].items():
        color_columns.update(f"v4_color__{key}" for key in flat)
    for cid, flat in data["category_flat"].items():
        category_columns.update(f"v4_category__{key}" for key in flat)

    for product in data["products"]:
        pid = text(product, "Product_Id")
        flat = flatten(product, "Product", skip_direct={"Colors"})
        product_flat_cache[pid] = flat
        v4_columns.update(f"v4__{key}" for key in flat)
        translation = data["translations"].get(pid)
        if translation is not None:
            it_flat = flatten_it_translation(translation)
            translation_flat_cache[pid] = it_flat
            it_columns.update(f"v4_it__{key[9:] if key.startswith('product__') else key}" for key in it_flat)

    v4_columns.update({"v4__Color__@id", "v4__Color__EAN_Code", "v4__Color__Image_Color"})
    price_columns = ["v4_price__Excluded_From_Discount"]
    for index in range(1, data["max_tiers"] + 1):
        for field in ("Start", "End", "Price"):
            price_columns.append(f"v4_price__tier_{index}__{field}")

    headers = (
        LEGACY_HEADERS
        + sorted(v4_columns)
        + sorted(color_columns)
        + sorted(category_columns)
        + sorted(it_columns)
        + price_columns
    )

    rows = []
    for product in data["products"]:
        pid = text(product, "Product_Id")
        colors_node = product.find("Colors")
        variants = colors_node.findall("Color") if colors_node is not None else []
        if not variants:
            variants = [None]

        category_ids = [text(node, ".") for node in product.findall("./Categories/Category_Id")]
        category_aggregate: dict[str, list[str]] = defaultdict(list)
        for cid in category_ids:
            for key, value in data["category_flat"].get(cid, {}).items():
                category_aggregate[key].append(value)
        category_aggregate = {key: unique_join(values, ", ") for key, values in category_aggregate.items()}

        for variant in variants:
            row = build_legacy_row(data, product, variant)
            raw_color = variant.attrib.get("id", "") if variant is not None else ""
            compatibility_color = raw_color or "N0999"

            for key, value in product_flat_cache[pid].items():
                row[f"v4__{key}"] = value
            row["v4__Color__@id"] = raw_color
            row["v4__Color__EAN_Code"] = text(variant, "EAN_Code") if variant is not None else ""
            row["v4__Color__Image_Color"] = text(variant, "Image_Color") if variant is not None else ""

            for key, value in data["color_flat"].get(compatibility_color, {}).items():
                row[f"v4_color__{key}"] = value
            for key, value in category_aggregate.items():
                row[f"v4_category__{key}"] = value
            for key, value in translation_flat_cache.get(pid, {}).items():
                clean = key[9:] if key.startswith("product__") else key
                row[f"v4_it__{clean}"] = value

            price = data["prices"].get((pid, compatibility_color), {})
            row["v4_price__Excluded_From_Discount"] = price.get("excluded", "")
            for index, tier in enumerate(price.get("tiers", []), 1):
                for field in ("Start", "End", "Price"):
                    row[f"v4_price__tier_{index}__{field}"] = tier.get(field, "")
            rows.append(row)

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return {"rows": len(rows), "columns": len(headers), "path": str(destination)}


def build_dpo(data, destination: Path) -> dict:
    rows, seen = [], set()
    for pos in data["print_rows"]:
        pid = pos.get("Product_Code", "")
        height, width, diameter = pos.get("Height", ""), pos.get("Width", ""), pos.get("Diameter", "")
        if diameter not in {"", "0"} and height in {"", "0"} and width in {"", "0"}:
            dimensions = f"diam {diameter} mm"
        else:
            dimensions = f"{height}x{width} mm"
        group = pos.get("Print_Group", "").replace("DPN_DW1", "DPN-DW1")
        original_position = pos.get("Position", "")
        translated = data["position_it"].get(original_position) or original_position
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


def build_from_source_root(source_root: Path, output_dir: Path) -> dict:
    data = load_indexes(source_root)
    products = build_products(data, output_dir / "Products.csv")
    dpo = build_dpo(data, output_dir / "DPO PRINT.csv")
    print(f"Products: {products['rows']} righe, {products['columns']} colonne")
    print(f"DPO PRINT: {dpo['rows']} righe")
    return {"products": products, "dpo_print": dpo}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("source_root", type=Path)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    build_from_source_root(args.source_root, args.output_dir)
