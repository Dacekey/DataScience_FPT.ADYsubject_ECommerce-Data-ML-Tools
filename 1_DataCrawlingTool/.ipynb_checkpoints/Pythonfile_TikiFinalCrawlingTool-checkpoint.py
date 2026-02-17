import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random

# ===========================================================================================================================
# MODIFY HERE
# 1. Choose Tiki category
category_name = "nha-sach-tiki"
category_numb = "8322"
filename_output = "tiki_library"
# 2. Choose number of pages
START_PAGE = 1
END_PAGE = 2
# ===========================================================================================================================

PAGE_SLEEP_MIN = 1
PAGE_SLEEP_MAX = 2.5

# Default
url = "https://tiki.vn/api/personalish/v1/blocks/listings"

pages_default = 1
MAX_WORKERS = 10
rows = []

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/147.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": f"https://tiki.vn/{category_name}/c{category_numb}"
}
HEADERS = {
    "user-agent": "Mozilla/5.0",
    "accept": "application/json",
}

params = {
    "category": int(category_numb),
    "page": pages_default,
    "limit": 40,
    "sort": "top_seller",
    "urlKey": category_name
}

def make_session(headers: dict):
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.headers.update(headers)
    return s

def get_json(url: str, params: dict | None = None, timeout: int = 30) -> dict:
    r = SESSION.get(url, params=params, timeout=timeout)
    # gentle handling for 429 (rate limit)
    if r.status_code == 429:
        import time, random
        time.sleep(1.0 + random.random())
        r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_product_detail(product_id: int, spid: int | None = None) -> dict:
    url = f"https://tiki.vn/api/v2/products/{product_id}"
    params = {"platform": "web", "version": 3}
    if spid is not None:
        params["spid"] = spid
    return get_json(url, params=params, timeout=30)

def _fmt_kv(pairs: list[tuple[str, object]]) -> str:
    # format: "k:v; k:v; ..."
    return "; ".join([f"{k}:{v}" for k, v in pairs])


def parse_detail_fields(detail: dict) -> dict:
    out = {}

    # ---- 2) Tracking_info_amplitude ----
    amp = (
        (detail.get("tracking_info") or {}).get("amplitude")
        if isinstance(detail.get("tracking_info"), dict)
        else None
    )
    if isinstance(amp, dict):
        out["tracking_info_amplitude"] = _fmt_kv([
            ("is_authentic", amp.get("is_authentic")),
            ("is_freeship_xtra", amp.get("is_freeship_xtra")),
            ("is_hero", amp.get("is_hero")),
            ("is_top_brand", amp.get("is_top_brand")),
            ("return_reason", amp.get("return_reason")),
        ])
    else:
        out["tracking_info_amplitude"] = ""

    # ---- 3-6) inventory + versions ----
    out["inventory_status"] = detail.get("inventory_status")
    out["inventory_type"] = detail.get("inventory_type")
    out["data_version"] = detail.get("data_version")
    out["day_ago_created"] = detail.get("day_ago_created")

    # ---- 7) brand OR author (UPDATED) ----
    brand_or_author = ""

    # ---- 1) Try AUTHORS first ----
    authors = detail.get("authors")
    if isinstance(authors, list) and authors:
        ids = []
        names = []
        for a in authors:
            if isinstance(a, dict):
                if a.get("id") is not None:
                    ids.append(str(a.get("id")))
                if a.get("name"):
                    names.append(a.get("name"))

        brand_or_author = _fmt_kv([
            ("id", ",".join(ids) if ids else None),
            ("name", ", ".join(names) if names else None),
        ])

    # ---- 2) Fallback to BRAND if no author ----
    if not brand_or_author:
        brand = detail.get("brand")
        if isinstance(brand, dict):
            brand_or_author = _fmt_kv([
                ("id", brand.get("id")),
                ("name", brand.get("name")),
            ])

    out["brand_or_author"] = brand_or_author


    # ---- 8) current_seller ----
    cs = detail.get("current_seller") if isinstance(detail.get("current_seller"), dict) else None
    if isinstance(cs, dict):
        out["current_seller"] = _fmt_kv([
            ("id", cs.get("id")),
            ("sku", cs.get("sku")),
            ("name", cs.get("name")),
            ("store_id", cs.get("store_id")),
            ("is_best_store", cs.get("is_best_store")),
            ("is_offline_installment_supported", cs.get("is_offline_installment_supported")),
        ])
    else:
        out["current_seller"] = ""

    # ---- 9) stock_item ----
    stock = detail.get("stock_item") if isinstance(detail.get("stock_item"), dict) else None
    if isinstance(stock, dict):
        out["stock_item"] = _fmt_kv([
            ("max_sale_qty", stock.get("max_sale_qty")),
            ("min_sale_qty", stock.get("min_sale_qty")),
            ("qty", stock.get("qty")),
        ])
    else:
        out["stock_item"] = ""

    # ---- 10) categories (DIRECT categories dict) ----
    categories_str = ""
    cat = detail.get("categories")

    if isinstance(cat, dict):
        categories_str = _fmt_kv([
            ("id", cat.get("id")),
            ("name", cat.get("name")),
            ("is_leaf", cat.get("is_leaf")),
        ])
    elif isinstance(cat, list) and cat:
        # fallback: if some products return list, take the last/leaf
        leaf = cat[-1] if isinstance(cat[-1], dict) else None
        if isinstance(leaf, dict):
            categories_str = _fmt_kv([
                ("id", leaf.get("id")),
                ("name", leaf.get("name")),
                ("is_leaf", leaf.get("is_leaf")),
            ])

    out["categories"] = categories_str

    # ---- 11) benefits_count ----
    benefits = detail.get("benefits")
    if isinstance(benefits, list):
        out["benefits_count"] = len(benefits)
    elif isinstance(benefits, dict):
        out["benefits_count"] = len(benefits)
    else:
        out["benefits_count"] = 0

    return out

def fetch_review_summary(product_id: int, spid: int | None = None, seller_id: int | None = None, page: int = 1, limit: int = 5) -> dict:
    url = "https://tiki.vn/api/v2/reviews"
    params = {
        "limit": limit,
        "include": "comments,contribute_info,attribute_vote_summary",
        "sort": "score|desc,id|desc,stars|all",
        "page": page,
        "product_id": product_id,
    }
    if spid is not None:
        params["spid"] = spid
    if seller_id is not None:
        params["seller_id"] = seller_id

    return get_json(url, params=params, timeout=30)

def parse_review_fields(review_payload: dict) -> dict:
    out = {}

    # ---- rating_average / reviews_count ----
    out["rating_average"] = review_payload.get("rating_average")
    out["reviews_count"] = review_payload.get("reviews_count")

    # Sometimes nested
    if out["rating_average"] is None or out["reviews_count"] is None:
        rating_sum = review_payload.get("rating_summary")
        if isinstance(rating_sum, dict):
            out["rating_average"] = out["rating_average"] or rating_sum.get("rating_average")
            out["reviews_count"] = out["reviews_count"] or rating_sum.get("reviews_count")

    # ---- stars distribution ----
    stars_obj = review_payload.get("stars")

    if not isinstance(stars_obj, dict):
        rating_sum = review_payload.get("rating_summary")
        if isinstance(rating_sum, dict):
            stars_obj = rating_sum.get("stars")

    # Format required: "1:count_x,percent_y;2:count_x,percent_y;..."
    stars_parts = []
    if isinstance(stars_obj, dict):
        for star in [1, 2, 3, 4, 5]:
            s = stars_obj.get(str(star)) or stars_obj.get(star)
            if isinstance(s, dict):
                count = s.get("count")
                percent = s.get("percent")
                if count is not None or percent is not None:
                    stars_parts.append(f"{star}:count_{count},percent_{percent}")
            else:
                # fallback: if API uses direct numbers (rare)
                if s is not None:
                    stars_parts.append(f"{star}:count_{s},percent_None")

    out["stars"] = ";".join(stars_parts) if stars_parts else ""
    return out

def normalize_quantity_sold(x):
    if isinstance(x, dict):
        return x.get("value")
    return x

def enrich_one_product(p: dict) -> dict:
    """Fetch B & C for one product item and return merged row dict."""
    detail_product_id = p.get("id")
    detail_seller_product_id = p.get("seller_product_id")
    detail_seller_id = p.get("seller_id")

    # Fetch B & C (still sequential inside one item; overall parallel across items)
    product_detail = parse_detail_fields(fetch_product_detail(detail_product_id, detail_seller_product_id))
    product_review = parse_review_fields(fetch_review_summary(detail_product_id, detail_seller_product_id, detail_seller_id))

    return {
        # ===== Identity =====
        "id": p.get("id"),
        "sku": p.get("sku"),
        "name": p.get("name"),
        "seller_product_id": p.get("seller_product_id"),
        "seller_id": p.get("seller_id"),

        # ===== Product Detail (B) =====
        "tracking_info_amplitude": product_detail.get("tracking_info_amplitude"),
        "inventory_status": product_detail.get("inventory_status"),
        "inventory_type": product_detail.get("inventory_type"),
        "data_version": product_detail.get("data_version"),
        "day_ago_created": product_detail.get("day_ago_created"),
        "brand_or_author": product_detail.get("brand_or_author"),
        "current_seller": product_detail.get("current_seller"),
        "stock_item": product_detail.get("stock_item"),
        "categories": product_detail.get("categories"),
        "benefits_count": product_detail.get("benefits_count"),

        # ===== Product Review (C) =====
        "rating_average": product_review.get("rating_average"),
        "reviews_count": product_review.get("reviews_count"),
        "stars": product_review.get("stars"),

        # ===== Product grouping =====
        "productset_id": p.get("productset_id"),
        "primary_category_path": p.get("primary_category_path"),

        # ===== Pricing =====
        "price": p.get("price"),
        "original_price": p.get("original_price"),
        "discount": p.get("discount"),
        "discount_rate": p.get("discount_rate"),

        # ===== Trust / Social proof =====
        "favourite_count": p.get("favourite_count"),

        # ===== Badges =====
        "badges_new": p.get("badges_new"),
        "badges_v3": p.get("badges_v3"),

        # ===== Sales / Exposure =====
        "quantity_sold": normalize_quantity_sold(p.get("quantity_sold")),
        "product_reco_score": p.get("product_reco_score"),

        # ===== Inventory / Availability =====
        "availability": p.get("availability"),
        "shippable": p.get("shippable"),
    }

SESSION = make_session(HEADERS)

for page in range(START_PAGE, END_PAGE + 1):
    
    params["page"] = page
    r = requests.get(url, headers=headers, params=params, timeout=30)
    print("Status:", r.status_code)
    r.raise_for_status()
    j = r.json()
    
    products = j.get("data", [])

    # j = get_json(url, params=params)

    if not products:
        break

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(enrich_one_product, p) for p in products]
        for fut in as_completed(futures):
            rows.append(fut.result())

    time.sleep(random.uniform(PAGE_SLEEP_MIN, PAGE_SLEEP_MAX))


df = pd.DataFrame(rows)
print(j["paging"])

# Export to .csv file
df.to_csv(f"{filename_output}.csv", index=False, encoding="utf-8-sig")