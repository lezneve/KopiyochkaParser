"""
копійочка: каталог -> postgresql (+ зображення в r2).
сторінки товарів: obscura, якщо знайдено, інакше звичайний aiohttp; ціни й залишки: ajax.
"""

import asyncio
import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
import asyncpg
import aioboto3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

# ====================== CONFIG ======================
PG_URL = os.getenv("DATABASE_URL")
R2_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET = os.getenv("R2_SECRET_KEY")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_BUCKET = os.getenv("R2_BUCKET")
CDN_URL = os.getenv("CDN_URL")

CITY_NAME = "Хмельницький"
CONCURRENCY = 2
RECENT_SKIP_HOURS = 24
MAX_RETRIES = 5
RETRY_BASE_DELAY = 2.0
OBSCURA_TIMEOUT = 20
REQUEST_DELAY = (1.5, 3.0)  # random delay between requests (min, max seconds)

AJAX_URL = "https://www.kopiyochka.ua/user-pannel/admin-ajax.php"
SITEMAP_INDEX = "https://www.kopiyochka.ua/sitemap_index.xml"
BASE_URL = "https://www.kopiyochka.ua"

# Path to obscura binary — check common locations
def find_obscura():
    candidates = [
        Path(__file__).parent / "obscura" / "obscura.exe",
        Path(__file__).parent / "obscura" / "obscura",
        Path("obscura.exe"),
        Path("obscura"),
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    # Try PATH
    import shutil as _shutil
    found = _shutil.which("obscura")
    if found:
        return found
    return None

OBSCURA_BIN = find_obscura()

# ====================== LOGGING ======================
logging.basicConfig(
    level=logging.WARNING,
    format="%(message)s",
)
log = logging.getLogger("kopiyochka")


def is_access_limited(status_code: int, body_text: str) -> bool:
    if status_code in (429, 503):
        return True
    lowered = (body_text or "").lower()
    return any(x in lowered for x in [
        "your access to this site has been limited",
        "wordfence",
        "temporarily limited for security reasons",
    ])


# ====================== SQL ======================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kopiyochka_products (
    product_code TEXT PRIMARY KEY,
    title TEXT,
    url TEXT,
    price NUMERIC(12,2),
    price_old NUMERIC(12,2),
    stock INTEGER DEFAULT 0,
    category TEXT,
    image_url TEXT,
    image_hash TEXT,
    image_base_url TEXT,
    city_name TEXT,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_kopi_image_hash ON kopiyochka_products(image_hash);
CREATE INDEX IF NOT EXISTS idx_kopi_city ON kopiyochka_products(city_name);
"""

UPSERT_SQL = """
INSERT INTO kopiyochka_products (
    product_code, title, url, price, price_old, stock,
    category, image_url, image_hash, image_base_url, city_name, last_checked
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP)
ON CONFLICT (product_code) DO UPDATE SET
    title = EXCLUDED.title,
    url = EXCLUDED.url,
    price = EXCLUDED.price,
    price_old = EXCLUDED.price_old,
    stock = EXCLUDED.stock,
    category = EXCLUDED.category,
    image_url = EXCLUDED.image_url,
    image_hash = COALESCE(EXCLUDED.image_hash, kopiyochka_products.image_hash),
    image_base_url = COALESCE(EXCLUDED.image_base_url, kopiyochka_products.image_base_url),
    city_name = EXCLUDED.city_name,
    last_checked = CURRENT_TIMESTAMP
RETURNING (xmax = 0) as is_new
"""


# ====================== OBSCURA CLIENT ======================

class ObscuraFetcher:
    """Wraps Obscura CLI for anti-detection HTTP fetching."""

    def __init__(self, bin_path: str, timeout: int = OBSCURA_TIMEOUT):
        self.bin = bin_path
        self.timeout = timeout
        self.warmed_up = False

    def _warmup_sync(self):
        """Visit homepage + catalog to establish session cookies."""
        for url in ["https://www.kopiyochka.ua/", "https://www.kopiyochka.ua/catalog/"]:
            try:
                subprocess.run(
                    [self.bin, "fetch", url, "--dump", "original", "--timeout", "15", "--quiet"],
                    capture_output=True, timeout=25,
                )
                time.sleep(1.0)
            except Exception:
                pass
        self.warmed_up = True

    async def warmup(self):
        """Warm up session cookies via Obscura."""
        if not self.warmed_up:
            print("🔥 Прогріваю сесію (homepage + catalog)...")
            await asyncio.to_thread(self._warmup_sync)
            print("✅ Сесія прогріта")

    def _fetch_sync(self, url: str) -> tuple[int, str]:
        """Synchronous fetch via Obscura --dump original (lightweight, no V8)."""
        try:
            r = subprocess.run(
                [self.bin, "fetch", url,
                 "--dump", "original",
                 "--timeout", str(self.timeout),
                 "--quiet"],
                capture_output=True, text=True,
                timeout=self.timeout + 15,
            )
            html = r.stdout
            if is_access_limited(200, html):
                return 429, html
            if not html or len(html) < 100:
                return 500, ""
            return 200, html
        except subprocess.TimeoutExpired:
            return 408, ""
        except Exception:
            return 500, ""

    async def fetch_html(self, url: str, retries: int = MAX_RETRIES) -> tuple[int, str]:
        """Fetch a page via Obscura browser. Returns (status_code, html)."""
        for attempt in range(retries + 1):
            status, html = await asyncio.to_thread(self._fetch_sync, url)

            if status == 429:
                if attempt < retries:
                    delay = min(20.0, RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0.2, 1.0))
                    log.warning(f"⚠️ Wordfence для {url.split('/')[-1]}, ретрай {attempt + 1}/{retries}, {delay:.1f}с")
                    await asyncio.sleep(delay)
                    continue
                return 429, html

            if status in (408, 500) and attempt < retries:
                await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                continue

            return status, html

        return 500, ""

    def _fetch_original_sync(self, url: str, headers: dict = None) -> tuple[int, bytes]:
        """Synchronous raw fetch (runs in thread)."""
        cmd = [self.bin, "fetch", url, "--dump", "original", "--timeout", str(self.timeout), "--quiet"]
        if headers:
            for k, v in headers.items():
                cmd.extend(["--header", f"{k}: {v}"])
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=self.timeout + 15)
            return 200, r.stdout
        except Exception:
            return 500, b""

    async def fetch_original(self, url: str, headers: dict = None, retries: int = 3) -> tuple[int, bytes]:
        """Fetch raw bytes via Obscura --dump original."""
        for attempt in range(retries + 1):
            status, data = await asyncio.to_thread(self._fetch_original_sync, url, headers)
            if status == 200 and data:
                return status, data
            if attempt < retries:
                await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
        return 500, b""


# ====================== AIOHTTP CLIENT ======================

class HttpFetcher:
    """Simple aiohttp-based fetcher for AJAX and image downloads."""

    def __init__(self):
        self.session = None

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit=CONCURRENCY, force_close=True)
            self.session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
                    "X-Requested-With": "XMLHttpRequest",
                }
            )
        return self.session

    async def post_ajax(self, data: dict, referer: str, retries: int = MAX_RETRIES) -> dict | None:
        """POST to AJAX endpoint for pricing."""
        session = await self.ensure_session()
        for attempt in range(retries + 1):
            try:
                async with session.post(
                    AJAX_URL,
                    data=data,
                    headers={"Referer": referer},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 429:
                        if attempt < retries:
                            delay = min(20.0, RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0.2, 1.0))
                            await asyncio.sleep(delay)
                            continue
                        return None
                    if resp.status != 200:
                        return None
                    text = await resp.text()
                    if is_access_limited(resp.status, text):
                        if attempt < retries:
                            await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                            continue
                        return None
                    return json.loads(text)
            except Exception:
                if attempt < retries:
                    await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                    continue
                return None
        return None

    async def fetch_bytes(self, url: str, retries: int = 3) -> bytes | None:
        """Download raw bytes (for images)."""
        session = await self.ensure_session()
        for attempt in range(retries + 1):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    if resp.status == 429 and attempt < retries:
                        await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                        continue
                    return None
            except Exception:
                if attempt < retries:
                    await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
                    continue
                return None
        return None

    async def fetch_html(self, url: str) -> tuple[int, str]:
        """Сторінка без obscura. Повертає (status, html)."""
        session = await self.ensure_session()
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.get(
                    url, headers={"Accept": "text/html"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    text = await resp.text()
                    if is_access_limited(resp.status, text):
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(min(20.0, RETRY_BASE_DELAY * (2 ** attempt)))
                            continue
                        return 429, text
                    return resp.status, text
            except Exception:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
        return 500, ""

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


# ====================== IMAGE HELPERS ======================

def process_image(img_bytes: bytes, size: int, quality: int) -> bytes:
    buf = io.BytesIO()
    with Image.open(io.BytesIO(img_bytes)) as img:
        img = img.convert("RGB")
        img.thumbnail((size, size), Image.Resampling.LANCZOS)
        img.save(buf, format="AVIF", quality=quality, speed=6)
    return buf.getvalue()


def parse_price_str(text: str) -> float:
    cleaned = re.sub(r'<[^>]+>', '', str(text)).strip()
    cleaned = re.sub(r'[^\d,.]', '', cleaned)
    cleaned = cleaned.replace(',', '.')
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


# ====================== SITEMAP ======================

async def fetch_product_urls_http(session: aiohttp.ClientSession) -> list[str]:
    """Collect product URLs from sitemap via aiohttp."""
    print("📦 Завантажую sitemap_index.xml...")

    product_urls = set()
    try:
        async with session.get(SITEMAP_INDEX, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                print(f"❌ Sitemap index HTTP {resp.status}")
                return []
            text = await resp.text()
    except Exception as e:
        print(f"❌ Помилка завантаження sitemap: {e}")
        return []

    sitemap_urls = re.findall(r'<loc>(https://www\.kopiyochka\.ua/[^<]*\.xml)</loc>', text)
    catalog_sitemaps = [u for u in sitemap_urls if 'catalog' in u or 'product' in u]
    if not catalog_sitemaps:
        catalog_sitemaps = sitemap_urls

    print(f"📄 Знайдено {len(catalog_sitemaps)} sitemap-файлів")

    for sitemap_url in catalog_sitemaps:
        try:
            async with session.get(sitemap_url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    continue
                sm_text = await resp.text()
        except Exception:
            continue

        urls = re.findall(r'<loc>(https://www\.kopiyochka\.ua/[^<]*)</loc>', sm_text)
        new_urls = [
            u for u in urls
            if "/catalog/" in u and ".xml" not in u and not u.rstrip("/").endswith("/catalog")
        ]
        product_urls.update(new_urls)
        if new_urls:
            print(f"  → {sitemap_url.split('/')[-1]}: {len(new_urls)} товарів")
        await asyncio.sleep(0.15)

    print(f"✅ Всього зібрано {len(product_urls)} унікальних URL товарів")
    return sorted(product_urls)


# ====================== PARSING ======================

def parse_product_page(html: str, url: str) -> dict | None:
    """Parse product page HTML → extract code, title, image, category."""
    if not html or len(html) < 100:
        return None

    # Product code
    code_match = re.search(r'productCode\s*=\s*["\']?(\d+)', html, re.IGNORECASE)
    if not code_match:
        code_match = re.search(r'product_code\s*[:=]\s*["\']?(\d+)', html, re.IGNORECASE)
    if not code_match:
        soup = BeautifulSoup(html, 'html.parser')
        sku_el = soup.find(string=re.compile(r'Артикул', re.IGNORECASE))
        if sku_el:
            num_match = re.search(r'(\d{4,})', sku_el.parent.get_text() if sku_el.parent else str(sku_el))
            if num_match:
                code_match = num_match
    if not code_match:
        return None

    product_code = code_match.group(1) if hasattr(code_match, 'group') else str(code_match)

    soup = BeautifulSoup(html, 'html.parser')

    # Title
    h1 = soup.find('h1')
    title = h1.get_text(strip=True) if h1 else f"Товар {product_code}"

    # Image
    image_url = None
    og_img = soup.find('meta', property='og:image')
    if og_img and og_img.get('content'):
        image_url = og_img['content']
    if not image_url:
        for img in soup.find_all('img'):
            src = img.get('src', '') or img.get('data-src', '')
            if src and ('product' in src.lower() or 'upload' in src.lower() or 'wp-content' in src.lower()):
                image_url = src
                break

    # Category from breadcrumbs
    category = ""
    breadcrumbs = soup.find(class_=re.compile(r'breadcrumb', re.IGNORECASE))
    if breadcrumbs:
        crumbs = breadcrumbs.find_all('a')
        if len(crumbs) >= 2:
            category = crumbs[-1].get_text(strip=True)

    return {
        "product_code": product_code,
        "title": title,
        "url": url,
        "image_url": image_url,
        "category": category,
    }


def _price_from_html(price_html) -> tuple[float, float]:
    soup = BeautifulSoup(str(price_html), 'html.parser')
    ins, del_tag = soup.find('ins'), soup.find('del')
    if ins and del_tag:
        return parse_price_str(ins.get_text()), parse_price_str(del_tag.get_text())
    return parse_price_str(price_html), 0.0


def _price_from_item(item: dict) -> tuple[float, float]:
    """Ціна магазину: нове api дає base/promo_unit_price, старе — formatted_price."""
    base = parse_price_str(item.get('base_unit_price') or 0)
    promo = parse_price_str(item.get('promo_unit_price') or 0)
    if base > 0 and 0 < promo < base:
        return promo, base
    if base > 0:
        return base, 0.0
    return _price_from_html(item.get('formatted_price', ''))


def parse_price_response(data: dict, city: str = CITY_NAME) -> tuple[float, float, int] | None:
    """Відповідь ajax → (price, price_old, total_stock) або None, якщо в місті товару немає.

    Зараз сайт віддає магазини в `map_items` (усі міста), а `city_items`
    порожній; тому беремо магазини потрібного міста звідти.
    """
    if not data:
        return None
    items = data.get('city_items') or []
    if not items and data.get('current_place'):
        items = [data['current_place']]
    if not items:
        items = [m for m in (data.get('map_items') or []) if m.get('city') == city]
    if not items:
        return None

    total_stock = sum(int(float(i.get('stock_available') or 0)) for i in items)
    price = price_old = 0.0
    for item in items:
        price, price_old = _price_from_item(item)
        if price > 0:
            break
    return price, price_old, total_stock


# ====================== DUMPER ======================

class KopiyochkaDumper:
    def __init__(self, city: str = CITY_NAME, skip_images: bool = False, skip_recent_hours: int = 0,
                 limit: int = 0):
        self.limit = limit
        self.city = city
        self.skip_images = skip_images
        self.skip_recent = skip_recent_hours
        self.db_pool = None
        self.r2_session = aioboto3.Session()
        self.obscura = None
        self.http = HttpFetcher()
        self.recent_codes = set()
        self.stats = {
            'new': 0, 'updated': 0, 'unchanged': 0,
            'images': 0, 'errors': 0, 'discounts': 0,
            'rate_limited': 0, 'retried': 0, 'unavailable': 0,
        }
        self.processed = 0
        self.total = 0
        self.started_at = None

    async def run(self):
        self.started_at = time.monotonic()
        if OBSCURA_BIN:
            self.obscura = ObscuraFetcher(OBSCURA_BIN)
            await self.obscura.warmup()
        else:
            print("obscura не знайдено, сторінки беру через aiohttp")

        # 1. DB setup
        self.db_pool = await asyncpg.create_pool(PG_URL)
        async with self.db_pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            if self.skip_recent > 0:
                rows = await conn.fetch(
                    "SELECT product_code FROM kopiyochka_products "
                    "WHERE last_checked >= NOW() - ($1 * INTERVAL '1 hour')",
                    self.skip_recent
                )
                self.recent_codes = {r['product_code'] for r in rows}
                if self.recent_codes:
                    print(f"⏩ Пропускаємо {len(self.recent_codes)} товарів (оновлено < {self.skip_recent} год)")

        # 2. Get URLs from sitemap
        session = await self.http.ensure_session()
        product_urls = await fetch_product_urls_http(session)
        if self.limit:
            product_urls = product_urls[:self.limit]
        self.total = len(product_urls)
        if not product_urls:
            print("❌ Немає URL для обробки")
            return

        print(f"🚀 Старт: {self.total} товарів, місто: {self.city}")

        # 3. Process in batches
        semaphore = asyncio.Semaphore(CONCURRENCY)
        tasks = [self._process_one(url, semaphore) for url in product_urls]
        await asyncio.gather(*tasks, return_exceptions=True)

        # 4. Summary
        elapsed = time.monotonic() - self.started_at
        print(f"\n\n📊 Підсумок ({elapsed:.0f}с): "
              f"нові={self.stats['new']}, оновлено={self.stats['updated']}, "
              f"без змін={self.stats['unchanged']}, фото={self.stats['images']}, "
              f"знижки={self.stats['discounts']}, немає в місті={self.stats['unavailable']}, помилки={self.stats['errors']}")

        await self.http.close()
        if self.db_pool:
            await self.db_pool.close()

    async def _process_one(self, url: str, sem: asyncio.Semaphore):
        async with sem:
            # Random delay between requests to avoid rate limiting
            await asyncio.sleep(random.uniform(*REQUEST_DELAY))
            try:
                await self._handle_product(url)
            except Exception as e:
                self.stats['errors'] += 1
                print(f"❌ {url.split('/')[-1]}: {e}")
            self.processed += 1
            if self.processed % 25 == 0 or self.processed == self.total:
                elapsed = time.monotonic() - self.started_at
                rate = self.processed / max(elapsed, 0.001)
                pct = self.processed / max(self.total, 1) * 100
                eta = (self.total - self.processed) / rate if rate > 0 else 0
                print(f"  [{self.processed}/{self.total}] {pct:.0f}% — {rate:.1f}/с — "
                      f"нові:{self.stats['new']} онов:{self.stats['updated']} без змін:{self.stats['unchanged']} "
                      f"ETA:{int(eta//60)}х{int(eta%60):02d}с")

    async def _handle_product(self, url: str):
        # Fetch page via Obscura
        fetcher = self.obscura or self.http
        status, html = await fetcher.fetch_html(url)
        if status != 200:
            self.stats['errors'] += 1
            return

        info = parse_product_page(html, url)
        if not info:
            self.stats['errors'] += 1
            return

        code = info["product_code"]
        if code in self.recent_codes:
            self.stats['unchanged'] += 1
            return

        # Get price via AJAX
        ajax_data = {
            'action': 'get_price_and_remain',
            'product_code': code,
            'city_name': self.city,
            'load_all': '1',
        }
        price_resp = await self.http.post_ajax(ajax_data, referer=url)
        if not price_resp:
            self.stats['errors'] += 1
            return

        parsed = parse_price_response(price_resp, self.city)
        if parsed is None:
            self.stats['unavailable'] += 1
            return
        price, price_old, stock = parsed

        # Log big discounts
        if price_old > 0 and price > 0 and price < price_old:
            disc = int((1 - price / price_old) * 100)
            if disc >= 50:
                self.stats['discounts'] += 1
                print(f"🔥 -{disc}% {info['title'][:48]} | {price} грн (було {price_old})")

        # Handle image
        img_hash = None
        img_base_url = None
        if not self.skip_images and info.get("image_url"):
            img_hash, img_base_url = await self._handle_image(code, info["image_url"])
            if img_base_url:
                self.stats['images'] += 1

        # Save to DB
        await self._save(code, info, price, price_old, stock, img_hash, img_base_url)

    async def _handle_image(self, code: str, image_url: str) -> tuple[str | None, str | None]:
        if not image_url or not image_url.startswith("http"):
            return None, None

        img_bytes = await self.http.fetch_bytes(image_url)
        if not img_bytes:
            return None, None

        img_hash = hashlib.sha256(img_bytes).hexdigest()

        # Check dedup
        async with self.db_pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT image_base_url FROM kopiyochka_products WHERE image_hash = $1 LIMIT 1",
                img_hash
            )
            if existing and existing['image_base_url']:
                return img_hash, existing['image_base_url']

        # Process + upload
        t_bytes = await asyncio.to_thread(process_image, img_bytes, 256, 35)
        p_bytes = await asyncio.to_thread(process_image, img_bytes, 512, 45)

        async with self.r2_session.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_KEY,
            aws_secret_access_key=R2_SECRET
        ) as s3:
            await s3.put_object(
                Bucket=R2_BUCKET, Key=f"kopiyochka/{code}/thumb.avif",
                Body=t_bytes, ContentType="image/avif",
                CacheControl="public, max-age=31536000, immutable"
            )
            await s3.put_object(
                Bucket=R2_BUCKET, Key=f"kopiyochka/{code}/preview.avif",
                Body=p_bytes, ContentType="image/avif",
                CacheControl="public, max-age=31536000, immutable"
            )

        return img_hash, f"{CDN_URL}/kopiyochka/{code}"

    async def _save(self, code, info, price, price_old, stock, img_hash, img_base_url):
        try:
            async with self.db_pool.acquire() as conn:
                old = await conn.fetchrow(
                    "SELECT title, price, price_old, stock, category, image_hash "
                    "FROM kopiyochka_products WHERE product_code = $1", code
                )

                if old:
                    changed = []
                    if str(old["title"] or "") != info["title"]:
                        changed.append("назва")
                    if abs(float(old["price"] or 0) - price) > 0.001:
                        changed.append(f"ціна")
                    if abs(float(old["price_old"] or 0) - price_old) > 0.001:
                        changed.append("стара ціна")
                    if int(old["stock"] or 0) != stock:
                        changed.append("залишок")
                    if not changed:
                        await conn.execute(
                            "UPDATE kopiyochka_products SET last_checked = CURRENT_TIMESTAMP WHERE product_code = $1",
                            code
                        )
                        self.stats['unchanged'] += 1
                        return

                row = await conn.fetchrow(
                    UPSERT_SQL,
                    code, info["title"], info["url"], price, price_old, stock,
                    info["category"], info.get("image_url"), img_hash, img_base_url, self.city
                )

                if row and row.get('is_new'):
                    self.stats['new'] += 1
                else:
                    self.stats['updated'] += 1

        except Exception as e:
            self.stats['errors'] += 1
            print(f"❌ DB [{code}]: {e}")


# ====================== MAIN ======================

def main():
    import argparse
    ap = argparse.ArgumentParser(description="копійочка -> postgresql")
    ap.add_argument("--no-images", action="store_true", help="не завантажувати зображення в r2")
    ap.add_argument("--recent", type=int, default=0, metavar="ГОД",
                    help="пропустити товари, оновлені за останні N годин")
    ap.add_argument("--limit", type=int, default=0, metavar="N", help="лише перші N товарів (для перевірки)")
    ap.add_argument("--city", default=CITY_NAME)
    a = ap.parse_args()
    if not PG_URL:
        sys.exit("задайте DATABASE_URL у .env")
    if not a.no_images and not all([R2_KEY, R2_SECRET, R2_ENDPOINT, R2_BUCKET, CDN_URL]):
        sys.exit("для зображень потрібні змінні R2_* і CDN_URL; або --no-images")
    print(f"місто: {a.city}; obscura: {OBSCURA_BIN or 'немає (aiohttp)'}")
    asyncio.run(KopiyochkaDumper(a.city, a.no_images, a.recent, a.limit).run())


if __name__ == "__main__":
    main()
