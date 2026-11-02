from urllib.parse import urlsplit, urljoin, urlparse
from bs4 import BeautifulSoup, Tag
import pprint
from typing import TypedDict
import requests
import asyncio
import aiohttp


DEFAULT_MAX_CONCURRENCY = 10
DEFAULT_MAX_PAGES = 100


class PageData(TypedDict):
    url: str
    heading: str
    first_paragraph: str
    outgoing_links: list[str]
    image_urls: list[str]


class AsyncCrawler:
    def __init__(self, base_url: str, max_concurrency: int = DEFAULT_MAX_CONCURRENCY, max_pages: int = DEFAULT_MAX_PAGES):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
        self.page_data = {}
        self.visited = set()
        self.lock = asyncio.Lock()
        self.max_concurrency = max_concurrency
        self.max_pages = max_pages
        self.should_stop = False
        self.all_tasks = set()
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.session = None

    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
        headers={"User-Agent": "BootCrawler/1.0"}
        )
        return self


    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def add_page_visit(self, normalized_url):
        async with self.lock:
            if len(self.visited) >= self.max_pages:
                self.should_stop = True
                print("Reached maximum number of pages to crawl.")
                return False

            if normalized_url in self.visited:
                return False
            
            self.visited.add(normalized_url)
            return True

    async def get_html(self, url: str) -> str | None:
        try:
            async with self.session.get(url) as resp:
                if resp.status >= 400:
                    print(f"Skipping HTTP {resp.status}: {url}")
                    return None

                content_type = resp.headers.get("content-type", "")
                if "text/html" not in content_type.lower():
                    print(f"Skipping non-HTML ({content_type}): {url}")
                    return None

                return await resp.text()

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise Exception(f"network error while fetching {url}: {e}") from e


    async def crawl_page(self, url: str | None = None):
        # check if max page limit has been reached
        if self.should_stop:
            return

        if url is None:
            url = self.base_url

        # check if current url is still part of the original base url
        if urlparse(url).netloc != self.base_domain:
            return

        # get normalized url
        normalized = normalize_url(url)

        # atomically claim this URL; False means someone else already has it
        if not await self.add_page_visit(normalized):
            return

        print(f"Crawling: {url}")

        async with self.semaphore: # permit held only during the fetch
            html = await self.get_html(url)

        if html is None:
            return  # skip this URL, the caller's loop moves on to the next link

        # extract data and add it to page_data
        data = extract_page_data(html, url)
        async with self.lock:
            self.page_data[normalized] = data
            reached_limit = len(self.page_data) >= self.max_pages

        if reached_limit:
            self.stop_crawling()
            return

        # create task for each url and crawl all links concurrently
        tasks = []
        try:
            for next_url in data["outgoing_links"]:
                task = asyncio.create_task(self.crawl_page(next_url))
                self.all_tasks.add(task)
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    print(f"Task error: {result}")

        finally:
            # Runs on success, on error, and when this task is cancelled,
            # so finished tasks never linger in the set
            # Use discard, not remove. remove raises KeyError if the item is missing, and discard doesn’t.
            for task in tasks:
                self.all_tasks.discard(task)


    async def crawl(self):

        # Start the recursive crawl at the base URL.
        # crawl_page only returns once every page it spawned has finished,
        # so when this await completes, the whole site has been crawled.
        await self.crawl_page(self.base_url)

        # All tasks write into self.page_data, so just return the shared dict
        return self.page_data


    def stop_crawling(self):
        # Tell any task that hasn't started yet to bail out
        self.should_stop = True

        # Don't cancel the task that is calling this method
        current = asyncio.current_task()

        # list(...) copies the set, because tasks remove themselves
        # in their finally blocks while we iterate
        for task in list(self.all_tasks):
            if task is not current:
                task.cancel()


def normalize_url(url: str) -> str:
    parsed_url = urlsplit(url)
    full_path = f"{parsed_url.netloc}{parsed_url.path}"
    full_path = full_path.rstrip("/")
    return full_path.lower()


def get_heading_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    h_tag = soup.find("h1") or soup.find("h2")
    return h_tag.get_text(strip=True) if isinstance(h_tag, Tag) else ""


def get_first_paragraph_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    main_section = soup.find("main")
    if isinstance(main_section, Tag):
        first_p = main_section.find("p")
    else:
        first_p = soup.find("p")

    return first_p.get_text(strip=True) if isinstance(first_p, Tag) else ""

def get_urls_from_html(html: str, base_url: str) -> list[str]:
    """Return all unique absolute URLs found in an HTML string."""
    soup = BeautifulSoup(html, "html.parser")

    # (tag, attribute) pairs that commonly contain URLs
    targets = [
        ("a", "href"),
        ("link", "href"),
        ("img", "src"),
        ("script", "src"),
        ("iframe", "src"),
        ("source", "src"),
        ("video", "src"),
        ("audio", "src"),
        ("form", "action"),
    ]

    # Respect a <base href="..."> tag if the document defines one
    base_tag = soup.find("base", href=True)
    if base_tag:
        base_url = urljoin(base_url, base_tag["href"])

    urls = []
    seen = set()

    for tag_name, attr in targets:
        for tag in soup.find_all(tag_name, **{attr: True}):
            raw = tag[attr].strip()

            # Skip empty values, anchors, and non-http schemes
            if not raw or raw.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
                continue

            # check if url is image related
            filter_out_terms = ["image", "img", ".css"]
            if any(substring in raw for substring in filter_out_terms):
                continue

            absolute = urljoin(base_url, raw)

            # Keep only http(s) URLs
            if urlparse(absolute).scheme not in ("http", "https"):
                continue

            if absolute not in seen:
                seen.add(absolute)
                urls.append(absolute)

    return urls

def get_images_from_html(html: str, base_url: str) -> list[str]:
    """Return unique absolute URLs of images found in an HTML string."""
    soup = BeautifulSoup(html, "html.parser")

    # Respect a <base href="..."> tag if the document defines one
    base_tag = soup.find("base", href=True)
    if base_tag:
        base_url = urljoin(base_url, base_tag["href"])

    urls = []
    candidates = []
    seen = set()

    # Single-URL attributes
    for tag in soup.find_all(["img", "source"]):
        for attr in ("src", "data-src"):
            if tag.get(attr):
                candidates.append(tag[attr])

    # srcset: "small.jpg 480w, large.jpg 1080w" -> take the URL part of each entry
    for tag in soup.find_all(["img", "source"], srcset=True):
        for entry in tag["srcset"].split(","):
            parts = entry.strip().split()
            if parts:
                candidates.append(parts[0])

    for raw in candidates:
        raw = raw.strip()

        # Skip empty values
        if not raw:
            continue

        absolute = urljoin(base_url, raw)

        if urlparse(absolute).scheme not in ("http", "https"):
            continue

        if absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)
    
    return urls


def extract_page_data(html: str, page_url: str) -> PageData:
    return {
        "url": page_url,
        "heading": get_heading_from_html(html),
        "first_paragraph": get_first_paragraph_from_html(html),
        "outgoing_links": get_urls_from_html(html, page_url),
        "image_urls": get_images_from_html(html, page_url),
    }

async def crawl_site_async(
    base_url: str,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    max_pages: int = DEFAULT_MAX_PAGES,
):
    # "async with" calls __aenter__ on entry and __aexit__ on exit,
    # which guarantees the aiohttp session is closed, even if the crawl crashes
    async with AsyncCrawler(base_url, max_concurrency, max_pages) as crawler:
        # Run the crawl and collect the finished dictionary
        page_data = await crawler.crawl()

    # The session is closed by now; page_data is just a normal dict
    return page_data

