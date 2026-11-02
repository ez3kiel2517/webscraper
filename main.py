import sys
from crawl import *
import asyncio
from json_report import write_json_report


async def main():

    #  Grab the URL from the command line: uv run main.py <url>
    if len(sys.argv) < 2:
        print("no website provided")
        print("Usage: python3 main.py <URL>")
        sys.exit(1)

    if len(sys.argv) > 4:
        print("too many arguments provided")
        print("Usage: python3 main.py <URL> <max_concurrency> <max_pages>")
        sys.exit(1)
    
    base_url = sys.argv[1]
    max_concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_MAX_CONCURRENCY
    max_pages = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_MAX_PAGES
    print(f"starting crawl of: {base_url}")

    page_data = await crawl_site_async(base_url, max_concurrency, max_pages)
    print(f"Found {len(page_data)} pages")

    # page_data is keyed by normalized URL; we only need the page dicts here
    # for page in page_data.values():
    #     print(page)

    write_json_report(page_data)

    sys.exit(0)

if __name__ == "__main__":
    # asyncio.run creates the event loop, runs main() to completion, then closes the loop
    asyncio.run(main())
