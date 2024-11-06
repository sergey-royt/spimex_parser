import asyncio
from datetime import datetime
from itertools import chain

import aiohttp
from bs4 import BeautifulSoup


UP_TO_DATE = datetime(2022, 12, 31)
PAGE_TO_PARSE_RANGE = range(1, 48)
DATE_SPAN_IDX = 0
URL_SCHEMA = (
    "https://spimex.com/markets/oil_products/trades/results/?page=page-{}"
)


async def download_page_content(url: str) -> bytes:
    """
    Make request to given url and return the response content
    """

    user_agent = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=user_agent) as response:
            return await response.read()


async def parse_page(page_content: bytes) -> list[str]:
    """
    Parse page to find trade report urls
    """

    soup = BeautifulSoup(page_content, "html.parser")
    blocks = soup.find("div", "page-content__tabs__block").find_all(
        "div", "accordeon-inner__item"
    )
    file_urls = []
    for block in blocks:
        file_date_str = block.span.contents[DATE_SPAN_IDX]
        file_date = datetime.strptime(file_date_str, "%d.%m.%Y")
        if file_date > UP_TO_DATE:
            file_link = block.a["href"]
            file_urls.append(file_link)
        else:
            break
    return file_urls


async def get_reports_urls() -> list[str]:
    """
    Download page content parse it to find trade report urls
    and return list of all trade report urls
    """

    download_pages_content_tasks = [
        download_page_content(URL_SCHEMA.format(i))
        for i in PAGE_TO_PARSE_RANGE
    ]

    pages_content = await asyncio.gather(*download_pages_content_tasks)

    trade_report_urls_tasks = [parse_page(page) for page in pages_content]
    trade_report_urls = await asyncio.gather(*trade_report_urls_tasks)

    return list(chain.from_iterable(trade_report_urls))
