import asyncio
from itertools import chain
from typing import Any

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from file_downloader import AsyncMemoryFileManager
from model import Base, TradeReportEntity
from link_collector import get_reports_urls
from db import add_all
from setings import DATABASE_URL
from xls_parser import SpimexXlsParser


SITE_URL = "https://spimex.com/{}"


engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def main() -> None:
    """
    Get all trade report urls.
    Create database table.
    Download and parse all reports
    and create TradeReportEntity objects from it.
    Add all TradeReportEntities to database.
    """

    files_urls = await get_reports_urls()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    entities = await download_and_parse(files_urls)
    await add_all(entities, async_session)
    await engine.dispose()


async def download_and_parse(urls: list[str]) -> list[TradeReportEntity]:
    """
    Create and launch tasks for downloading
    and parsing each given url from list
    Return list of TradeReportEntities
    """

    tasks = []
    for url in urls:
        tasks.append(download_and_parse_single(url))
    entities = await asyncio.gather(*tasks)
    return list(chain.from_iterable(entities))


async def download_and_parse_single(url: str) -> Any:
    """
    Download and parse trade report from given url
    """

    async with AsyncMemoryFileManager(SITE_URL.format(url)) as file:
        return await SpimexXlsParser(file).parse()


if __name__ == "__main__":
    asyncio.run(main())
