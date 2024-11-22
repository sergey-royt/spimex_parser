import asyncio
from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from file_downloader import AsyncMemoryFileManager
from model import Base
from link_generator import generate_report_urls
from db import insert_data_frame
from setings import DATABASE_URL
from xls_parser import parse_xls

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
    files_urls = generate_report_urls()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await download_and_parse(files_urls)
    await engine.dispose()


async def download_and_parse(urls: list[str]) -> None:
    """
    Create and launch tasks for downloading
    and parsing each given url from list.
    """

    tasks = []
    for url in urls:
        tasks.append(process_report(url))
    await asyncio.gather(*tasks)


async def process_report(url: str) -> None:
    """
    Download, parse and add to db
    trade report from given url
    """

    async with AsyncMemoryFileManager(url) as file:
        if file:
            report = await parse_xls(file)
            await insert_data_frame(report, async_session)


if __name__ == "__main__":
    asyncio.run(main())
