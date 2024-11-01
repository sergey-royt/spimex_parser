import asyncio
from itertools import chain

from file_downloader import AsyncMemoryFileManager
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from spimex_parser.new.model import Base, TradeReportEntity
from spimex_parser.new.link_collector import get_reports_urls
from spimex_parser.helpers import show_time
from spimex_parser.settings import DATABASE_URL
from xls_parser import parse
from db import add_all


engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)


@show_time
async def main() -> None:
    files_urls = await get_reports_urls()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    entities = await download_and_parse(files_urls)
    await add_all(entities, async_session)
    await engine.dispose()


async def download_and_parse(urls: list[str]) -> list[TradeReportEntity]:
    tasks = []
    for url in urls:
        tasks.append(download_and_parse_single(url))
    entities = await asyncio.gather(*tasks)
    return list(chain.from_iterable(entities))


async def download_and_parse_single(url: str) -> list[TradeReportEntity]:
    url_schema = 'https://spimex.com/{}'

    async with AsyncMemoryFileManager(url_schema.format(url)) as file:
        return await parse(file)


asyncio.run(main())