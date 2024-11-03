from model import TradeReportEntity
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


async def add_all(
    entities: list[TradeReportEntity], async_session: async_sessionmaker[AsyncSession]
) -> None:
    """
    Add all TradeReportEntities from given list to database
    """

    async with async_session() as session:
        async with session.begin():
            session.add_all(entities)
