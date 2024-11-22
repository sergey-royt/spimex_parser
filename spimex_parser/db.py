from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
import pandas as pd

from model import TradeReportEntity


async def insert_data_frame(
    df: pd.DataFrame,
    async_session: async_sessionmaker[AsyncSession],
) -> None:
    """
    Insert data frame to database
    """

    async with async_session() as session:
        async with session.begin():
            await session.execute(
                TradeReportEntity.__table__.insert(),
                df.to_dict(orient="records")
            )
