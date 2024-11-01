from spimex_parser.new.model import TradeReportEntity


async def add_all(entities: list[TradeReportEntity], async_session) -> None:
    async with async_session() as session:
        async with session.begin():
            session.add_all(entities)
