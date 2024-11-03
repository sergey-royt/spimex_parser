from datetime import datetime
from io import BytesIO
import xlrd
from model import TradeReportEntity
from decimal import Decimal


async def parse(file: BytesIO) -> list[TradeReportEntity]:
    """
    Find non-empty rows in trade report XLS file and create
    TradeReportEntities from them.
    """

    workbook = xlrd.open_workbook(file_contents=file.getvalue())
    sheet = workbook.sheet_by_index(0)
    date_line = sheet.row(3)[1].value
    date_part = date_line.split(": ")[1]
    trading_date = datetime.strptime(date_part, "%d.%m.%Y").date()
    res: list[TradeReportEntity] = []
    for i in range(8, sheet.nrows - 2):
        row = sheet.row(i)

        if row[14].value == "-" or row[14].value == "" or not len(row[1].value) == 11:
            continue

        res.append(
            TradeReportEntity(
                exchange_product_id=row[1].value,
                exchange_product_name=row[2].value,
                delivery_basis_name=row[3].value,
                volume=int(row[4].value),
                total=Decimal(row[5].value),
                count=int(row[14].value),
                date=trading_date,
            )
        )
    return res
