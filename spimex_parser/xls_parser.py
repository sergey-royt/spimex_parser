from datetime import datetime
from io import BytesIO
import xlrd
from model import TradeReportEntity


async def parse(file: BytesIO) -> list[TradeReportEntity]:
    """
    Find non-empty rows in trade report XLS file and create
    TradeReportEntities from them.
    """

    workbook = xlrd.open_workbook(file_contents=file.getvalue())

    # parse trading date
    sheet = workbook.sheet_by_index(0)
    date_line = sheet.row_values(3)[1]
    date_part = date_line.split(": ")[1]
    trading_date = datetime.strptime(date_part, "%d.%m.%Y").date()

    # searching beginning of proper table
    n = 5
    while n < sheet.nrows:
        row = sheet.row_values(n)
        if row[1] == "Единица измерения: Метрическая тонна":
            break
        n += 1

    res: list[TradeReportEntity] = []
    for i in range(n, sheet.nrows - 2):
        row = sheet.row_values(i)
        if row[14] == "-" or row[14] == "" or not len(row[1]) == 11:
            continue

        res.append(
            TradeReportEntity(
                exchange_product_id=row[1],
                exchange_product_name=row[2],
                delivery_basis_name=row[3],
                volume=int(row[4]),
                total=int(row[5]),
                count=int(row[14]),
                date=trading_date,
            )
        )
    return res
