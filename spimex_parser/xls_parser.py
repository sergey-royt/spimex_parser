from datetime import datetime, date
from io import BytesIO

import xlrd

from model import TradeReportEntity


class SpimexXlsParser:

    REPORT_SHEET_INDEX = 0

    DATE_LINE_ROW_INDEX = 3
    DATE_LINE_COLUMN_INDEX = 1
    DATE_LINE_DATE_INDEX = 1

    REPORT_TABLE_START_INDEX = 5

    TABLE_DESCRIPTION_COLUMN_INDEX = 1
    TABLE_TO_PARSE_DESCRIPTION_VALUE = "Единица измерения: Метрическая тонна"

    EXCHANGE_PRODUCT_ID_LEN = 11

    EXCHANGE_PRODUCT_ID_IDX = 1
    EXCHANGE_PRODUCT_NAME_IDX = 2
    DELIVERY_BASIS_NAME_IDX = 3
    VOLUME_IDX = 4
    TOTAL_IDX = 5
    COUNT_IDX = 14

    def __init__(self, file: BytesIO):
        self.sheet = xlrd.open_workbook(
            file_contents=file.getvalue()
        ).sheet_by_index(self.REPORT_SHEET_INDEX)

    async def parse(self) -> list[TradeReportEntity]:
        # parse trading date
        trading_date = await self._parse_date()

        # parse table beginning index
        beg_idx = await self._parse_table_beginning_idx()

        result: list[TradeReportEntity] = []

        for i in range(beg_idx, self.sheet.nrows):
            row = self.sheet.row_values(i)
            if (
                row[self.COUNT_IDX] == "-"
                or row[self.COUNT_IDX] == ""
                or not len(row[self.EXCHANGE_PRODUCT_ID_IDX])
                == self.EXCHANGE_PRODUCT_ID_LEN
            ):
                continue

            result.append(
                TradeReportEntity(
                    exchange_product_id=row[self.EXCHANGE_PRODUCT_ID_IDX],
                    exchange_product_name=row[self.EXCHANGE_PRODUCT_NAME_IDX],
                    delivery_basis_name=row[self.DELIVERY_BASIS_NAME_IDX],
                    volume=int(row[self.VOLUME_IDX]),
                    total=int(row[self.TOTAL_IDX]),
                    count=int(row[self.COUNT_IDX]),
                    date=trading_date,
                )
            )
        return result

    async def _parse_date(self) -> date:
        """
        Retrieve trading date value from report
        """

        date_line = self.sheet.row_values(self.DATE_LINE_ROW_INDEX)[
            self.DATE_LINE_COLUMN_INDEX
        ]
        date_part = date_line.split(": ")[self.DATE_LINE_DATE_INDEX]
        trading_date = datetime.strptime(date_part, "%d.%m.%Y").date()
        return trading_date

    async def _parse_table_beginning_idx(self) -> int:
        """
        Find row where proper table starts
        """

        n = self.REPORT_TABLE_START_INDEX
        while n < self.sheet.nrows:
            row = self.sheet.row_values(n)
            if (
                row[self.TABLE_DESCRIPTION_COLUMN_INDEX]
                == self.TABLE_TO_PARSE_DESCRIPTION_VALUE
            ):
                break
            n += 1
        return n
