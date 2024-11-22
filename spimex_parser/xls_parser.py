from datetime import datetime, date
from io import BytesIO

import pandas as pd

from model import TradeReportEntity


class SpimexXlsParser:
    DATAFRAME_INT_INDEX_IDX = 0

    DATE_LINE_ROW_INDEX = 2
    DATE_LINE_COLUMN_INDEX = 1
    DATE_LINE_DATE_INDEX = 1

    TABLE_DESCRIPTION_COLUMN_INDEX = 1
    TABLE_TO_PARSE_DESCRIPTION_VALUE = "Единица измерения: Метрическая тонна"
    OFFSET_BTWN_DSCRPTN_AND_DATA = 3

    EXCHANGE_PRODUCT_ID_LEN = 11

    EXCHANGE_PRODUCT_ID_IDX = 1
    EXCHANGE_PRODUCT_NAME_IDX = 2
    DELIVERY_BASIS_NAME_IDX = 3
    VOLUME_IDX = 4
    TOTAL_IDX = 5
    COUNT_IDX = 14

    def __init__(self, file: BytesIO):
        self.df = pd.read_excel(file)

    def parse(self) -> list[TradeReportEntity]:
        trading_date = self._parse_date()
        table_beginning_idx = self._parse_table_beginning_idx()

        result = []

        for row_idx in range(table_beginning_idx, self.df.shape[0]):
            row = self.df.iloc[row_idx, :]
            if (
                row.iloc[self.COUNT_IDX] == "-"
                or row.iloc[self.COUNT_IDX] == ""
                or not len(row.iloc[self.EXCHANGE_PRODUCT_ID_IDX])
                == self.EXCHANGE_PRODUCT_ID_LEN
            ):
                continue

            result.append(
                TradeReportEntity(
                    exchange_product_id=row.iloc[self.EXCHANGE_PRODUCT_ID_IDX],
                    exchange_product_name=row.iloc[
                        self.EXCHANGE_PRODUCT_NAME_IDX
                    ],
                    delivery_basis_name=row.iloc[self.DELIVERY_BASIS_NAME_IDX],
                    volume=int(row.iloc[self.VOLUME_IDX]),
                    total=int(row.iloc[self.TOTAL_IDX]),
                    count=int(row.iloc[self.COUNT_IDX]),
                    date=trading_date,
                )
            )
        return result

    def _parse_date(self) -> date:
        """
        Retrieve trading date value from report
        """

        date_line = self.df.iloc[
            self.DATE_LINE_ROW_INDEX, self.DATE_LINE_COLUMN_INDEX
        ]
        date_part = date_line.split(": ")[self.DATE_LINE_DATE_INDEX]
        trading_date = datetime.strptime(date_part, "%d.%m.%Y").date()
        return trading_date

    def _parse_table_beginning_idx(self) -> int:
        """
        Find row where proper table starts
        """
        return (
            self.df[
                self.df.iloc[:, self.TABLE_DESCRIPTION_COLUMN_INDEX]
                == self.TABLE_TO_PARSE_DESCRIPTION_VALUE
            ].index[self.DATAFRAME_INT_INDEX_IDX]
            + self.OFFSET_BTWN_DSCRPTN_AND_DATA
        )
