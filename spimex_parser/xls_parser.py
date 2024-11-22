from datetime import datetime, date
from io import BytesIO

import pandas as pd


DATAFRAME_START_IDX = 0

DATAFRAME_INT_INDEX_IDX = 0

DATE_LINE_ROW_INDEX = 2
DATE_LINE_COLUMN_INDEX = 1
DATE_LINE_DATE_INDEX = 1

TABLE_DESCRIPTION_COLUMN_INDEX = 1
TABLE_TO_PARSE_DESCRIPTION_VALUE = "Единица измерения: Метрическая тонна"
OFFSET_BTWN_DSCRPTN_AND_DATA = 1

EXCHANGE_PRODUCT_ID_LEN = 11

EXCHANGE_PRODUCT_ID_IDX = 1
EXCHANGE_PRODUCT_NAME_IDX = 2
DELIVERY_BASIS_NAME_IDX = 3
VOLUME_IDX = 4
TOTAL_IDX = 5
COUNT_IDX = -1

OIL_ID_END_IDX = 4
DELIVERY_BASIS_ID_END_IDX = 7
DELIVERY_TYPE_ID_IDX = -1


def parse_xls(file: BytesIO) -> pd.DataFrame:
    """
    Parse xls file and return data frame prepared
    for adding to database
    """

    df = pd.read_excel(file)
    trading_date = parse_date(df)
    table_beginning_idx = parse_table_beginning_idx(df)

    df = df.iloc[
        table_beginning_idx:,
        [
            EXCHANGE_PRODUCT_ID_IDX,
            EXCHANGE_PRODUCT_NAME_IDX,
            DELIVERY_BASIS_NAME_IDX,
            VOLUME_IDX,
            TOTAL_IDX,
            COUNT_IDX,
        ],
    ].reset_index(drop=True)
    df.columns = pd.Index(df.iloc[DATAFRAME_START_IDX].str.replace("\n", " "))
    df = df.drop([DATAFRAME_START_IDX, DATAFRAME_START_IDX + 1])
    df = df[df.iloc[:, COUNT_IDX] != "-"].dropna().reset_index(drop=True)

    df = df.rename(
        columns={
            "Код Инструмента": "exchange_product_id",
            "Наименование Инструмента": "exchange_product_name",
            "Базис поставки": "delivery_basis_name",
            "Объем Договоров в единицах измерения": "volume",
            "Обьем Договоров, руб.": "total",
            "Количество Договоров, шт.": "count",
        }
    )
    df["date"] = trading_date
    for col in ["volume", "total", "count"]:
        df[col] = df[col].fillna(0).astype(int)
    df["oil_id"] = df["exchange_product_id"].apply(
        lambda x: x[:OIL_ID_END_IDX]
    )
    df["delivery_basis_id"] = df["exchange_product_id"].apply(
        lambda x: x[OIL_ID_END_IDX:DELIVERY_BASIS_ID_END_IDX]
    )
    df["delivery_type_id"] = df["exchange_product_id"].apply(
        lambda x: x[DELIVERY_TYPE_ID_IDX]
    )
    return df


def parse_date(df: pd.DataFrame) -> date:
    """
    Retrieve trading date value from report data frame
    """

    date_line = df.iloc[DATE_LINE_ROW_INDEX, DATE_LINE_COLUMN_INDEX]
    date_part = date_line.split(": ")[DATE_LINE_DATE_INDEX]
    trading_date = datetime.strptime(date_part, "%d.%m.%Y").date()
    return trading_date


def parse_table_beginning_idx(df: pd.DataFrame) -> int:
    """
    Find row where proper table starts
    """

    return (
        df[
            df.iloc[:, TABLE_DESCRIPTION_COLUMN_INDEX]
            == TABLE_TO_PARSE_DESCRIPTION_VALUE
        ].index[DATAFRAME_INT_INDEX_IDX]
        + OFFSET_BTWN_DSCRPTN_AND_DATA
    )
