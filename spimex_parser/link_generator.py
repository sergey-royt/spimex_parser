from datetime import date, timedelta


URL_SSCHEMA = "https://spimex.com/upload/reports/oil_xls/oil_xls_{}162000.xls"
DOWNLOAD_START_DATE = date(2023, 1, 1)
DOWNLOAD_END_DATE = date.today()


def generate_report_urls() -> list[str]:
    """
    Generate report urls
    """

    current_date = DOWNLOAD_START_DATE
    result = []

    while current_date <= DOWNLOAD_END_DATE:
        str_date = current_date.strftime("%Y%m%d")
        result.append(URL_SSCHEMA.format(str_date))
        current_date += timedelta(days=1)

    return result
