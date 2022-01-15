__version__ = '0.3.0'

from json import loads as _loads
from functools import partial as _partial

from requests import get as _get
from pandas import DataFrame as _DataFrame, to_datetime as _to_dt, \
    to_numeric as _to_num


_YK = ''.maketrans('يك', 'یک')
_DF = _partial(_DataFrame, copy=False)


def _api_json(path) -> list | dict:
    return _loads(
        _get('https://api.iranetf.org/' + path).content.decode().translate(_YK)
    )


def funds() -> _DataFrame:
    j = _api_json('odata/company/GetFunds')['value']
    df = _DF(j)
    df[['UpdateDate', 'CreateDate']] = df[['UpdateDate', 'CreateDate']].apply(_to_dt)
    df['NameDisplay'] = df['NameDisplay'].astype('string', copy=False).str.strip()
    return df


def fund_portfolio_report_latest(id_: int) -> _DataFrame:
    j = _api_json(
        'odata/FundPortfolioReport'
        f'?$top=1'
        f'&$orderby=FromDate desc'
        f'&$filter=CompanyId eq {id_}&$expand=trades')['value']
    df = _DF(j[0]['Trades'])
    return df


def funds_deviation_week_month(
    set_index='companyId'
) -> tuple[_DataFrame, _DataFrame]:
    j = _api_json('bot/funds/fundPriceAndNavDeviation')
    week = _DF(j['seven'])
    month = _DF(j['thirty'])
    if set_index:
        week.set_index(set_index, inplace=True)
        month.set_index(set_index, inplace=True)
    return week, month


def funds_trade_price(set_index='companyId') -> _DataFrame:
    j = _api_json('bot/funds/allFundLastStatus/tradePrice')
    df = _DF(j)
    numeric_cols = [
        'tradePrice', 'priceDiff', 'nav', 'navDiff', 'priceAndNavDiff']
    df[numeric_cols] = df[numeric_cols].apply(_to_num, downcast='unsigned')
    if set_index:
        df.set_index(set_index, inplace=True)
    return df


def fund_trade_info(id_: int | str, month: int) -> _DataFrame:
    j = _api_json(
        'odata/stockTradeInfo/'
        f'GetCompanyStockTradeInfo(companyId={id_},month={month})')
    df = _DF(j['value'])
    df['Date'] = _to_dt(df['Date'])
    return df


def companies() -> _DataFrame:
    return _DF(_api_json('odata/company')['value'])
