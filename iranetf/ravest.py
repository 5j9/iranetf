from json import loads as _loads
from functools import partial as _partial

from pandas import DataFrame as _DataFrame, NaT as _NaT, NA as _NA

from iranetf import _read, _j2g


_DF = _partial(_DataFrame, copy=False)
_YK = ''.maketrans('يك', 'یک')

_FUNDS_DTYPE = {
    'Url': 'string',
    'NameDisplay': 'string',
    'Labels': 'string',
    'UpdateDate': 'datetime64',
    'CreateDate': 'datetime64',
    'TsetmcId': 'int64',
}


async def _api_json(path) -> list | dict:
    content = await _read('https://api.ravest.ir/' + path)
    return _loads(content.decode().translate(_YK))


async def funds() -> _DataFrame:
    j = (await _api_json('odata/company/GetFunds'))['value']
    df = _DF(j)
    start_date = df['StartDate'].replace('', _NaT)
    df['StartDate'] = start_date.map(_j2g, na_action='ignore')
    df = df.astype(_FUNDS_DTYPE, copy=False)
    df['NameDisplay'] = df['NameDisplay'].str.strip()
    return df


async def fund_portfolio_report_latest(id_: int) -> _DataFrame:
    j = (await _api_json(
        'odata/FundPortfolioReport'
        f'?$top=1'
        f'&$orderby=FromDate desc'
        f'&$filter=CompanyId eq {id_}&$expand=trades'))['value']
    df = _DataFrame(j[0]['Trades'], copy=False)
    df['CompanyId'] = df['CompanyId'].astype('Int64')
    return df


async def funds_deviation_week_month(
    set_index='companyId'
) -> tuple[_DataFrame, _DataFrame]:
    j = await _api_json('bot/funds/fundPriceAndNavDeviation')
    week = _DF(j['seven'])
    month = _DF(j['thirty'])
    if set_index:
        week.set_index(set_index, inplace=True)
        month.set_index(set_index, inplace=True)
    return week, month


async def funds_trade_price(set_index='companyId') -> _DataFrame:
    j = await _api_json('bot/funds/allFundLastStatus/tradePrice')
    df = _DataFrame(j, copy=False)
    df = df.astype({
        'fundType': 'category',
        'symbol': 'string',
        'tradePrice': 'float64',
        'priceDiff': 'float64',
        'nav': 'float64',
        'navDiff': 'float64',
        'priceAndNavDiff': 'float64',
    }, copy=False)
    if set_index:
        df.set_index(set_index, inplace=True)
    return df


async def fund_trade_info(id_: int | str, month: int) -> _DataFrame:
    j = await _api_json(
        'odata/stockTradeInfo/'
        f'GetCompanyStockTradeInfo(companyId={id_},month={month})')
    df = _DF(j['value'])
    df = df.astype({
        'Date': 'datetime64',
        'TsetmcId': 'Int64',
    }, copy=False)
    return df


async def companies() -> _DataFrame:
    df = _DataFrame((await _api_json('odata/company'))['value'], copy=False)
    df = df.astype({
        'Labels': 'string',
    }, copy=False)
    df['StartDate'] = df['StartDate'].replace('', _NaT).map(_j2g, na_action='ignore')
    df['TsetmcId'] = df['TsetmcId'].replace('', _NA).astype('Int64', copy=False)
    return df
