__version__ = '0.5.1.dev0'

from json import loads as _loads
from functools import partial as _partial
from datetime import datetime as _datetime

from jdatetime import datetime as _jdatetime
from aiohttp import ClientSession as _ClientSession, \
    ClientTimeout as _ClientTimeout
from pandas import DataFrame as _DataFrame, to_numeric as _to_num, \
    NaT as _NaT, NA as _NA


_YK = ''.maketrans('يك', 'یک')
_DF = _partial(_DataFrame, copy=False)


SESSION : _ClientSession | None = None


class Session(_ClientSession):

    def __init__(self, **kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = _ClientTimeout(
                total=60, sock_connect=10, sock_read=10)
        super().__init__(**kwargs)

    async def __aenter__(self) -> _ClientSession:
        global SESSION
        SESSION = await super().__aenter__()
        return SESSION


async def _session_get(url: str) -> bytes:
    return await (await SESSION.get(url)).read()


async def _api_json(path) -> list | dict:
    content = await _session_get('https://api.iranetf.org/' + path)
    return _loads(content.decode().translate(_YK))


def _j2g(s: str) -> _datetime:
    try:
        return _jdatetime(*[int(i) for i in s.split('/')]).togregorian()
    except AttributeError:
        assert not s
        return _NaT


async def funds() -> _DataFrame:
    j = (await _api_json('odata/company/GetFunds'))['value']
    df = _DF(j)
    df['StartDate'] = df['StartDate'].apply(_j2g)
    df = df.astype({
        'Url': 'string',
        'NameDisplay': 'string',
        'Labels': 'string',
        'UpdateDate': 'datetime64',
        'CreateDate': 'datetime64',
        'TsetmcId': 'int64',
    }, copy=False)
    df['NameDisplay'] = df['NameDisplay'].str.strip()
    return df


async def fund_portfolio_report_latest(id_: int) -> _DataFrame:
    j = (await _api_json(
        'odata/FundPortfolioReport'
        f'?$top=1'
        f'&$orderby=FromDate desc'
        f'&$filter=CompanyId eq {id_}&$expand=trades'))['value']
    df = _DF(j[0]['Trades'])
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
    df = _DF(j)
    numeric_cols = [
        'tradePrice', 'priceDiff', 'nav', 'navDiff', 'priceAndNavDiff']
    df[numeric_cols] = df[numeric_cols].apply(_to_num, downcast='unsigned')
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
    df['TsetmcId'] = df['TsetmcId'].replace('', _NA).astype('Int64', copy=False)
    return df
