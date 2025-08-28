__version__ = '0.29.0'
import logging as _logging
from abc import ABC as _ABC, abstractmethod as _abstractmethod
from asyncio import gather as _gather
from datetime import date as _date, datetime as _datetime
from io import StringIO as _StringIO
from json import JSONDecodeError as _JSONDecodeError, loads as _loads
from logging import (
    debug as _debug,
    error as _error,
    info as _info,
    warning as _warning,
)
from pathlib import Path as _Path
from re import (
    findall as _findall,
    search as _search,
    split as _split,
)
from typing import Any as _Any, TypedDict as _TypedDict

import pandas as _pd
from aiohttp import (
    ClientConnectorError as _ClientConnectorError,
    ClientOSError as _ClientOSError,
    ClientResponse as _ClientResponse,
    ClientResponseError as _ClientResponseError,
    ServerDisconnectedError as _ServerDisconnectedError,
    ServerTimeoutError as _ServerTimeoutError,
    TooManyRedirects as _TooManyRedirects,
)
from aiohutils.session import SessionManager
from jdatetime import datetime as _jdatetime
from pandas import (
    DataFrame as _DataFrame,
    Series as _Series,
    concat as _concat,
    read_csv as _read_csv,
    to_datetime as _to_datetime,
    to_numeric as _to_numeric,
)
from tsetmc.instruments import (
    Instrument as _Instrument,
    search as _tsetmc_search,
)

_pd.options.mode.copy_on_write = True
_pd.options.future.infer_string = True  # type: ignore
_pd.options.future.no_silent_downcasting = True  # type: ignore

session_manager = SessionManager()


ssl: bool = False  # as horrible as this is, many sites fail ssl verification


async def _get(
    url: str, params: dict | None = None, cookies: dict | None = None
) -> _ClientResponse:
    return await session_manager.get(
        url, ssl=ssl, cookies=cookies, params=params
    )


async def _read(url: str) -> bytes:
    return await (await _get(url)).read()


def _j2g(s: str) -> _datetime:
    return _jdatetime(*[int(i) for i in s.split('/')]).togregorian()


_fa_to_en_tt = str.maketrans(
    {
        '۰': '0',
        '۱': '1',
        '۲': '2',
        '۳': '3',
        '۴': '4',
        '۵': '5',
        '۶': '6',
        '۷': '7',
        '۸': '8',
        '۹': '9',
        ',': '',
    }
)


def _fanum_to_num(series: _Series):
    """
    Converts a pandas Series from strings containing Persian digits and commas
    to a numeric data type.
    """
    return _to_numeric(series.str.translate(_fa_to_en_tt))  # type: ignore


_ETF_TYPES = {  # numbers are according to fipiran
    6: 'Stock',
    4: 'Fixed',
    7: 'Mixed',
    5: 'Commodity',
    17: 'FOF',
    18: 'REIT',
    21: 'Sector',
    22: 'Leveraged',
    23: 'Index',
    24: 'Guarantee',
}


class LiveNAVPS(_TypedDict):
    creation: int
    redemption: int
    date: _datetime


class TPLiveNAVPS(LiveNAVPS):
    dailyTotalNetAssetValue: int
    dailyTotalUnit: int
    finalCancelNAV: int
    finalEsmiNAV: int
    finalSubscriptionNAV: int
    maxUnit: str
    navDate: str
    nominal: int
    totalNetAssetValue: int
    totalUnit: int


type AnySite = 'LeveragedTadbirPardaz | TadbirPardaz | RayanHamafza | MabnaDP | LeveragedMabnaDP'


class BaseSite(_ABC):
    __slots__ = '_home_info_cache', 'last_response', 'url'

    ds: _DataFrame
    _aa_keys: set

    def __init__(self, url: str):
        assert url[-1] == '/', 'the url must end with `/`'
        self.url = url

    def __repr__(self):
        return f"{type(self).__name__}('{self.url}')"

    def __eq__(self, value):
        if not isinstance(value, BaseSite):
            return NotImplemented
        if value.url == self.url and type(value) is type(self):
            return True
        return False

    async def _json(
        self,
        path: str,
        *,
        params: dict | None = None,
        cookies: dict | None = None,
        df: bool = False,
    ) -> _Any:
        r = await _get(self.url + path, params, cookies)
        self.last_response = r
        content = await r.read()
        j = _loads(content)
        if df is True:
            return _DataFrame(j, copy=False)
        return j

    @_abstractmethod
    async def live_navps(self) -> LiveNAVPS: ...

    @_abstractmethod
    async def navps_history(self) -> _DataFrame: ...

    @_abstractmethod
    async def cache(self) -> float: ...

    @classmethod
    def from_l18(cls, l18: str) -> AnySite:
        try:
            ds = cls.ds
        except AttributeError:
            ds = cls.ds = load_dataset(site=True).set_index('l18')
        return ds.loc[l18, 'site']  # type: ignore

    def _check_aa_keys(self, d: dict):
        if d.keys() <= self._aa_keys:
            return
        _warning(
            f'Unknown asset allocation keys on {self!r}: {d.keys() - self._aa_keys}'
        )

    @staticmethod
    async def from_url(url: str) -> AnySite:
        content = await _read(url)
        rfind = content.rfind

        if rfind(b'<div class="tadbirLogo"></div>') != -1:
            tp_site = TadbirPardaz(url)
            info = await tp_site.home_info()
            if info['isLeveragedMode']:
                return LeveragedTadbirPardaz(url)
            if info['isETFMultiNavMode']:
                return TadbirPardazMultiNAV(url + '#2')
            return tp_site

        if rfind(b'Rayan Ham Afza') != -1:
            return RayanHamafza(url)

        if rfind(b'://mabnadp.com') != -1:
            if rfind(rb'\"fundType\":\"leverage\"') != -1:
                assert (
                    rfind(
                        rb'\"isMultiNav\":false,\"isSingleNav\":true,\"isEtf\":true'
                    )
                    != -1
                ), 'Uknown MabnaDP site type.'
                return LeveragedMabnaDP(url)
            return MabnaDP(url)

        raise ValueError(f'Could not determine site type for {url}.')

    async def leverage(self) -> float:
        return 1.0 - await self.cache()

    async def _home(self) -> str:
        return (await _read(self.url)).decode()

    @_abstractmethod
    async def _home_info(self) -> dict[str, _Any]: ...

    async def home_info(self) -> dict[str, _Any]:
        try:
            return self._home_info_cache
        except AttributeError:
            i = self._home_info_cache = await self._home_info()
            return i

    async def reg_no(self) -> str:
        return (await self.home_info())['seo_reg_no']


def _comma_int(s: str) -> int:
    return int(s.replace(',', ''))


def _comma_float(s: str) -> float:
    return float(s.replace(',', ''))


class MabnaDPBase(BaseSite):
    async def _home_info(self):
        d = {}
        html = await self._home()
        m = _search(r'(\d+)\s*نزد سازمان بورس', html)
        if m:
            d['seo_reg_no'] = m[1]
        return d


class MabnaDP(MabnaDPBase):
    async def _json(self, path, **kwa) -> _Any:
        return await super()._json(f'api/v1/overall/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        j: dict = await self._json('navetf.json')
        j['date'] = _jdatetime.strptime(
            j['date_time'], '%H:%M %Y/%m/%d'
        ).togregorian()
        j['creation'] = _comma_int(j.pop('purchase_price'))
        j['redemption'] = _comma_int(j.pop('redemption_price'))
        return j  # type: ignore

    async def navps_history(self) -> _DataFrame:
        j: list[dict] = await self._json('navps.json')
        df = _DataFrame(j[0]['values'])
        df['date'] = (
            df['date']
            .astype(str)
            .apply(
                lambda i: _jdatetime.strptime(
                    i, format='%Y%m%d000000'
                ).togregorian()
            )
        )
        df['creation'] = df.pop('purchase_price')
        df['redemption'] = df.pop('redeption_price')
        df['statistical'] = df.pop('statistical_value')
        df.set_index('date', inplace=True)
        return df

    async def version(self) -> str:
        content = await _read(self.url)
        start = content.find('نگارش '.encode())
        if start == -1:
            start = content.find('نسخه '.encode())
            if start == -1:
                raise ValueError('version was not found')
            start += 9
        else:
            start += 11

        end = content.find(b'<', start)
        return content[start:end].strip().decode()

    _aa_keys = {'سهام', 'سایر دارایی ها', 'وجه نقد', 'سایر', 'سپرده بانکی'}

    async def asset_allocation(self) -> dict:
        j: dict = await self._json(
            'dailyvalue.json', params={'portfolioIds': '0'}
        )
        d = {i['name']: i['percentage'] for i in j['values']}
        self._check_aa_keys(d)
        return d

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        g = aa.get
        return g('وجه نقد', 0.0) + g('سپرده بانکی', 0.0)


class LeveragedMabnaDP(MabnaDPBase):
    async def _json(self, path, **kwa) -> _Any:
        params: dict | None = kwa.get('params')
        if params is None:
            kwa['params'] = {'portfolio_id': '1'}
        else:
            params.setdefalt('portfolio_id', '1')

        return await super()._json(f'api/v2/public/fund/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        data = (await self._json('etf/navps/latest'))['data']
        data['date'] = _datetime.fromisoformat(data.pop('date_time')).replace(
            tzinfo=None
        )
        data['creation'] = data.pop('purchase_price')
        data['redemption'] = data.pop('redemption_price')
        return data

    async def navps_history(self) -> _DataFrame:
        data: list[dict] = (await self._json('chart'))['data']
        df = _DataFrame(data)
        df.rename(
            columns={
                'redemption_price': 'redemption',
                'statistical_value': 'statistical',
                'purchase_price': 'creation',
            },
            inplace=True,
        )
        df['date_time'] = df['date_time'].astype('datetime64[ns, UTC+03:30]')  # type: ignore
        df.set_index(
            df['date_time'].dt.normalize().dt.tz_localize(None), inplace=True
        )
        df.index.name = 'date'
        return df

    _aa_keys = {
        'اوراق',
        'سهام',
        'سایر دارایی ها',
        'سایر دارایی\u200cها',
        'وجه نقد',
        'سایر',
        'سایر سهام',
        'پنج سهم با بیشترین وزن',
        'سپرده بانکی',
    }

    async def asset_allocation(self) -> dict:
        assets: list[dict] = (await self._json('assets-classification'))[
            'data'
        ]['assets']
        d = {i['title']: i['percentage'] / 100 for i in assets}
        self._check_aa_keys(d)
        return d

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        g = aa.get
        return sum(g(k, 0.0) for k in ('اوراق', 'وجه نقد', 'سپرده بانکی'))

    async def home_data(self) -> dict:
        html = await (await _get(self.url)).text()
        return {
            '__REACT_QUERY_STATE__': _loads(
                _loads(
                    html.rpartition('window.__REACT_QUERY_STATE__ = ')[
                        2
                    ].partition(';')[0]
                )
            ),
            '__REACT_REDUX_STATE__': _loads(
                _loads(
                    html.rpartition('window.__REACT_REDUX_STATE__ = ')[
                        2
                    ].partition(';')[0]
                )
            ),
            '__ENV__': _loads(
                _loads(
                    html.rpartition('window.__ENV__ = ')[2].partition('\n')[0]
                )
            ),
        }

    async def leverage(self) -> float:
        data, cache = await _gather(self.home_data(), self.cache())
        data = data['__REACT_QUERY_STATE__']['queries'][9]['state']['data'][
            '1'
        ]
        return (
            1.0
            + data['commonUnitRedemptionValueAmount']
            / data['preferredUnitRedemptionValueAmount']
        ) * (1.0 - cache)


class _RHNavLight(_TypedDict):
    NextTimeInterval: int
    FundId: int
    FundNavId: int
    PurchaseNav: int
    SaleNav: int
    Date: str
    Time: str


class RayanHamafza(BaseSite):
    _api_path = 'api/data'
    __slots__ = 'fund_id'

    async def _home_info(self) -> dict[str, _Any]:
        html = await self._home()
        d = {}
        reg_no_match = _search(r'ثبت شده به شماره (\d+) نزد سازمان بورس', html)
        if reg_no_match:
            d['seo_reg_no'] = reg_no_match[1]
        return d

    def __init__(self, url: str):
        url, _, fund_id = url.partition('#')
        self.fund_id = fund_id or '1'
        super().__init__(url)

    async def _json(self, path, **kwa) -> _Any:
        return await super()._json(f'{self._api_path}/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        d: _RHNavLight = await self._json(f'NavLight/{self.fund_id}')
        return {
            'creation': d['PurchaseNav'],
            'redemption': d['SaleNav'],
            'date': _jdatetime.strptime(
                f'{d["Date"]} {d["Time"]}', '%Y/%m/%d %H:%M:%S'
            ).togregorian(),
        }

    async def navps_history(self) -> _DataFrame:
        df: _DataFrame = await self._json(
            f'NavPerShare/{self.fund_id}', df=True
        )
        df.columns = ['date', 'creation', 'redemption', 'statistical']
        df['date'] = df['date'].map(_j2g)
        df.set_index('date', inplace=True)
        return df

    _nav_history_path = 'DailyNAVChart/1'

    async def nav_history(self) -> _DataFrame:
        df: _DataFrame = await self._json(self._nav_history_path, df=True)
        df.columns = ['nav', 'date', 'creation_navps']
        df['date'] = df['date'].map(_j2g)
        return df

    _portfolio_industries_path = 'Industries/1'

    async def portfolio_industries(self) -> _DataFrame:
        return await self._json(self._portfolio_industries_path, df=True)

    _aa_keys = {
        'DepositTodayPercent',
        'TopFiveStockTodayPercent',
        'CashTodayPercent',
        'OtherAssetTodayPercent',
        'BondTodayPercent',
        'OtherStock',
        'JalaliDate',
    }

    _asset_allocation_path = 'MixAsset/1'

    async def asset_allocation(self) -> dict:
        d: dict = await self._json(self._asset_allocation_path)
        self._check_aa_keys(d)
        return {k: v / 100 if type(v) is not str else v for k, v in d.items()}

    async def dividend_history(self) -> _DataFrame:
        j: dict = await self._json('Profit/1')
        df = _DataFrame(j['data'])
        df['ProfitDate'] = df['ProfitDate'].apply(
            lambda i: _jdatetime.strptime(i, format='%Y/%m/%d').togregorian()
        )
        return df

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        return (
            aa['DepositTodayPercent']
            + aa['CashTodayPercent']
            + aa['BondTodayPercent']
        )


_jp = _jdatetime.strptime


def _jymd_to_greg(date_string, /):
    return _jp(date_string, format='%Y/%m/%d').togregorian()


class BaseTadbirPardaz(BaseSite):
    async def version(self) -> str:
        return (await self.home_info())['version']

    _aa_keys = {
        'اوراق گواهی سپرده',
        'اوراق مشارکت',
        'پنج سهم برتر',
        'سایر دارایی\u200cها',
        'سایر سهام',
        'سایر سهم\u200cها',
        'سهم\u200cهای برتر',
        'شمش و طلا',
        'صندوق سرمایه\u200cگذاری در سهام',
        'صندوق های سرمایه گذاری',
        'نقد و بانک (جاری)',
        'نقد و بانک (سپرده)',
        'گواهی سپرده کالایی',
        'اختیار معامله',
    }

    async def asset_allocation(self) -> dict:
        j: dict = await self._json('Chart/AssetCompositions')
        d = {i['x']: i['y'] / 100 for i in j['List']}
        self._check_aa_keys(d)
        return d

    async def _home_info(self) -> dict[str, _Any]:
        html = await self._home()
        d: dict[str, _Any] = {
            'isETFMultiNavMode': _search(
                r'isETFMultiNavMode\s*=\s*true;', html
            )
            is not None,
            'isLeveragedMode': _search(r'isLeveragedMode\s*=\s*true;', html)
            is not None,
            'isEtfMode': _search(r'isEtfMode\s*=\s*true;', html) is not None,
        }
        if d['isETFMultiNavMode']:
            baskets = _findall(
                r'<option [^>]*?value="(\d+)">([^<]*)</option>',
                html.partition('<div class="drp-basket-header">')[2].partition(
                    '</select>'
                )[0],
            )
            d['basketIDs'] = dict(baskets)

        start = html.find('version number:')
        end = html.find('\n', start)
        d['version'] = html[start + 15 : end].strip()

        reg_no_match = _search(
            r'<td>شماره ثبت نزد سازمان بورس و اوراق بهادار</td>\s*'
            '<td style="text-align:left">(.*?)</td>',
            html,
        )
        if reg_no_match:
            d['seo_reg_no'] = str(int(reg_no_match[1]))

        return d

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        g = aa.get
        return (
            g('نقد و بانک (سپرده)', 0.0)
            + g('نقد و بانک (جاری)', 0.0)
            + g('اوراق مشارکت', 0.0)
        )

    async def nav_history(
        self, *, from_: _date = _date(1970, 1, 1), to: _date, basket_id=0
    ) -> _DataFrame:
        """
        This function uses the HTML output available at /Reports/FundNAVList.
        This is better than the excel output because it includes statistical
        nav column which excel does not have.

        If the output is in multiple pages, this function will fetch them
        all and return the result in a single dataframe.

        Tip: the from_ date can be arbitrary old, e.g. 1900-01-01 but
            there is a 50 year limit on how far in the future the
            `to` can be.
        """
        path = f'Reports/FundNAVList?FromDate={_jdatetime.fromgregorian(date=from_):%Y/%m/%d}&ToDate={_jdatetime.fromgregorian(date=to):%Y/%m/%d}&BasketId={basket_id}&page=1'
        dfs = []
        while True:
            r = await _get(self.url + path)
            html = (await r.read()).decode()
            # the first table contains regNo which can be ignored
            table = _pd.read_html(_StringIO(html))[1]
            # the last row is <tfoot> containing next/previous links
            table.drop(table.index[-1], inplace=True)
            dfs.append(table)
            m = _search('<a href="([^"]*)" title="Next page">»</a>', html)
            if m is None:
                break
            path = m[1]

        df = _concat(dfs, ignore_index=True)
        df.rename(
            columns={
                'ردیف': 'Row',
                'تاریخ': 'Date',
                'قیمت صدور': 'Issue Price',
                'قیمت ابطال': 'Redemption Price',
                'قیمت آماری': 'Statistical Price',
                'NAV صدور واحدهای ممتاز': 'NAV of Premium Units Issued',
                'NAV ابطال واحدهای ممتاز': 'NAV of Premium Units Redeemed',
                'NAV آماری ممتاز': 'Statistical NAV of Premium Units',
                'NAV واحدهای عادی': 'NAV of Normal Units',
                'خالص ارزش صندوق': 'Net Asset Value of Fund',
                'خالص ارزش واحدهای ممتاز': 'Net Asset Value of Premium Units',
                'خالص ارزش واحدهای عادی': 'Net Asset Value of Normal Units',
                'تعداد واحد ممتاز صادر شده': 'Number of Premium Units Issued',
                'تعداد واحد ممتاز باطل شده': 'Number of Premium Units Redeemed',
                'تعداد واحد عادی صادر شده': 'Number of Normal Units Issued',
                'تعداد واحد عادی باطل شده': 'Number of Normal Units Redeemed',
                'مانده گواهی ممتاز': 'Remaining Premium Certificate',
                'مانده گواهی عادی': 'Remaining Normal Certificate',
                'کل واحدهای صندوق': 'Total Fund Units',
                'نسبت اهرمی': 'Leverage Ratio',
                'تعداد سرمایه‌گذاران واحدهای عادی': 'Number of Normal Unit Investors',
                'Unnamed: 21': 'Unnamed_21',
            },
            inplace=True,
        )
        numeric_cols = [
            'Row',
            'Issue Price',
            'Redemption Price',
            'Statistical Price',
            'NAV of Premium Units Issued',
            'NAV of Premium Units Redeemed',
            'Statistical NAV of Premium Units',
            'NAV of Normal Units',
            'Net Asset Value of Fund',
            'Net Asset Value of Premium Units',
            'Net Asset Value of Normal Units',
            'Number of Premium Units Issued',
            'Number of Premium Units Redeemed',
            'Number of Normal Units Issued',
            'Number of Normal Units Redeemed',
            'Remaining Premium Certificate',
            'Remaining Normal Certificate',
            'Total Fund Units',
            'Leverage Ratio',
            'Number of Normal Unit Investors',
        ]
        df[numeric_cols] = df[numeric_cols].apply(_fanum_to_num)
        df['Date'] = df['Date'].map(_jymd_to_greg)
        df.set_index('Date', inplace=True)
        return df


class TadbirPardaz(BaseTadbirPardaz):
    async def live_navps(self) -> TPLiveNAVPS:
        d: str = await self._json('Fund/GetETFNAV')  # type: ignore
        # the json is escaped twice, so it needs to be loaded again
        d: dict = _loads(d)  # type: ignore

        d['creation'] = d.pop('subNav')
        d['redemption'] = d.pop('cancelNav')
        d['nominal'] = d.pop('esmiNav')

        for k, t in TPLiveNAVPS.__annotations__.items():
            if t is int:
                try:
                    d[k] = _comma_int(d[k])
                except KeyError:
                    _warning(f'key {k!r} not found')

        date = d.pop('publishDate')
        try:
            date = _jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = _jdatetime.strptime(date, '%Y/%m/%d ')
        d['date'] = date.togregorian()

        return d  # type: ignore

    async def navps_history(self) -> _DataFrame:
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )
        creation, statistical, redemption = [
            [d['y'] for d in i['List']] for i in j
        ]
        date = [d['x'] for d in j[0]['List']]
        df = _DataFrame(
            {
                'date': date,
                'creation': creation,
                'redemption': redemption,
                'statistical': statistical,
            }
        )
        df['date'] = _to_datetime(df.date)
        df.set_index('date', inplace=True)
        return df

    async def dividend_history(self) -> _DataFrame:
        path = 'Reports/FundDividendProfitReport'
        all_rows = []
        while path:
            html = (await _read(f'{self.url}{path}')).decode()
            table, _, after_table = html.partition('<tbody>')[2].rpartition(
                '</tbody>'
            )
            all_rows += [
                _findall(r'<td>([^<]*)</td>', r)
                for r in _split(r'</tr>\s*<tr>', table)
            ]
            path = after_table.rpartition('" title="Next page">')[
                0
            ].rpartition('<a href="/')[2]
        # try to use the same column names as RayanHamafza.dividend_history
        df = _DataFrame(
            all_rows,
            columns=[
                'row',
                'ProfitDate',
                'FundUnit',
                'UnitProfit',
                'SUMAllProfit',
                'ProfitPercent',
            ],
        )
        df['ProfitDate'] = df['ProfitDate'].apply(_jymd_to_greg)
        comma_cols = ['FundUnit', 'SUMAllProfit']
        df[comma_cols] = df[comma_cols].map(_comma_int)
        int_cols = ['row', 'UnitProfit']
        df[int_cols] = df[int_cols].map(_comma_int)
        df['ProfitPercent'] = df['ProfitPercent'].astype(float)
        return df


class TadbirPardazMultiNAV(TadbirPardaz):
    """Same as TadbirPardaz, only send basketId to request params."""

    __slots__ = 'basket_id'

    def __init__(self, url: str):
        """Note: the url ends with #<basket_id> where basket_id is an int."""
        url, _, self.basket_id = url.partition('#')
        super().__init__(url)

    async def _json(
        self, path: str, params: dict | None = None, **kwa
    ) -> _Any:
        return await super()._json(
            path,
            params=(params or {}) | {'basketId': self.basket_id},
            **kwa,
        )


class LeveragedTadbirPardazLiveNAVPS(LiveNAVPS):
    BaseUnitsCancelNAV: float
    BaseUnitsTotalNetAssetValue: float
    BaseUnitsTotalSubscription: int
    SuperUnitsTotalSubscription: int
    SuperUnitsTotalNetAssetValue: float


class LeveragedTadbirPardaz(BaseTadbirPardaz):
    async def navps_history(self) -> _DataFrame:
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )

        append = (frames := []).append

        for i, name in zip(
            j,
            (
                'normal_creation',
                'normal_statistical',
                'normal_redemption',
                'creation',
                'redemption',
                'normal',
            ),
        ):
            df = _DataFrame.from_records(i['List'], exclude=['name'])
            df['date'] = _to_datetime(df['x'], format='%m/%d/%Y')
            df.drop(columns='x', inplace=True)
            df.rename(columns={'y': name}, inplace=True)
            df.drop_duplicates('date', inplace=True)
            df.set_index('date', inplace=True)
            append(df)

        df = _concat(frames, axis=1)
        return df

    async def live_navps(self) -> LeveragedTadbirPardazLiveNAVPS:
        j: str = await self._json('Fund/GetLeveragedNAV')  # type: ignore
        # the json is escaped twice, so it needs to be loaded again
        j: dict = _loads(j)  # type: ignore

        pop = j.pop
        date = j.pop('PublishDate')

        result = {}

        for k in (
            'BaseUnitsCancelNAV',
            'BaseUnitsTotalNetAssetValue',
            'SuperUnitsTotalNetAssetValue',
        ):
            result[k] = _comma_float(pop(k))

        result['creation'] = _comma_int(pop('SuperUnitsSubscriptionNAV'))
        result['redemption'] = _comma_int(pop('SuperUnitsCancelNAV'))

        for k, v in j.items():
            result[k] = _comma_int(v)

        try:
            date = _jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = _jdatetime.strptime(date, '%Y/%m/%d ')
        result['date'] = date.togregorian()

        return result  # type: ignore

    async def leverage(self) -> float:
        navps, cache = await _gather(self.live_navps(), self.cache())
        return (
            1.0
            + navps['BaseUnitsTotalNetAssetValue']
            / navps['SuperUnitsTotalNetAssetValue']
        ) * (1.0 - cache)


_DATASET_PATH = _Path(__file__).parent / 'dataset.csv'


def _make_site(row) -> BaseSite:
    type_str = row['siteType']
    site_class = globals()[type_str]
    return site_class(row['url'])


def load_dataset(*, site=True, inst=False) -> _DataFrame:
    """Load dataset.csv as a DataFrame.

    If site is True, convert url and siteType columns to site object.
    """
    df = _read_csv(
        _DATASET_PATH,
        encoding='utf-8-sig',
        low_memory=False,
        lineterminator='\n',
        dtype={
            'l18': 'string',
            'name': 'string',
            'type': _pd.CategoricalDtype([*_ETF_TYPES.values()]),
            'insCode': 'string',
            'regNo': 'string',
            'url': 'string',
            'siteType': 'category',
        },
    )

    if site:
        df['site'] = df[df['siteType'].notna()].apply(_make_site, axis=1)  # type: ignore

    if inst:
        df['inst'] = df['insCode'].apply(_Instrument)  # type: ignore

    return df


def save_dataset(ds: _DataFrame):
    ds[
        [  # sort columns
            'l18',
            'name',
            'type',
            'insCode',
            'regNo',
            'url',
            'siteType',
        ]
    ].sort_values('l18').to_csv(
        _DATASET_PATH, lineterminator='\n', encoding='utf-8-sig', index=False
    )


async def _check_validity(site: BaseSite, retry=0) -> tuple[str, str] | None:
    try:
        await site.live_navps()
    except (
        TimeoutError,
        _JSONDecodeError,
        _ClientConnectorError,
        _ServerTimeoutError,
        _ClientOSError,
        _TooManyRedirects,
        _ServerDisconnectedError,
        _ClientResponseError,
    ):
        if retry > 0:
            return await _check_validity(site, retry - 1)
        return None
    last_url = site.last_response.url  # to avoid redirected URLs
    return f'{last_url.scheme}://{last_url.host}/', type(site).__name__


# sorted from most common to least common
SITE_TYPES = (RayanHamafza, TadbirPardaz, LeveragedTadbirPardaz, MabnaDP)


async def _url_type(domain: str) -> tuple:
    coros = [
        _check_validity(SiteType(f'http://{domain}/'), 2)
        for SiteType in SITE_TYPES
    ]
    results = await _gather(*coros)

    for result in results:
        if result is not None:
            return result

    _warning(f'_url_type failed for {domain}')
    return None, None


async def _add_url_and_type(
    fipiran_df: _DataFrame, known_domains: _Series | None
):
    domains_to_be_checked = fipiran_df['domain'][~fipiran_df['domain'].isna()]
    if known_domains is not None:
        domains_to_be_checked = domains_to_be_checked[
            ~domains_to_be_checked.isin(known_domains)
        ]

    _info(f'checking site types of {len(domains_to_be_checked)} domains')
    if domains_to_be_checked.empty:
        return

    # there will be a lot of redirection warnings, let's silent them
    _logging.disable()  # to disable redirection warnings
    list_of_tuples = await _gather(
        *[_url_type(d) for d in domains_to_be_checked]
    )
    _logging.disable(_logging.NOTSET)

    url, site_type = zip(*list_of_tuples)
    fipiran_df.loc[:, ['url', 'siteType']] = _DataFrame(
        {'url': url, 'siteType': site_type}, index=domains_to_be_checked.index
    )


async def _add_ins_code(new_items: _DataFrame) -> None:
    names_without_code = new_items[new_items['insCode'].isna()].name
    if names_without_code.empty:
        return
    _info('searching names on tsetmc to find their insCode')
    results = await _gather(
        *[_tsetmc_search(name) for name in names_without_code]
    )
    ins_codes = [(None if len(r) != 1 else r[0]['insCode']) for r in results]
    new_items.loc[names_without_code.index, 'insCode'] = ins_codes


async def _fipiran_data(ds) -> _DataFrame:
    import fipiran.funds

    _info('await fipiran.funds.funds()')
    fipiran_df = await fipiran.funds.funds()

    reg_not_in_fipiran = ds[~ds['regNo'].isin(fipiran_df['regNo'])]
    if not reg_not_in_fipiran.empty:
        _warning(
            f'Some dataset rows were not found on fipiran:\n{reg_not_in_fipiran}'
        )

    df = fipiran_df[
        (fipiran_df['typeOfInvest'] == 'Negotiable')
        # 11: 'Market Maker', 12: 'VC', 13: 'Project', 14: 'Land and building',
        # 16: 'PE'
        & ~(fipiran_df['fundType'].isin((11, 12, 13, 14, 16)))
        & fipiran_df['isCompleted']
    ]

    df = df[
        [
            'regNo',
            'smallSymbolName',
            'name',
            'fundType',
            'websiteAddress',
            'insCode',
        ]
    ]

    df.rename(
        columns={
            'fundType': 'type',
            'websiteAddress': 'domain',
            'smallSymbolName': 'l18',
        },
        copy=False,
        inplace=True,
        errors='raise',
    )

    df['type'] = df['type'].replace(_ETF_TYPES)

    return df


async def _tsetmc_dataset() -> _DataFrame:
    from tsetmc.dataset import LazyDS, update

    _info('await tsetmc.dataset.update()')
    await update()

    df = LazyDS.df
    df.drop(columns=['l30', 'isin', 'cisin'], inplace=True)
    return df


def _add_new_items_to_ds(new_items: _DataFrame, ds: _DataFrame) -> _DataFrame:
    if new_items.empty:
        return ds
    new_with_code = new_items[new_items['insCode'].notna()]
    if not new_with_code.empty:
        ds = _concat(
            [ds, new_with_code.set_index('insCode').drop(columns=['domain'])]
        )
    else:
        _info('new_with_code is empty!')
    return ds


async def _update_existing_rows_using_fipiran(
    ds: _DataFrame, fipiran_df: _DataFrame, check_existing_sites: bool
) -> _DataFrame:
    """Note: ds index will be set to insCode."""
    await _add_url_and_type(
        fipiran_df,
        known_domains=None
        if check_existing_sites
        else ds['url'].str.extract('//(.*)/')[0],
    )

    # to update existing urls and names
    # NA values in regNo cause error later due to duplication
    regno = ds[~ds['regNo'].isna()].set_index('regNo')
    regno['domain'] = None
    regno.update(fipiran_df.set_index('regNo'))

    ds.set_index('insCode', inplace=True)
    # Do not overwrite MultiNAV type and URL.
    regno.set_index('insCode', inplace=True)
    ds.update(regno, overwrite=False)

    # Update ds types using fipiran values
    # ds['type'] = regno['type'] will create NA values in type column.
    common_indices = regno.index.intersection(ds.index)
    ds.loc[common_indices, 'type'] = regno.loc[common_indices, 'type']

    # use domain as URL for those who do not have any URL
    ds.loc[ds['url'].isna(), 'url'] = 'http://' + regno['domain'] + '/'
    return ds


async def update_dataset(*, check_existing_sites=False) -> _DataFrame:
    """Update dataset and return newly found that could not be added."""
    ds = load_dataset(site=False)
    fipiran_df = await _fipiran_data(ds)
    ds = await _update_existing_rows_using_fipiran(
        ds, fipiran_df, check_existing_sites
    )

    new_items = fipiran_df[~fipiran_df['regNo'].isin(ds['regNo'])]

    tsetmc_df = await _tsetmc_dataset()
    await _add_ins_code(new_items)
    ds = _add_new_items_to_ds(new_items, ds)

    # update all data, old or new, using tsetmc_df
    ds.update(tsetmc_df)

    ds.reset_index(inplace=True)
    save_dataset(ds)

    return new_items[new_items['insCode'].isna()]


_NA = _pd.NA


async def _check_site_type(row) -> None:
    site: BaseSite = row.site
    if site is _NA:  # na
        return

    try:
        detected = await BaseSite.from_url(site.url)
    except Exception as e:
        _error(f'Exception occured during checking of {site}: {e}')
        return
    if type(detected) is type(site):
        _debug(f'checked {site.url}')
        return
    _error(
        f'Detected site type for {site.url} is {type(detected).__name__},'
        f' but dataset site type is {type(site).__name__}.'
    )


async def _check_reg_no(row):
    ds_reg_no = row.regNo
    if ds_reg_no is _NA:  # todo: remove this after adding regNo for all
        return
    site: BaseSite = row.site
    try:
        actual_reg_no = await site.reg_no()
    except Exception as e:
        _error(f'Exception during checking regNo on {site.url}: {e}')
        return
    if ds_reg_no == actual_reg_no:
        return
    _error(f'regNo mismatch:\n {site.url=}\n {ds_reg_no=}\n {actual_reg_no=}')


async def check_dataset(live=False):
    global ssl
    ds = load_dataset(site=False)
    assert ds['l18'].is_unique
    assert ds['name'].is_unique, ds['name'][ds['name'].duplicated()]
    assert ds['type'].unique().isin(_ETF_TYPES.values()).all()  # type: ignore
    assert ds['insCode'].is_unique
    reg_numbers = ds['regNo']
    known_reg_numbers = reg_numbers[reg_numbers.notna()]
    assert known_reg_numbers.is_unique, ds[known_reg_numbers.duplicated()]

    if not live:
        return

    ds['site'] = ds[ds['siteType'].notna()].apply(_make_site, axis=1)  # type: ignore

    rows = [*ds.itertuples()]
    check_site_coros = [_check_site_type(r) for r in rows]
    check_reg_no_coros = [_check_reg_no(r) for r in rows]

    local_ssl = ssl
    ssl = False  # many sites fail ssl verification
    try:
        await _gather(*check_site_coros)
        await _gather(*check_reg_no_coros)
    finally:
        ssl = local_ssl

    if not (no_site := ds[ds['site'].isna()]).empty:
        _warning(
            f'some dataset entries have no associated site:\n{no_site["l18"]}'
        )
