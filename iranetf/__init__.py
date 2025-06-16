__version__ = '0.25.3.dev0'
import logging as _logging
from abc import ABC as _ABC, abstractmethod as _abstractmethod
from asyncio import gather as _gather
from datetime import datetime as _datetime
from functools import reduce
from json import JSONDecodeError as _JSONDecodeError, loads as _loads
from logging import (
    debug as _debug,
    error as _error,
    info as _info,
    warning as _warning,
)
from pathlib import Path as _Path
from re import (
    ASCII as _ASCII,
    findall as _findall,
    search as _search,
    split as _split,
)
from typing import Any as _Any, TypedDict as _TypedDict

import polars as _pl
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
from pandas import DataFrame as _Df
from tsetmc.instruments import (
    Instrument as _Instrument,
    search as _tsetmc_search,
)

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
    """Converts a Jalaali date string to a Gregorian datetime object."""
    # Ensure all parts are integers before passing to _jdatetime
    return _jdatetime(*[int(i) for i in s.split('/')]).togregorian()


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
    __slots__ = 'last_response', 'url'

    ds: _pl.DataFrame  # Changed type hint to Polars DataFrame
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
        """
        Fetches JSON data from the given path.
        If df is True, returns a Polars DataFrame.
        """
        r = await _get(self.url + path, params, cookies)
        self.last_response = r
        content = await r.read()
        j = _loads(content)
        if df is True:
            return _pl.DataFrame(j)  # Changed to Polars DataFrame
        return j

    @_abstractmethod
    async def live_navps(self) -> LiveNAVPS: ...

    @_abstractmethod
    async def navps_history(self) -> _pl.DataFrame: ...  # Changed type hint

    @_abstractmethod
    async def cache(self) -> float: ...

    @classmethod
    def from_l18(cls, l18: str) -> AnySite:
        """
        Loads the dataset and filters for the specific 'l18' value to
        retrieve the associated site object.
        """
        try:
            ds = cls.ds
        except AttributeError:
            # load_dataset now returns a Polars DataFrame
            ds = cls.ds = load_dataset(site=True)
        # Polars doesn't use `.loc` for indexing; filter and select column
        # .item() extracts the single scalar value from the resulting Series
        return ds.filter(_pl.col('l18') == l18).select('site').item()

    def _check_aa_keys(self, d: dict):
        """Checks for unknown asset allocation keys and logs a warning."""
        if d.keys() <= self._aa_keys:
            return
        _warning(
            f'Unknown asset allocation keys on {self!r}: {d.keys() - self._aa_keys}'
        )

    @staticmethod
    async def from_url(url: str) -> AnySite:
        """Determines the site type based on the content of the URL."""
        content = await (await _get(url)).read()
        rfind = content.rfind

        if rfind(b'<div class="tadbirLogo"></div>') != -1:
            tp_site = TadbirPardaz(url)
            info = await tp_site.info()
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


def _comma_int(s: str) -> int:
    """Converts a comma-separated string to an integer."""
    return int(s.replace(',', ''))


def _comma_float(s: str) -> float:
    """Converts a comma-separated string to a float."""
    return float(s.replace(',', ''))


class MabnaDP(BaseSite):
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

    async def navps_history(self) -> _pl.DataFrame:  # Changed type hint
        """
        Fetches NAVPS history and returns it as a Polars DataFrame.
        Converts Jalaali dates to Gregorian and renames columns.
        """
        j: list[dict] = await self._json('navps.json')
        df = _pl.DataFrame(j[0]['values'])
        df = df.with_columns(
            _pl.col('date').map_elements(
                lambda x: _jdatetime.strptime(
                    x, format='%Y%m%d000000'
                ).togregorian(),
                return_dtype=_pl.Datetime,
            )
        )
        # Polars chain operations, no inplace
        df = df.rename(
            {
                'purchase_price': 'creation',
                'redeption_price': 'redemption',
                'statistical_value': 'statistical',
            }
        )
        # Polars doesn't use set_index; 'date' remains a regular column
        return df

    async def version(self) -> str:
        """Extracts version number from the site's HTML content."""
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


class LeveragedMabnaDP(BaseSite):
    async def _json(self, path, **kwa) -> _Any:
        params: dict | None = kwa.get('params')
        if params is None:
            kwa['params'] = {'portfolio_id': '1'}
        else:
            # Using .setdefault instead of .setdefalt (typo in original)
            params.setdefault('portfolio_id', '1')

        return await super()._json(f'api/v2/public/fund/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        data = (await self._json('etf/navps/latest'))['data']
        # Converting ISO format string to datetime and removing timezone
        data['date'] = _datetime.fromisoformat(data.pop('date_time')).replace(
            tzinfo=None
        )
        data['creation'] = data.pop('purchase_price')
        data['redemption'] = data.pop('redemption_price')
        return data

    async def navps_history(self) -> _pl.DataFrame:  # Changed type hint
        """
        Fetches NAVPS history for leveraged funds and returns it as a Polars DataFrame.
        Handles datetime conversion and column renaming.
        """
        data: list[dict] = (await self._json('chart'))['data']
        df = _pl.DataFrame(data)  # Create Polars DataFrame
        df = df.rename(
            {  # Rename columns
                'redemption_price': 'redemption',
                'statistical_value': 'statistical',
                'purchase_price': 'creation',
            }
        )
        # Convert date_time to datetime and normalize
        df = df.with_columns(
            _pl.col('date_time')
            .str.to_datetime(time_unit='ns')
            .dt.convert_time_zone('Asia/Tehran')
            .dt.replace_time_zone(None)
            .dt.date()  # Get only the date part
            .cast(_pl.Datetime)  # Cast back to Datetime type
            .alias('date')
        ).drop('date_time')  # Drop the original date_time column
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
        # Extracting JSON blobs from HTML using partition and loads
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

    _col_rename = {
        'JalaliDate': 'date',
        'PurchaseNAVPerShare': 'creation',
        'SellNAVPerShare': 'redemption',
        'StatisticalNAVPerShare': 'statistical',
    }

    async def navps_history(self) -> _pl.DataFrame:  # Changed type hint
        """
        Fetches NAVPS history from Rayan Hamafza and returns it as a Polars DataFrame.
        Converts Jalaali dates to Gregorian.
        """
        df: _pl.DataFrame = await self._json(  # Create Polars DataFrame
            f'NavPerShare/{self.fund_id}', df=True
        )
        df = df.rename(self._col_rename)
        df = df.with_columns(
            _pl.col('date').map_elements(  # Apply _j2g using map_elements
                _j2g, return_dtype=_pl.Datetime
            )
        )
        # Polars doesn't use set_index
        return df

    _nav_history_path = 'DailyNAVChart/1'

    async def nav_history(self) -> _pl.DataFrame:  # Changed type hint
        df: _pl.DataFrame = await self._json(self._nav_history_path, df=True)
        df = df.rename(self._col_rename)
        df = df.with_columns(
            _pl.col('date').map_elements(  # Apply _j2g using map_elements
                _j2g, return_dtype=_pl.Datetime
            )
        )
        return df

    _portfolio_industries_path = 'Industries/1'

    async def portfolio_industries(self) -> _pl.DataFrame:  # Changed type hint
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
        return {
            k: v / 100 if isinstance(v, int | float) else v
            for k, v in d.items()
        }

    async def dividend_history(self) -> _pl.DataFrame:  # Changed type hint
        """
        Fetches dividend history from Rayan Hamafza and returns it as a Polars DataFrame.
        Converts Jalaali dates to Gregorian.
        """
        j: dict = await self._json('Profit/1')
        df = _pl.DataFrame(j['data'])  # Create Polars DataFrame
        df = df.with_columns(
            _pl.col(
                'ProfitDate'
            ).map_elements(  # Apply UDF for date conversion
                lambda i: _jdatetime.strptime(
                    i, format='%Y/%m/%d'
                ).togregorian(),
                return_dtype=_pl.Datetime,
            )
        )
        return df

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        return (
            aa['DepositTodayPercent']
            + aa['CashTodayPercent']
            + aa['BondTodayPercent']
        )


# noinspection PyAbstractClass
class BaseTadbirPardaz(BaseSite):
    async def version(self) -> str:
        content = await _read(self.url)
        start = content.find(b'version number:')
        end = content.find(b'\n', start)
        return content[start + 15 : end].strip().decode()

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
    }

    async def asset_allocation(self) -> dict:
        j: dict = await self._json('Chart/AssetCompositions')
        d = {i['x']: i['y'] / 100 for i in j['List']}
        self._check_aa_keys(d)
        return d

    async def info(self) -> dict[str, _Any]:
        content = await (await _get(self.url)).read()
        d: dict[str, _Any] = {
            'isETFMultiNavMode': _search(
                rb'isETFMultiNavMode\s*=\s*true;', content, _ASCII
            )
            is not None,
            'isLeveragedMode': _search(
                rb'isLeveragedMode\s*=\s*true;', content, _ASCII
            )
            is not None,
            'isEtfMode': _search(rb'isEtfMode\s*=\s*true;', content, _ASCII)
            is not None,
        }
        if d['isETFMultiNavMode']:
            baskets = _findall(
                r'<option [^>]*?value="(\d+)">([^<]*)</option>',
                content.partition(b'<div class="drp-basket-header">')[2]
                .partition(b'</select>')[0]
                .decode(),
            )
            d['basketIDs'] = dict(baskets)
        return d

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        g = aa.get
        return (
            g('نقد و بانک (سپرده)', 0.0)
            + g('نقد و بانک (جاری)', 0.0)
            + g('اوراق مشارکت', 0.0)
        )


def _fanum2en(expr: _pl.Expr) -> _pl.Expr:
    return (
        expr.str.replace_all('۰', '0')
        .str.replace_all('۱', '1')
        .str.replace_all('۲', '2')
        .str.replace_all('۳', '3')
        .str.replace_all('۴', '4')
        .str.replace_all('۵', '5')
        .str.replace_all('۶', '6')
        .str.replace_all('۷', '7')
        .str.replace_all('۸', '8')
        .str.replace_all('۹', '9')
        .str.replace_all(' ', '')
    )


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
                    # Apply _comma_int for integer conversion
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

    async def navps_history(self) -> _pl.DataFrame:  # Changed type hint
        """
        Fetches NAVPS history and returns it as a Polars DataFrame.
        Converts dates to datetime objects.
        """
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )
        creation, statistical, redemption = [
            [d['y'] for d in i['List']] for i in j
        ]
        date = [d['x'] for d in j[0]['List']]
        df = _pl.DataFrame(  # Create Polars DataFrame
            {
                'date': date,
                'creation': creation,
                'redemption': redemption,
                'statistical': statistical,
            }
        )
        # Convert date column to datetime
        df = df.with_columns(
            _pl.col('date').str.to_datetime(format='%m/%d/%Y')
        )
        # Polars doesn't use set_index
        return df

    async def dividend_history(self) -> _pl.DataFrame:  # Changed type hint
        """
        Fetches dividend history and returns it as a Polars DataFrame.
        Handles parsing HTML tables, date conversion, and type casting.
        """
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

        # Define schema for Polars DataFrame explicitly
        schema = {
            'row': _pl.String,
            'ProfitDate': _pl.String,
            'FundUnit': _pl.String,
            'UnitProfit': _pl.String,
            'SUMAllProfit': _pl.String,
            'ProfitPercent': _pl.String,
        }
        df = _pl.DataFrame(all_rows, schema=schema, orient='row')

        # Convert ProfitDate using map_elements
        df = df.with_columns(
            _pl.col('ProfitDate').map_elements(
                lambda i: _jdatetime.strptime(
                    i, format='%Y/%m/%d'
                ).togregorian(),
                return_dtype=_pl.Datetime,
            )
        )
        comma_cols = ['FundUnit', 'SUMAllProfit']
        # Apply _comma_int to multiple columns using Polars expressions
        df = df.with_columns(
            [
                _pl.col(c).map_elements(_comma_int, return_dtype=_pl.Int64)
                for c in comma_cols
            ]
        )
        int_cols = ['row', 'UnitProfit']
        df = df.with_columns(
            [
                _pl.col(c).map_elements(_comma_int, return_dtype=_pl.Int64)
                for c in int_cols
            ]
        )

        # Cast ProfitPercent to Float64
        df = df.with_columns(
            _pl.col('ProfitPercent').pipe(_fanum2en).cast(_pl.Float64)
        )
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
    async def navps_history(self) -> _pl.DataFrame:  # Changed type hint
        """
        Fetches NAVPS history for leveraged Tadbir Pardaz funds and returns it as a Polars DataFrame.
        Handles merging multiple data series by date.
        """
        j: list = await self._json(
            'Chart/TotalNAV', params={'type': 'getnavtotal'}
        )

        frames: list[_pl.DataFrame] = []
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
            df = _pl.DataFrame(i['List'])
            df = df.drop('name')
            df = df.with_columns(
                _pl.col('x').str.to_datetime('%m/%d/%Y').alias('date')
            ).drop('x')  # Drop original 'x' column
            df = df.rename({'y': name})  # Rename 'y' to the specific name
            df = df.unique('date')  # Drop duplicates based on 'date' column
            frames.append(df)

        # Use reduce with join to combine all DataFrames by 'date' column
        df = frames.pop()
        df = reduce(
            lambda left, right: left.join(right, on='date', how='left'),
            frames,
        )
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


def _make_site_polars_udf(row: dict) -> BaseSite:
    """UDF for creating a BaseSite object from a Polars row (dict)."""
    type_str = row['siteType']
    site_class = globals()[type_str]
    return site_class(row['url'])


def load_dataset(
    *, site=True, inst=False
) -> _pl.DataFrame:  # Changed return type hint
    """
    Load dataset.csv as a Polars DataFrame.
    If site is True, convert url and siteType columns to site object.
    """
    df = _pl.read_csv(  # Changed to Polars read_csv
        _DATASET_PATH,
        encoding='utf-8-sig',
        # low_memory, lineterminator not directly applicable to Polars read_csv
        # Polars handles string inference by default
        # No need for pandas CategoricalDtype, use pl.Categorical
        schema={
            'l18': _pl.String,
            'name': _pl.String,
            'type': _pl.Categorical,
            'insCode': _pl.String,
            'regNo': _pl.String,
            'url': _pl.String,
            'siteType': _pl.Categorical,
        },
    )

    if site:
        # Create a struct column with 'url' and 'siteType'
        # Then map_elements with the UDF to create 'site' objects
        df = df.with_columns(
            _pl.when(_pl.col('siteType').is_not_null())
            .then(
                _pl.struct(['url', 'siteType']).map_elements(
                    _make_site_polars_udf, return_dtype=_pl.Object
                )
            )
            .otherwise(
                _pl.lit(None, dtype=_pl.Object)
            )  # Handle nulls for siteType
            .alias('site')
        )

    if inst:
        # Apply _Instrument to insCode using map_elements
        df = df.with_columns(
            _pl.col('insCode')
            .map_elements(_Instrument, return_dtype=_pl.Object)
            .alias('inst')
        )

    return df


def save_dataset(ds: _pl.DataFrame):  # Changed type hint
    """
    Saves the Polars DataFrame to dataset.csv.
    """
    ds.select(  # Use select to reorder columns
        [
            'l18',
            'name',
            'type',
            'insCode',
            'regNo',
            'url',
            'siteType',
        ]
    ).sort('l18').write_csv(  # Use Polars sort and write_csv
        _DATASET_PATH,
        include_bom=True,
        include_header=True,  # Explicitly include header
    )


async def _check_validity(site: BaseSite, retry=0) -> tuple[str, str] | None:
    """Checks the validity of a site by trying to fetch its live NAVPS."""
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
    """Determines the site type for a given domain."""
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
    fipiran_df: _pl.DataFrame,
    known_domains: _pl.Series | None,  # Changed type hints
):
    """
    Adds URL and site type information to the fipiran DataFrame
    by checking unknown domains.
    """
    domains_to_be_checked = fipiran_df.filter(
        _pl.col('domain').is_not_null()
    ).select('domain')
    if known_domains is not None:
        domains_to_be_checked = domains_to_be_checked.filter(
            ~_pl.col('domain').is_in(known_domains)
        ).select('domain')

    _info(f'checking site types of {domains_to_be_checked.height} domains')
    if domains_to_be_checked.is_empty():
        return fipiran_df

    # there will be a lot of redirection warnings, let's silent them
    _logging.disable()  # to disable redirection warnings
    list_of_tuples = await _gather(
        *[
            _url_type(d)
            for d in domains_to_be_checked.get_column('domain').to_list()
        ]  # Convert to Python list
    )
    _logging.disable(_logging.NOTSET)

    # Correct way to apply results back based on domains_to_be_checked
    # Create a temporary DataFrame with domains and their new url/siteType
    temp_df = _pl.DataFrame(
        {
            'domain': domains_to_be_checked.get_column('domain').to_list(),
            'url_found': [tup[0] for tup in list_of_tuples],
            'site_type_found': [tup[1] for tup in list_of_tuples],
        }
    )

    # Join fipiran_df with temp_df to get the new url and siteType
    fipiran_df = fipiran_df.join(temp_df, on='domain', how='left')

    # Use coalesce to update 'url' and 'siteType' only where they are null in fipiran_df
    # and new values were found
    fipiran_df = fipiran_df.with_columns(
        _pl.coalesce(_pl.col('url_found'), _pl.col('url')).alias('url'),
        _pl.coalesce(_pl.col('site_type_found'), _pl.col('siteType')).alias(
            'siteType'
        ),
    ).drop(['url_found', 'site_type_found'])  # Drop temporary columns

    return fipiran_df  # Return the updated DataFrame


async def _add_ins_code(
    new_items: _pl.DataFrame,
) -> _pl.DataFrame:  # Changed type hint
    """
    Adds 'insCode' to new items by searching on tsetmc for names without a code.
    """
    names_without_code = new_items.filter(_pl.col('insCode').is_null()).select(
        'name'
    )
    if names_without_code.is_empty():
        return new_items  # Return original if no updates needed

    _info('searching names on tsetmc to find their insCode')
    results = await _gather(
        *[
            _tsetmc_search(name)
            for name in names_without_code.get_column('name').to_list()
        ]
    )
    ins_codes = [(None if len(r) != 1 else r[0]['insCode']) for r in results]

    # Create a DataFrame for the new insCodes to join back
    ins_code_update_df = _pl.DataFrame(
        {
            'name': names_without_code.get_column('name').to_list(),
            'insCode_new': ins_codes,
        }
    )

    # Left join new_items with the ins_code_update_df
    new_items = new_items.join(ins_code_update_df, on='name', how='left')

    # Coalesce the original 'insCode' with the new 'insCode_new'
    new_items = new_items.with_columns(
        _pl.coalesce(_pl.col('insCode_new'), _pl.col('insCode')).alias(
            'insCode'
        )
    ).drop('insCode_new')  # Drop the temporary column

    return new_items


async def _fipiran_data(
    ds: _pl.DataFrame,
) -> _pl.DataFrame:  # Changed type hint
    """
    Fetches fund data from Fipiran and processes it into a Polars DataFrame.
    """
    import fipiran.funds

    _info('await fipiran.funds.funds()')
    fipiran_pd_df: _Df = await fipiran.funds.funds()

    # Convert fipiran_df to Polars DataFrame immediately
    fipiran_df = _pl.DataFrame(fipiran_pd_df)

    reg_not_in_fipiran = ds.filter(
        ~_pl.col('regNo').is_in(fipiran_df.get_column('regNo'))
    )
    if not reg_not_in_fipiran.is_empty():
        # Polars has a nice __repr__ for DataFrames
        _warning(
            f'Some dataset rows were not found on fipiran:\n{reg_not_in_fipiran}'
        )

    df = fipiran_df.filter(
        (_pl.col('typeOfInvest') == 'Negotiable')
        & ~(_pl.col('fundType').is_in([11, 12, 13, 14, 16]))
        & _pl.col('isCompleted')
    )

    df = df.select(
        [
            'regNo',
            'smallSymbolName',
            'name',
            'fundType',
            'websiteAddress',
            'insCode',
        ]
    )

    df = df.rename(
        {  # Rename columns
            'fundType': 'type',
            'websiteAddress': 'domain',
            'smallSymbolName': 'l18',
        }
    )

    # Replace fundType numbers with their string equivalents
    df = df.with_columns(
        _pl.col('type').replace_strict(_ETF_TYPES, return_dtype=_pl.String)
    )

    return df


async def _tsetmc_dataset() -> _pl.DataFrame:  # Changed type hint
    """
    Updates and loads the tsetmc dataset into a Polars DataFrame.
    """
    from tsetmc.dataset import LazyDS, update

    _info('await tsetmc.dataset.update()')
    await update()

    df = LazyDS.df  # This will likely return a pandas DataFrame
    df = _pl.DataFrame(df)  # Convert to Polars DataFrame

    df = df.drop(['l30', 'isin', 'cisin'])  # Drop columns
    return df


def _add_new_items_to_ds(
    new_items: _pl.DataFrame, ds: _pl.DataFrame
) -> _pl.DataFrame:  # Changed type hints
    """
    Adds newly found items to the main dataset.
    """
    if new_items.is_empty():
        return ds
    new_with_code = new_items.filter(_pl.col('insCode').is_not_null())
    if not new_with_code.is_empty():
        # Select relevant columns from new_with_code and then concatenate vertically
        # Polars does not have set_index for concatenation; assumes schema compatibility
        ds = _pl.concat(
            [
                ds,
                new_with_code.select(
                    [
                        'l18',
                        'name',
                        'type',
                        'insCode',
                        'regNo',
                        'url',
                        'siteType',
                    ]
                ),
            ],  # Explicitly select columns to match ds
            how='vertical',
        )
    else:
        _info('new_with_code is empty!')
    return ds


async def _update_existing_rows_using_fipiran(
    ds: _pl.DataFrame,
    fipiran_df: _pl.DataFrame,
    check_existing_sites: bool,  # Changed type hints
) -> _pl.DataFrame:
    """
    Updates existing rows in the dataset using data from Fipiran DataFrame.
    """
    # _add_url_and_type now returns the updated fipiran_df
    fipiran_df = await _add_url_and_type(
        fipiran_df,
        known_domains=None
        if check_existing_sites
        else ds.filter(_pl.col('url').is_not_null())
        .select(
            _pl.col('url').str.extract('//(.*)/').alias('domain_extracted')
        )
        .get_column('domain_extracted'),
    )

    # Join ds with fipiran_df on 'regNo' to get updated values
    # Use 'left' join to keep all rows from ds
    ds = ds.join(
        fipiran_df.select(
            ['regNo', 'name', 'type', 'url', 'siteType', 'domain', 'l18']
        ),
        on='regNo',
        how='left',
        suffix='_fipiran',
    )

    # Coalesce (take first non-null) to update existing columns
    ds = ds.with_columns(
        _pl.coalesce(_pl.col('name_fipiran'), _pl.col('name')).alias('name'),
        _pl.coalesce(_pl.col('type_fipiran'), _pl.col('type')).alias('type'),
        # Do not overwrite MultiNAV type and URL - this is handled by coalesce
        _pl.coalesce(_pl.col('url_fipiran'), _pl.col('url')).alias('url'),
        _pl.coalesce(_pl.col('siteType_fipiran'), _pl.col('siteType')).alias(
            'siteType'
        ),
        _pl.coalesce(_pl.col('l18_fipiran'), _pl.col('l18')).alias('l18'),
    )

    # use domain as URL for those who do not have any URL
    # Assuming 'domain' from fipiran_df is now available in ds due to the join
    ds = ds.with_columns(
        _pl.when(_pl.col('url').is_null())
        .then(
            _pl.lit('http://') + _pl.col('domain_fipiran') + _pl.lit('/')
        )  # Use domain from fipiran join
        .otherwise(_pl.col('url'))
        .alias('url')
    )

    # Drop the temporary fipiran columns used for coalescing
    ds = ds.drop([col for col in ds.columns if col.endswith('_fipiran')])
    return ds


async def update_dataset(
    *, check_existing_sites=False
) -> _pl.DataFrame:  # Changed return type hint
    """
    Updates the main dataset by fetching data from Fipiran and Tsetmc.
    Returns newly found items that could not be added due to missing insCode.
    """
    ds = load_dataset(site=False)
    fipiran_df = await _fipiran_data(
        ds
    )  # This function now returns a Polars DataFrame
    ds = await _update_existing_rows_using_fipiran(
        ds, fipiran_df, check_existing_sites
    )

    # Filter new items using Polars expressions
    new_items = fipiran_df.filter(
        ~_pl.col('regNo').is_in(ds.get_column('regNo'))
    )

    tsetmc_df = (
        await _tsetmc_dataset()
    )  # This function now returns a Polars DataFrame
    new_items = await _add_ins_code(
        new_items
    )  # _add_ins_code now takes and returns Polars DataFrame
    ds = _add_new_items_to_ds(
        new_items, ds
    )  # _add_new_items_to_ds now takes and returns Polars DataFrame

    # update all data, old or new, using tsetmc_df
    # Join ds with tsetmc_df on 'insCode'
    ds = ds.join(tsetmc_df, on='insCode', how='left', suffix='_tsetmc')

    # Coalesce relevant columns to update ds with tsetmc_df values
    ds = ds.with_columns(
        _pl.coalesce(_pl.col('name_tsetmc'), _pl.col('name')).alias('name'),
        _pl.coalesce(_pl.col('type_tsetmc'), _pl.col('type')).alias('type'),
        _pl.coalesce(_pl.col('l18_tsetmc'), _pl.col('l18')).alias('l18'),
        _pl.coalesce(_pl.col('regNo_tsetmc'), _pl.col('regNo')).alias('regNo'),
        _pl.coalesce(_pl.col('url_tsetmc'), _pl.col('url')).alias('url'),
        _pl.coalesce(_pl.col('siteType_tsetmc'), _pl.col('siteType')).alias(
            'siteType'
        ),
    ).drop(
        [col for col in ds.columns if col.endswith('_tsetmc')]
    )  # Drop temporary tsetmc columns

    # No need for reset_index if we didn't explicitly set an index
    save_dataset(ds)

    return new_items.filter(
        _pl.col('insCode').is_null()
    )  # Filter for items with null insCode


async def _check_site_type(site: BaseSite | None) -> None:
    """Checks if the detected site type matches the dataset's recorded type."""
    if (
        site is None
    ):  # Use `is None` for Polars Object column nulls, not `!= site`
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


async def check_dataset(live=False):
    """
    Performs consistency checks on the dataset, optionally including live site checks.
    """
    global ssl
    ds = load_dataset(site=False)

    # Polars assertions for uniqueness
    assert ds.get_column('l18').n_unique() == ds.height, (
        'l18 column is not unique'
    )
    # To find duplicates in Polars: ds.group_by('name').len().filter(pl.col('len') > 1)
    assert ds.get_column('name').n_unique() == ds.height, (
        f'name column is not unique: {ds.filter(_pl.col("name").is_duplicated()).select("name")}'
    )
    assert (
        ds.get_column('type').unique().is_in([*_ETF_TYPES.values()]).all()
    ), 'Unknown ETF types found'  # Use .item() to get scalar boolean
    assert ds.get_column('insCode').n_unique() == ds.height, (
        'insCode column is not unique'
    )
    reg_numbers = ds.get_column('regNo')
    known_reg_numbers = reg_numbers.filter(reg_numbers.is_not_null())
    assert known_reg_numbers.n_unique() == len(known_reg_numbers)(  # type: ignore
        f'regNo column has duplicates: {ds.filter(_pl.col("regNo").is_duplicated()).select("regNo")}'
    )

    if not live:
        return

    # Create 'site' column for live check
    ds = ds.with_columns(
        _pl.when(_pl.col('siteType').is_not_null())
        .then(
            _pl.struct(['url', 'siteType']).map_elements(
                _make_site_polars_udf, return_dtype=_pl.Object
            )
        )
        .otherwise(_pl.lit(None, dtype=_pl.Object))
        .alias('site')
    )

    # Collect site objects to iterate and gather coroutines
    sites_to_check = (
        ds.filter(_pl.col('site').is_not_null()).get_column('site').to_list()
    )
    coros = [_check_site_type(site) for site in sites_to_check]

    local_ssl = ssl
    ssl = False  # many sites fail ssl verification
    try:
        await _gather(*coros)
    finally:
        ssl = local_ssl

    if not (no_site := ds.filter(_pl.col('site').is_null())).is_empty():
        _warning(
            f'some dataset entries have no associated site:\n{no_site.select("l18")}'
        )
