__version__ = '0.22.3'
import logging as _logging
from abc import ABC as _ABC, abstractmethod as _abstractmethod
from asyncio import gather as _gather
from datetime import datetime as _datetime
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


type AnySite = 'LeveragedTadbirPardaz | TadbirPardaz | RayanHamafza | MabnaDP | RayanHamafzaMultiNAV'


class BaseSite(_ABC):
    __slots__ = 'url', 'last_response'

    ds: _DataFrame
    _aa_keys: set

    def __init__(self, url: str):
        assert url[-1] == '/', 'the url must end with `/`'
        self.url = url

    def __repr__(self):
        return f"{type(self).__name__}('{self.url}')"

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
        content = await (await _get(url)).read()
        rfind = content.rfind
        find = content.find

        if rfind(b'<div class="tadbirLogo"></div>') != -1:
            tp_site = TadbirPardaz(url)
            info = await tp_site.info()
            if info['isLeveragedMode']:
                return LeveragedTadbirPardaz(url)
            if info['isETFMultiNavMode']:
                return TadbirPardazMultiNAV(url + '#2')
            return tp_site

        if rfind(b'Rayan Ham Afza') != -1:
            if find(b'<body class="multi-fund">') != -1:
                return RayanHamafzaMultiNAV(url + '#1')
            return RayanHamafza(url)

        if rfind(b'://mabnadp.com/') != -1:
            return MabnaDP(url)

        raise ValueError(f'Could not determine site type for {url}.')


def _comma_int(s: str) -> int:
    return int(s.replace(',', ''))


def _comma_float(s: str) -> float:
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
        return (g('وجه نقد', 0.0) + g('سپرده بانکی', 0.0)) / 100.0


class RayanHamafza(BaseSite):
    async def _json(self, path, **kwa) -> _Any:
        return await super()._json(f'Data/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        d: dict = await self._json('FundLiveData')
        d['creation'] = d.pop('PurchaseNAVPerShare')
        d['redemption'] = d.pop('SellNAVPerShare')
        d['date'] = _jdatetime.strptime(
            f'{d.pop("JalaliDate")} {d.pop("Time")}', '%Y/%m/%d %H:%M'
        ).togregorian()
        return d  # type: ignore

    async def navps_history(self) -> _DataFrame:
        df: _DataFrame = await self._json('NAVPerShare', df=True)
        df.columns = ['date', 'creation', 'redemption', 'statistical']
        df['date'] = df['date'].map(_j2g)
        df.set_index('date', inplace=True)
        return df

    async def nav_history(self) -> _DataFrame:
        df: _DataFrame = await self._json('PureAsset', df=True)
        df.columns = ['nav', 'date', 'redemption_navps']
        df['date'] = df['date'].map(_j2g)
        return df

    async def portfolio_industries(self) -> _DataFrame:
        return await self._json('Industries', df=True)

    _aa_keys = {
        'DepositTodayPercent',
        'TopFiveStockTodayPercent',
        'CashTodayPercent',
        'OtherAssetTodayPercent',
        'BondTodayPercent',
        'OtherStock',
        'JalaliDate',
    }

    async def asset_allocation(self) -> dict:
        d: dict = await self._json('MixAsset')
        self._check_aa_keys(d)
        return d

    async def dividend_history(self) -> _DataFrame:
        j: dict = await self._json('FundProfit')
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
        ) / 100.0


class RayanHamafzaMultiNAV(RayanHamafza):
    """Same as RayanHamafza, only send fundId as a cookie."""

    __slots__ = 'cookies'

    def __init__(self, url: str):
        """Note: the url should end with #<fund_id> where fund_id is an int."""
        url, _, fund_id = url.partition('#')
        self.cookies = {'fundId': fund_id}
        super().__init__(url)

    async def _json(self, path, **kwa) -> _Any:
        return await super()._json(path, cookies=self.cookies, **kwa)


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
        d = {i['x']: i['y'] for i in j['List']}
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
        ) / 100.0


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
        df['ProfitDate'] = df['ProfitDate'].apply(
            lambda i: _jdatetime.strptime(i, format='%Y/%m/%d').togregorian()
        )
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

        frames = []
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
            df = _DataFrame(i['List']).drop(columns='name')
            df['date'] = _to_datetime(df['x'])
            df.drop(columns='x', inplace=True)
            df.rename(columns={'y': name}, inplace=True)
            df.set_index('date', inplace=True)
            frames.append(df)

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


async def _check_site_type(site: BaseSite) -> None:
    if site != site:  # na
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
    global ssl
    ds = load_dataset(site=False)
    assert ds['l18'].is_unique
    assert ds['name'].is_unique
    assert ds['type'].unique().isin(_ETF_TYPES.values()).all()  # type: ignore
    assert ds['insCode'].is_unique
    reg_numbers = ds['regNo']
    known_reg_numbers = reg_numbers[reg_numbers.notna()]
    assert known_reg_numbers.is_unique, ds[known_reg_numbers.duplicated()]

    if not live:
        return

    ds['site'] = ds[ds['siteType'].notna()].apply(_make_site, axis=1)  # type: ignore

    coros = ds['site'].apply(_check_site_type)  # type: ignore

    local_ssl = ssl
    ssl = False  # many sites fail ssl verification
    try:
        await _gather(*coros)
    finally:
        ssl = local_ssl

    if not (no_site := ds[ds['site'].isna()]).empty:
        _warning(
            f'some dataset entries have no associated site:\n{no_site["l18"]}'
        )
