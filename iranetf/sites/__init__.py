import warnings as _w
from abc import ABC as _ABC, abstractmethod as _abstractmethod
from asyncio import TimeoutError as _TimeoutError, gather as _gather
from json import JSONDecodeError as _JSONDecodeError, loads as _loads
from logging import error as _error, info as _info, warning as _warning
from pathlib import Path as _Path
from typing import TypedDict as _TypedDict

import pandas as _pd
from aiohttp import (
    ClientConnectorError as _ClientConnectorError,
    ClientOSError as _ClientOSError,
    ServerDisconnectedError as _ServerDisconnectedError,
    ServerTimeoutError as _ServerTimeoutError,
    TooManyRedirects as _TooManyRedirects,
)
from pandas import (
    DataFrame as _DataFrame,
    Series as _Series,
    concat as _concat,
    read_csv as _read_csv,
    to_datetime as _to_datetime,
)

import iranetf
from iranetf import _datetime, _get, _j2g, _jdatetime

_pd.options.mode.copy_on_write = True
_ETF_TYPES = {  # numbers are according to fipiran
    6: 'Stock',
    4: 'Fixed',
    7: 'Mixed',
    5: 'Commodity',
    17: 'FOF',
    18: 'REIT',
}


class LiveNAVPS(_TypedDict, total=True):
    issue: int
    cancel: int
    date: _datetime


class BaseSite(_ABC):
    __slots__ = 'url', 'last_response'

    def __init__(self, url: str):
        assert url[-1] == '/', 'the url must end with `/`'
        self.url = url

    def __repr__(self):
        return f'{type(self).__name__}({self.url})'

    async def _json(
        self, path: str, df: bool = False
    ) -> list | dict | str | _DataFrame:
        r = await _get(self.url + path)
        self.last_response = r
        content = await r.read()
        j = _loads(content)
        if df is True:
            return _DataFrame(j, copy=False)
        return j

    @_abstractmethod
    async def live_navps(self) -> LiveNAVPS:
        ...

    @_abstractmethod
    async def navps_history(self) -> _DataFrame:
        ...


def _fa_int(s: str) -> int:
    return int(s.replace(',', ''))


class MabnaDP(BaseSite):
    async def _json(
        self, path: str, df: bool = False
    ) -> list | dict | _DataFrame:
        return await super()._json(f'api/v1/overall/{path}', df)

    async def live_navps(self) -> LiveNAVPS:
        j = await self._json('navetf.json')
        j['date'] = _jdatetime.strptime(
            j['date_time'], '%H:%M %Y/%m/%d'
        ).togregorian()
        j['issue'] = _fa_int(j.pop('purchase_price'))
        j['cancel'] = _fa_int(j.pop('redemption_price'))
        return j

    async def navps_history(self) -> _DataFrame:
        j = await self._json('navps.json')
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
        df['issue'] = df.pop('purchase_price')
        df['cancel'] = df.pop('redeption_price')
        df['statistical'] = df.pop('statistical_value')
        return df


class RayanHamafza(BaseSite):
    async def _json(
        self, path: str, df: bool = False
    ) -> list | dict | _DataFrame:
        return await super()._json(f'Data/{path}', df)

    async def live_navps(self) -> LiveNAVPS:
        d = await self._json('FundLiveData')
        d['issue'] = d.pop('SellNAVPerShare')
        d['cancel'] = d.pop('PurchaseNAVPerShare')
        d['date'] = _jdatetime.strptime(
            f"{d.pop('JalaliDate')} {d.pop('Time')}", '%Y/%m/%d %H:%M'
        ).togregorian()
        return d

    async def navps_history(self) -> _DataFrame:
        df = await self._json('NAVPerShare', df=True)
        df.columns = ['date', 'issue', 'cancel', 'statistical']
        df['date'] = df['date'].map(_j2g)
        return df

    async def nav_history(self) -> _DataFrame:
        df = await self._json('PureAsset', df=True)
        df.columns = ['nav', 'date', 'cancel_navps']
        df['date'] = df['date'].map(_j2g)
        return df

    async def portfolio_industries(self) -> _DataFrame:
        return await self._json('Industries', df=True)

    async def asset_allocation(self) -> dict:
        return await self._json('MixAsset')

    async def fund_profit(self) -> _DataFrame:
        j = await self._json('FundProfit')
        df = _DataFrame(j['data'])
        df['ProfitDate'] = df['ProfitDate'].apply(
            lambda i: _jdatetime.strptime(i, format='%Y/%m/%d').togregorian()
        )
        return df


class TadbirPardaz(BaseSite):
    # version = '9.2.0'

    async def live_navps(self) -> LiveNAVPS:
        d = await self._json('Fund/GetETFNAV')
        # the json is escaped twice, so it needs to be loaded again
        d = _loads(d)
        d['issue'] = _fa_int(d.pop('subNav'))
        d['cancel'] = _fa_int(d.pop('cancelNav'))

        date = d.pop('publishDate')
        try:
            date = _jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = _jdatetime.strptime(date, '%Y/%m/%d ')
        d['date'] = date.togregorian()

        return d

    async def navps_history(self) -> _DataFrame:
        j: list = await self._json('Chart/TotalNAV?type=getnavtotal')
        issue, statistical, cancel = [[d['y'] for d in i['List']] for i in j]
        date = [d['x'] for d in j[0]['List']]
        df = _DataFrame(
            {
                'date': date,
                'issue': issue,
                'cancel': cancel,
                'statistical': statistical,
            }
        )
        df['date'] = _to_datetime(df.date)
        return df


class LeveragedTadbirPardazLiveNAVPS(LiveNAVPS):
    BaseUnitsCancelNAV: int
    BaseUnitsTotalNetAssetValue: int
    BaseUnitsTotalSubscription: int
    SuperUnitsTotalSubscription: int
    SuperUnitsTotalNetAssetValue: int


class LeveragedTadbirPardaz(BaseSite):
    async def navps_history(self) -> _DataFrame:
        j: list = await self._json('Chart/TotalNAV?type=getnavtotal')

        frames = []
        for i, name in zip(
            j,
            (
                'normal_issue',
                'normal_statistical',
                'normal_cancel',
                'issue',
                'cancel',
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
        df.reset_index(inplace=True)
        return df

    async def live_navps(self) -> LeveragedTadbirPardazLiveNAVPS:
        d = await self._json('Fund/GetLeveragedNAV')
        # the json is escaped twice, so it needs to be loaded again
        d = _loads(d)

        date = d.pop('PublishDate')
        d = {k: _fa_int(v) for k, v in d.items()}

        try:
            date = _jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = _jdatetime.strptime(date, '%Y/%m/%d ')
        d['date'] = date.togregorian()

        d['issue'] = d.pop('SuperUnitsSubscriptionNAV')
        d['cancel'] = d.pop('SuperUnitsCancelNAV')
        return d


_DATASET_PATH = _Path(__file__).parent / 'dataset.csv'


def _make_site(row) -> BaseSite:
    type_str = row['site_type']
    site_class = globals()[type_str]
    return site_class(row['url'])


def load_dataset(*, site=True) -> _DataFrame:
    """Load dataset.csv as a DataFrame.

    If site is True, convert url and site_type columns to site object.
    """
    df = _read_csv(
        _DATASET_PATH,
        encoding='utf-8-sig',
        low_memory=False,
        memory_map=True,
        lineterminator='\n',
        dtype={
            'symbol': 'string',
            'name': 'string',
            'type': 'category',
            'insCode': 'string',
            'regNo': 'int64',
            'url': 'string',
            'site_type': 'category',
        },
    )
    if site:
        df['site'] = df[df['site_type'].notna()].apply(_make_site, axis=1)
        df.drop(columns=['url', 'site_type'], inplace=True)
    return df


async def _check_validity(site: BaseSite, retry=0) -> tuple[str, str] | None:
    try:
        await site.live_navps()
    except (
        _JSONDecodeError,
        _ClientConnectorError,
        _ServerTimeoutError,
        _ClientOSError,
        _TooManyRedirects,
        _TimeoutError,
        _ServerDisconnectedError,
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
    domains_to_be_checked = fipiran_df['domain']
    if known_domains is not None:
        selector = ~domains_to_be_checked.isin(known_domains)
        domains_to_be_checked = domains_to_be_checked[selector]
    else:
        selector = slice(None)

    _info(f'checking site types of {len(domains_to_be_checked)} domains')
    # there will be a lot of redirection warnings, let's silent them
    with _w.catch_warnings():
        _w.filterwarnings(
            'ignore', category=UserWarning, module='aiohutils.session'
        )
        list_of_tuples = await _gather(
            *[_url_type(d) for d in domains_to_be_checked]
        )

    url, site_type = zip(*list_of_tuples)
    fipiran_df.loc[selector, ['url', 'site_type']] = _DataFrame(
        {'url': url, 'site_type': site_type}, index=domains_to_be_checked.index
    )


async def _add_ins_code(new_items: _DataFrame) -> None:
    names_without_code = new_items[new_items['insCode'].isna()].name
    if names_without_code.empty:
        return
    import tsetmc.instruments

    search = tsetmc.instruments.search
    _info('searching names on tsetmc to find their insCode')
    results = await _gather(*[search(name) for name in names_without_code])
    ins_codes = [(None if len(r) != 1 else r[0]['insCode']) for r in results]
    new_items['insCode'] = ins_codes


async def _fipiran_data(ds) -> _DataFrame:
    import fipiran.funds

    _info('await fipiran.funds.funds()')
    fipiran_df = await fipiran.funds.funds()

    reg_not_in_fipiran = ds[~ds['regNo'].isin(fipiran_df['regNo'])]
    if not reg_not_in_fipiran.empty:
        _warning('some dataset rows were not found on fipiran')

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
            'smallSymbolName': 'symbol',
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
    df.drop(columns='l30', inplace=True)
    df.columns = ['insCode', 'symbol']
    df.set_index('insCode', inplace=True)

    return df


def _add_new_items_to_ds(new_items: _DataFrame, ds: _DataFrame) -> _DataFrame:
    if new_items.empty:
        return ds
    new_with_code = new_items[new_items['insCode'].notna()]
    if not new_with_code.empty:
        ds = _concat([ds, new_with_code.drop(columns=['domain'])])
    else:
        _info('new_with_code is empty!')
    return ds


async def update_dataset(*, check_existing_sites=False) -> _DataFrame:
    """Update dataset and return newly found that could not be added."""
    ds = load_dataset(site=False)
    fipiran_df = await _fipiran_data(ds)
    await _add_url_and_type(
        fipiran_df,
        known_domains=None
        if check_existing_sites
        else ds['url'].str.extract('//(.*)/')[0],
    )

    # to update existing urls and names
    ds.set_index('regNo', inplace=True)
    ds['domain'] = None
    ds.update(fipiran_df.set_index('regNo'))
    # use domain as URL for those who do not have any URL
    ds.loc[ds['url'].isna(), 'url'] = 'http://' + ds['domain'] + '/'
    ds.drop(columns=['domain'], inplace=True)
    ds.reset_index(inplace=True)

    reg_existing_in_ds = fipiran_df['regNo'].isin(ds['regNo'])
    new_items = fipiran_df[~reg_existing_in_ds]

    tsetmc_df = await _tsetmc_dataset()
    await _add_ins_code(new_items)
    ds = _add_new_items_to_ds(new_items, ds)

    # update all data, old or new, using tsetmc_df
    ds.set_index('insCode', inplace=True)
    ds.update(tsetmc_df)
    ds.reset_index(inplace=True)

    ds[
        [  # resort columns (order was changed by the ds.reset_index)
            'symbol',
            'name',
            'type',
            'insCode',
            'regNo',
            'url',
            'site_type',
        ]
    ].sort_values('symbol').to_csv(
        _DATASET_PATH, lineterminator='\n', encoding='utf-8-sig', index=False
    )

    return new_items[new_items['insCode'].isna()]


async def _check_live_site(site: BaseSite):
    if site != site:  # na
        return

    try:
        navps = await site.live_navps()
    except Exception as e:
        _error(f'exception during checking of {site}: {e}')
    else:
        assert type(navps['issue']) is int


async def check_dataset(live=False):
    ds = load_dataset(site=False)
    assert ds['symbol'].is_unique
    assert ds['name'].is_unique
    assert ds['type'].unique().isin(_ETF_TYPES.values()).all()
    assert ds['insCode'].is_unique
    assert ds['regNo'].is_unique

    if not live:
        return

    ds['site'] = ds[ds['site_type'].notna()].apply(_make_site, axis=1)
    iranetf.SSL = False  # many sites fail ssl verification
    coros = ds['site'].apply(_check_live_site)
    await _gather(*coros)

    if not (no_site := ds[ds['site'].isna()]).empty:
        _warning(
            f'some dataset entries have no associated site:\n{no_site.symbol}'
        )
