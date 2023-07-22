from abc import ABC as _ABC, abstractmethod as _abstractmethod
from asyncio import TimeoutError as _TimeoutError, gather as _gather
from json import JSONDecodeError as _JSONDecodeError, loads as _loads
from logging import error as _error, info as _info, warning as _warning
from pathlib import Path as _Path
from typing import TypedDict as _TypedDict

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

_ETF_TYPES = {  # numbers are according to fipiran
    6: 'Stock', 4: 'Fixed', 7: 'Mixed',
    5: 'Commodity', 17: 'FOF', 18: 'REIT',
}

class _LiveNAV(_TypedDict, total=True):
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

    async def _json(self, path: str, df: bool = False) -> list | dict | str | _DataFrame:
        r = await _get(self.url + path)
        self.last_response = r
        content = await r.read()
        j = _loads(content)
        if df is True:
            return _DataFrame(j, copy=False)
        return j

    @_abstractmethod
    async def live_navps(self) -> _LiveNAV:
        ...

    @_abstractmethod
    async def navps_history(self) -> _DataFrame:
        ...


class MabnaDP(BaseSite):

    async def _json(
        self, path: str, df: bool = False
    ) -> list | dict | _DataFrame:
        return await super()._json(f'api/v1/overall/{path}', df)

    async def live_navps(self) -> _LiveNAV:
        j = await self._json('navetf.json')
        j['date'] = _jdatetime.strptime(j['date_time'], '%H:%M %Y/%m/%d').togregorian()
        j['issue'] = int(j.pop('purchase_price').replace(',', ''))
        j['cancel'] = int(j.pop('redemption_price').replace(',', ''))
        return j

    async def navps_history(self) -> _DataFrame:
        j = await self._json('navps.json')
        df = _DataFrame(j[0]['values'])
        df['date'] = df['date'].astype(str).apply(lambda i: _jdatetime.strptime(i, format='%Y%m%d000000').togregorian())
        df['issue'] = df.pop('purchase_price')
        df['cancel'] = df.pop('redeption_price')
        df['statistical'] = df.pop('statistical_value')
        return df


class RayanHamafza(BaseSite):

    async def _json(
        self, path: str, df: bool = False
    ) -> list | dict | _DataFrame:
        return await super()._json(f'Data/{path}', df)

    async def live_navps(self) -> _LiveNAV:
        d = await self._json('FundLiveData')
        d['issue'] = d.pop('SellNAVPerShare')
        d['cancel'] = d.pop('PurchaseNAVPerShare')
        d['date'] = _jdatetime.strptime(
            f"{d.pop('JalaliDate')} {d.pop('Time')}",
            '%Y/%m/%d %H:%M'
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

    async def live_navps(self) -> _LiveNAV:
        d = await self._json('Fund/GetETFNAV')
        # the json is escaped twice, so it needs to be loaded again
        d = _loads(d)
        d['issue'] = int(d.pop('subNav').replace(',', ''))
        d['cancel'] = int(d.pop('cancelNav').replace(',', ''))

        date = d.pop('publishDate')
        try:
            date = _jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = _jdatetime.strptime(date, '%Y/%m/%d ')
        d['date'] = date.togregorian()

        return d

    async def navps_history(self) -> _DataFrame:
        j : list = await self._json('Chart/TotalNAV?type=getnavtotal')
        issue, statistical, cancel = [[d['y'] for d in i['List']] for i in j]
        date = [d['x'] for d in j[0]['List']]
        df = _DataFrame({
            'date': date,
            'issue': issue,
            'cancel': cancel,
            'statistical': statistical,
        })
        df['date'] = _to_datetime(df.date)
        return df


class LeveragedTadbirPardaz(BaseSite):

    async def navps_history(self) -> _DataFrame:
        j: list = await self._json('Chart/TotalNAV?type=getnavtotal')

        frames = []
        for i, name in zip(j, ('normal_issue', 'normal_statistical', 'normal_cancel', 'issue', 'cancel', 'normal')):
            df = _DataFrame(i['List']).drop(columns='name')
            df['date'] = _to_datetime(df['x'])
            df.drop(columns='x', inplace=True)
            df.rename(columns={'y': name}, inplace=True)
            df.set_index('date', inplace=True)
            frames.append(df)

        df = _concat(frames, axis=1)
        df.reset_index(inplace=True)
        return df

    async def live_navps(self):
        d = await self._json('Fund/GetLeveragedNAV')
        # the json is escaped twice, so it needs to be loaded again
        d = _loads(d)
        d['issue'] = int(d.pop('SuperUnitsSubscriptionNAV').replace(',', ''))
        d['cancel'] = int(d.pop('SuperUnitsCancelNAV').replace(',', ''))

        date = d.pop('PublishDate')
        try:
            date = _jdatetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        except ValueError:
            date = _jdatetime.strptime(date, '%Y/%m/%d ')
        d['date'] = date.togregorian()

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
        _DATASET_PATH, encoding='utf-8-sig', low_memory=False, memory_map=True,
        lineterminator='\n',
        dtype={
            'symbol': 'string',
            'name': 'string',
            'type': 'category',
            'tsetmc_id': 'string',
            'fipiran_id': 'int64',
            'url': 'string',
            'site_type': 'category',
        }
    )
    if site:
        df['site'] = df[df.site_type.notna()].apply(_make_site, axis=1)
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


async def _url_type_columns(domains):
    list_of_tuples = await _gather(*[_url_type(d) for d in domains])
    return zip(*list_of_tuples)


async def _inscodes(names_without_tsetmc_id) -> _Series:
    import tsetmc.instruments
    search = tsetmc.instruments.search
    async with tsetmc.Session():
        results = await _gather(*[
            search(name) for name in names_without_tsetmc_id
        ])
    results = [(None if len(r) != 1 else r.iat[0, 2]) for r in results]
    return _Series(results, index=names_without_tsetmc_id.index, dtype='Int64')


async def _fipiran_data(ds):
    import fipiran.funds
    _info('await fipiran.funds.funds()')
    async with fipiran.Session():
        fipiran_df = await fipiran.funds.funds()

    dataset_ids_not_on_fipiran = ds[~ds.fipiran_id.isin(fipiran_df.regNo)]
    if not dataset_ids_not_on_fipiran.empty:
        _warning('some dataset rows were not found on fipiran')

    df = fipiran_df[
        (fipiran_df['typeOfInvest'] == 'Negotiable')
        # 11: 'Market Maker', 12: 'VC', 13: 'Project', 14: 'Land and building',
        # 16: 'PE'
        & ~(fipiran_df['fundType'].isin((11, 12, 13, 14, 16)))
        & fipiran_df['isCompleted']
    ]

    df = df[['regNo', 'name', 'fundType', 'websiteAddress']]

    df.rename(
        columns={
            'regNo': 'fipiran_id',
            'fundType': 'type',
            'websiteAddress': 'domain',
        }, copy=False, inplace=True, errors='raise'
    )

    df.type.replace(_ETF_TYPES, inplace=True)

    return df


async def _tsetmc_dataset() -> _DataFrame:
    from tsetmc import Session
    from tsetmc.dataset import LazyDS, update

    _info('await tsetmc.dataset.update()')
    async with Session():
        await update()

    df = LazyDS.df
    df.drop(columns='l30', inplace=True)
    df.columns = ['tsetmc_id', 'symbol']
    df.set_index('tsetmc_id', inplace=True)

    return df


async def update_dataset() -> _DataFrame:
    """Update dataset and return newly found that could not be added."""
    ds = load_dataset(site=False)
    fipiran_df = await _fipiran_data(ds)

    _info('await _url_type_columns(...)')
    url, site_type = await _url_type_columns(fipiran_df['domain'])
    fipiran_df['url'] = url
    fipiran_df['site_type'] = site_type

    # to update existing urls and names
    ds.set_index('fipiran_id', inplace=True)
    ds.update(fipiran_df.set_index('fipiran_id'))
    ds.reset_index(inplace=True)

    fipiran_ids_existing_in_ds = fipiran_df.fipiran_id.isin(ds.fipiran_id)

    new_fipiran_df = fipiran_df[~fipiran_ids_existing_in_ds].copy()

    _info('await _inscodes(...)')
    new_fipiran_df['tsetmc_id'] = await _inscodes(new_fipiran_df.name)

    new_with_tsetmcid = new_fipiran_df[new_fipiran_df.tsetmc_id.notna()]

    tsetmc_df = await _tsetmc_dataset()

    # update symbol
    ds.set_index('tsetmc_id', inplace=True)
    ds.update(tsetmc_df)

    if not new_with_tsetmcid.empty:
        new_with_tsetmcid = new_with_tsetmcid.merge(
            tsetmc_df, 'left', on='tsetmc_id'
        )
        ds = _concat([ds, new_with_tsetmcid])

    ds.reset_index(inplace=True)
    ds[[  # resort columns (order was changed by the ds.reset_index)
        'symbol', 'name', 'type', 'tsetmc_id', 'fipiran_id', 'url', 'site_type'
    ]].sort_values('symbol').to_csv(
        _DATASET_PATH,
        lineterminator='\n',
        encoding='utf-8-sig',
        index=False
    )

    return new_fipiran_df[new_fipiran_df.tsetmc_id.isna()]


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
    assert ds.symbol.is_unique
    assert ds.name.is_unique
    assert ds.type.unique().isin(_ETF_TYPES.values()).all()
    assert ds.tsetmc_id.is_unique
    assert ds.fipiran_id.is_unique

    if not live:
        return

    ds['site'] = ds[ds.site_type.notna()].apply(_make_site, axis=1)
    iranetf.SSL = False  # many sites fail ssl verification
    coros = ds.site.apply(_check_live_site)
    await _gather(*coros)

    if not (no_site := ds[ds.site.isna()]).empty:
        _warning(
            f'some dataset entries have no associated site:\n{no_site.symbol}'
        )
