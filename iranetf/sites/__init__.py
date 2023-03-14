from pathlib import Path as _Path
from typing import TypedDict as _TypedDict
from json import loads as _loads, JSONDecodeError as _JSONDecodeError
from asyncio import gather as _gather
from logging import warning
from abc import ABC as _ABC, abstractmethod as _abstractmethod

from pandas import to_datetime as _to_datetime, DataFrame as _DataFrame, \
    read_csv as _read_csv, Series as _Series, concat as _concat
from aiohttp import ClientConnectorError as _ClientConnectorError, \
    ServerTimeoutError as _ServerTimeoutError

from iranetf import _get, _jdatetime, _j2g, _datetime


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
            'tsetmc_id': 'Int64',
            'fipiran_id': 'int64',
            'url': 'string',
            'site_type': 'category',
        }
    )
    if site:
        df['site'] = df[df.site_type.notna()].apply(_make_site, axis=1)
        df.drop(columns=['url', 'site_type'], inplace=True)
    return df


async def _url_type(domain: str) -> tuple:
    for protocol in ('https', 'http'):
        for SiteType in (RayanHamafza, TadbirPardaz):
            site = SiteType(f'{protocol}://{domain}/')
            try:
                await site.live_navps()
                domain = site.last_response.url
                return f'{domain.scheme}://{domain.host}/', SiteType.__name__
            except (_JSONDecodeError, _ClientConnectorError, _ServerTimeoutError):
                continue
    return f'http://{domain}/', None


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
    async with fipiran.Session():
        fipiran_df = await fipiran.funds.funds()

    dataset_ids_not_on_fiprian = ds[~ds.fipiran_id.isin(fipiran_df.regNo)]
    if not dataset_ids_not_on_fiprian.empty:
        warning('some dataset rows were not found on fipiran')

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

    df.type.replace(
        {
            6: 'Stock', 4: 'Fixed', 7: 'Mixed',
            5: 'Commodity', 17: 'FOF'
        }, inplace=True
    )

    return df


async def _tsetmc_dataset() -> _DataFrame:
    import tsetmc.dataset

    async with tsetmc.Session():
        await tsetmc.dataset.update()

    # noinspection PyProtectedMember
    return _DataFrame(
        tsetmc.dataset._L18S.values(),
        columns=['tsetmc_id', 'symbol', 'l30'],
        copy=False,
    ).drop(columns='l30')


async def _update_dataset():
    ds = load_dataset(site=False)
    df = await _fipiran_data(ds)

    url, site_type = await _url_type_columns(df['domain'])
    df['url'] = url
    df['site_type'] = site_type

    new_ids = df[~df.fipiran_id.isin(ds.fipiran_id)].copy()
    new_ids['tsetmc_id'] = await _inscodes(new_ids.name)

    new_ids_with_tsetmcid = new_ids[new_ids.tsetmc_id.notna()]

    tsetmc_df = await _tsetmc_dataset()
    new_ids_with_tsetmcid = new_ids_with_tsetmcid.merge(
        tsetmc_df, 'left', on='tsetmc_id'
    )
    new_ids_with_tsetmcid = new_ids_with_tsetmcid[[
        'symbol', 'name', 'type', 'tsetmc_id', 'fipiran_id', 'url', 'site_type'
    ]]

    new_dataset = _concat([ds, new_ids_with_tsetmcid]).sort_values('symbol')
    new_dataset.to_csv(
        _DATASET_PATH,
        lineterminator='\n',
        encoding='utf-8-sig',
        index=False
    )
