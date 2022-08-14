from pathlib import Path as _Path
from typing import TypedDict as _TypedDict
from json import loads as _loads, JSONDecodeError as _JSONDecodeError
from asyncio import gather as _gather

from pandas import to_datetime as _to_datetime, DataFrame as _DataFrame, \
    read_csv as _read_csv, Series as _Series
from aiohttp import ClientConnectorError as _ClientConnectorError, \
    ServerTimeoutError as _ServerTimeoutError

from iranetf import _get, _jdatetime, _j2g, _datetime


class _LiveNAV(_TypedDict, total=True):
    issue: int
    cancel: int
    date: _datetime


class _BaseSite:

    __slots__ = 'url', 'last_response'

    def __init__(self, url: str):
        assert url[-1] == '/', 'the url must end with `/`'
        self.url = url

    async def _json(self, path: str, df: bool = False) -> list | dict | str | _DataFrame:
        r = await _get(self.url + path)
        self.last_response = r
        j = _loads(await r.read())
        if df is True:
            return _DataFrame(j, copy=False)
        return j


class RayanHamafza(_BaseSite):

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


class TadbirPardaz(_BaseSite):

    # version = '9.2.0'

    async def live_navps(self) -> _LiveNAV:
        d = await self._json('Fund/GetETFNAV')
        # the json is escaped twice, so it need to be loaded again
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


_DATASET_PATH = _Path(__file__).parent / 'dataset.csv'


def _load_known_sites() -> _DataFrame:
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


async def _update_dataset_using_ravest():
    import iranetf.ravest
    ravest_df = await iranetf.ravest.funds()

    df1 = ravest_df[['Symbol', 'TsetmcId', 'Url']]
    df1.columns = df1.columns.str.lower()

    domains = df1.url.str.extract(r'/([^/]+)')

    url, site_type = _url_type_columns(domains)
    df1['url'].update(url)
    df1['site_type'] = site_type

    df1['type'] = ravest_df.FundType.replace(
        {0: 'Stock', 1: 'Fixed', 2: 'Mixed', 3: 'PE', 4: 'Commodity', 5: 'VC'},
    )

    df = df1.reindex(columns=['symbol', 'tsetmcid', 'type', 'url', 'site_type'])
    df.to_csv(
        _DATASET_PATH, line_terminator='\n', encoding='utf-8-sig', index=False)


async def _inscodes(names_without_tsetmc_id) -> _Series:
    import tsetmc.instruments
    search = tsetmc.instruments.search
    async with tsetmc.Session():
        results = await _gather(*[
            search(name) for name in names_without_tsetmc_id
        ])
    results = [(None if len(r) != 1 else r.iat[0, 2]) for r in results]
    return _Series(results, index=names_without_tsetmc_id.index, dtype='Int64')


async def _add_ravest_tsetmc_id(df: _DataFrame) -> _DataFrame:
    import iranetf.ravest
    ravest_df = await iranetf.ravest.funds()
    ravest_df['domain'] = ravest_df.Url.str.extract(r'/([^/]+)')
    # early conversion to Int64 prevents data loss due to conversion to floats
    ravest_df['tsetmc_id'] = ravest_df.TsetmcId.astype('Int64')
    merged_df = df.merge(ravest_df[['domain', 'tsetmc_id']], 'left', on='domain')
    return merged_df


async def _update_dataset():
    import fipiran.funds
    async with fipiran.Session():
        fipiran_df = await fipiran.funds.funds()

    df = fipiran_df[
        (fipiran_df['typeOfInvest'] == 'Negotiable')
        # 11: 'Market Maker', 12: 'VC', 13: 'Project', 14: 'Land and building',
        # 16: 'PE'
        & ~(fipiran_df['fundType'].isin((11, 12, 13, 14, 16)))
        & fipiran_df['isCompleted']
    ]

    df = df[['regNo', 'name', 'fundType', 'websiteAddress']]

    df.rename(columns={
        'regNo': 'fipiran_id',
        'fundType': 'type',
        'websiteAddress': 'domain',
    }, copy=False, inplace=True, errors='raise')

    df.type.replace({
        6: 'Stock', 4: 'Fixed', 7: 'Mixed',
        5: 'Commodity', 17: 'FOF'
    }, inplace=True)

    url, site_type = await _url_type_columns(df['domain'])
    df['url'] = url
    df['site_type'] = site_type

    df = await _add_ravest_tsetmc_id(df)
    names_without_tsetmc_id = df[df['tsetmc_id'].isna()].name
    df['tsetmc_id'].update(await _inscodes(names_without_tsetmc_id))

    import tsetmc.dataset

    async with tsetmc.Session():
        await tsetmc.dataset.update()

    # noinspection PyProtectedMember
    tsetmc_df = _DataFrame(
        tsetmc.dataset._L18S.values(),
        columns=['tsetmc_id', 'symbol', 'l30'],
        copy=False,
    ).drop(columns='l30')

    df_notna_tsetmc_id = df[df.tsetmc_id.notna()]
    symbols = df_notna_tsetmc_id.merge(
        tsetmc_df, 'left', on='tsetmc_id'
    ).symbol
    symbols.index = df_notna_tsetmc_id.index
    df['symbol'] = symbols

    df = df[['symbol', 'name', 'type', 'tsetmc_id', 'fipiran_id', 'url', 'site_type']].sort_values('symbol')
    df.to_csv(
        _DATASET_PATH, line_terminator='\n', encoding='utf-8-sig', index=False)
