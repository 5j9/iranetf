from pathlib import Path as _Path
from typing import TypedDict as _TypedDict
from json import loads as _loads, JSONDecodeError as _JSONDecodeError
from asyncio import gather as _gather


from pandas import to_datetime as _to_datetime, DataFrame as _DataFrame, \
    read_csv as _read_csv
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
        self.url = url if url[-1] == '/' else url + '/'

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


_CSV_PATH = f'{_Path(__file__).parent}/dataset.csv'


def _load_known_sites() -> _DataFrame:
    df = _read_csv(
        _CSV_PATH, encoding='utf-8-sig', low_memory=False, memory_map=True,
        lineterminator='\n'
    )
    return df


async def _site_type_url(url: str) -> tuple[str, str]:
    np_url = url[url.find('://'):]
    for protocol in ('https', 'http'):
        for SiteType in (RayanHamafza, TadbirPardaz):
            site = SiteType(protocol + np_url)
            try:
                await site.live_navps()
                url = site.last_response.url
                return SiteType.__name__, f'{url.scheme}://{url.host}/'
            except (_JSONDecodeError, _ClientConnectorError, _ServerTimeoutError):
                continue


async def _update_known_sites():
    from iranetf.ravest import funds
    fdf = await funds()
    df = fdf[['Symbol', 'TsetmcId', 'Url']].copy()
    df.columns = df.columns.str.lower()
    df['url'] = df.url.str.rstrip('/') + '/'
    site_url = await _gather(*[_site_type_url(url) for url in df.url])
    site_url_df = _DataFrame(site_url, columns=['site_type', 'url'])
    site_url_df['url'].fillna(df['url'], inplace=True)
    df[['site_type', 'url']] = site_url_df

    df.to_csv(
        _CSV_PATH, line_terminator='\n', encoding='utf-8-sig', index=False)
