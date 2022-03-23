from typing import TypedDict as _TypedDict

from pandas import to_datetime as _to_datetime

from iranetf import _session_get, _loads, _DataFrame, _jdatetime, _j2g, _datetime


class _LiveNAV(_TypedDict, total=True):
    issue: int
    cancel: int
    date: _datetime


class _BaseSite:

    __slots__ = 'url'

    def __init__(self, url: str):
        self.url = url

    async def _json(self, path: str, df: bool = False) -> list | dict | str | _DataFrame:
        c = await _session_get(self.url + path)
        j = _loads(c)
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
        d['date'] = _jdatetime.strptime(
            d.pop('publishDate'),
            '%Y/%m/%d %H:%M:%S'
        ).togregorian()
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
