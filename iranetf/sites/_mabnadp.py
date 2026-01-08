from asyncio import gather
from datetime import datetime
from json import loads
from re import search
from typing import Any

from jdatetime import datetime as jdatetime
from pandas import DataFrame

from iranetf.sites._lib import (
    BaseSite,
    LiveNAVPS,
    _get,
    _read,
    comma_int,
)


class MabnaDPBase(BaseSite):
    async def _home_info(self):
        d = {}
        html = await self._home()
        m = search(r'(\d+)\s*نزد سازمان بورس', html)
        if m:
            d['seo_reg_no'] = m[1]
        return d


class MabnaDP(MabnaDPBase):
    async def _json(self, path, **kwa) -> Any:
        return await super()._json(f'api/v1/overall/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        j: dict = await self._json('navetf.json')
        j['date'] = jdatetime.strptime(
            j['date_time'], '%H:%M %Y/%m/%d'
        ).togregorian()
        j['creation'] = comma_int(j.pop('purchase_price'))
        j['redemption'] = comma_int(j.pop('redemption_price'))
        return j  # type: ignore

    async def navps_history(self) -> DataFrame:
        j: list[dict] = await self._json('navps.json')
        df = DataFrame(j[0]['values'])
        df['date'] = (
            df['date']
            .astype(str)
            .apply(
                lambda i: jdatetime.strptime(
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


class MabnaDP2(MabnaDPBase):
    # uses api/v2/ path instead of api/v1/
    async def _json(self, path, **kwa) -> Any:
        params: dict | None = kwa.get('params')
        if params is None:
            kwa['params'] = {'portfolio_id': '1'}
        else:
            params.setdefault('portfolio_id', '1')

        return await super()._json(f'api/v2/public/fund/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        data = (await self._json('etf/navps/latest'))['data']
        data['date'] = datetime.fromisoformat(data.pop('date_time')).replace(
            tzinfo=None
        )
        data['creation'] = data.pop('purchase_price')
        data['redemption'] = data.pop('redemption_price')
        return data

    @staticmethod
    def _chart_df(j) -> DataFrame:
        df = DataFrame(j['data'])
        df['date_time'] = df['date_time'].astype('datetime64[ns, UTC+03:30]')  # type: ignore
        df.set_index(
            df['date_time'].dt.normalize().dt.tz_localize(None),  # pyright: ignore[reportAttributeAccessIssue]
            inplace=True,
        )
        df.index.name = 'date'
        return df

    async def navps_history(self) -> DataFrame:
        j = await self._json('chart')
        df = self._chart_df(j)
        df.rename(
            columns={
                'redemption_price': 'redemption',
                'statistical_value': 'statistical',
                'purchase_price': 'creation',
            },
            inplace=True,
        )
        return df

    async def assets_history(self):
        j = await self._json('navps/assets-chart')
        return self._chart_df(j)

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
            '__REACT_QUERY_STATE__': loads(
                loads(
                    html.rpartition('window.__REACT_QUERY_STATE__ = ')[
                        2
                    ].partition(';')[0]
                )
            ),
            '__REACT_REDUX_STATE__': loads(
                loads(
                    html.rpartition('window.__REACT_REDUX_STATE__ = ')[
                        2
                    ].partition(';')[0]
                )
            ),
            '__ENV__': loads(
                loads(
                    html.rpartition('window.__ENV__ = ')[2].partition('\n')[0]
                )
            ),
        }

    async def leverage(self) -> float:
        data, cache = await gather(self.home_data(), self.cache())
        data = data['__REACT_QUERY_STATE__']['queries'][9]['state']['data'][
            '1'
        ]
        return (
            1.0
            + data['commonUnitRedemptionValueAmount']
            / data['preferredUnitRedemptionValueAmount']
        ) * (1.0 - cache)
