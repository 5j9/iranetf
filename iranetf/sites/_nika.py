from asyncio import gather, run
from datetime import date, timedelta
from json import JSONDecodeError, loads
from re import DOTALL, MULTILINE, compile as rc
from typing import Any

from polars import LazyFrame, col

from iranetf.sites._lib import (
    BaseSite,
    LiveNAVPS,
    _get,
    reg_no_from_home_info,
)

finditer_pushes = rc(
    r'<script>self\.__next_f\.push\((.*?)\)</script>',
    DOTALL,
).finditer
finditer_key_values = rc(r'^([0-9a-z]+):(.*)$', MULTILINE).finditer


def find_value(obj: Any, key: str) -> Any | None:
    stack = [obj]

    while stack:
        obj = stack.pop()

        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            stack.extend(reversed(list(obj.values())))

        elif isinstance(obj, list):
            stack.extend(reversed(obj))

    return None


def find_list(
    obj: Any, key: str, required_keys: set[str]
) -> list[dict[str, Any]] | None:
    stack = [obj]

    while stack:
        obj = stack.pop()

        if isinstance(obj, dict):
            value = obj.get(key)
            if isinstance(value, list) and any(
                isinstance(item, dict) and required_keys <= item.keys()
                for item in value
            ):
                return value

            stack.extend(reversed(list(obj.values())))

        elif isinstance(obj, list):
            stack.extend(reversed(obj))

    return None


def decode(value: str) -> Any:
    try:
        return loads(value)
    except JSONDecodeError:
        return value


def to_nextjs_flight(html: str) -> dict:
    # Next.js: the web framework.
    # Flight: the name of a data protocol used by React Server Components
    rows = {}

    for match in finditer_pushes(html):
        chunk = loads(match[1])[1]
        rows.update((m[1], m[2]) for m in finditer_key_values(chunk))

    return {key: decode(value) for key, value in rows.items()}


class Nika(BaseSite):
    __slots__ = ('_api_cache', '_pam_code_cache')
    _api_cache: str

    def __init__(self, url: str, portfolio_id: str = ''):
        super().__init__(url, portfolio_id)

    async def _api(self) -> str:
        try:
            return self._api_cache
        except AttributeError:
            pass
        home_info = await self.home_info()
        api = self._api_cache = (
            f'https://{find_value(home_info, "apiPath")}/voak/oak/v1/api/public/'
        )
        return api

    async def _home_info(self):
        d = {}
        html = await self._home()
        d['nextjs_flight'] = nextjs_flight = to_nextjs_flight(html)
        seo_reg_no = find_value(nextjs_flight, 'seoRegisterNumber')
        if seo_reg_no is not None:
            d['seo_reg_no'] = seo_reg_no
        return d

    reg_no = reg_no_from_home_info

    async def live_navps(self) -> LiveNAVPS:
        raise NotImplementedError('Nika sites do provide live NAVPS data')

    async def navps_history(self) -> LazyFrame:
        data = find_list(
            await self.home_info(),
            'data',
            {'issuanceNav', 'date', 'redemptionNav', 'nominalNav'},
        )
        return (
            LazyFrame(data)
            .rename(
                {
                    'redemptionNav': 'redemption',
                    'nominalNav': 'statistical',
                    'issuanceNav': 'creation',
                }
            )
            .with_columns(col('date').str.to_date())
        )

    async def assets_history(self) -> LazyFrame:
        items = find_list(
            await self.home_info(),
            'items',
            {'date', 'totalUnit', 'totalIssuanceUnit', 'totalRedemptionUnit'},
        )
        return (
            LazyFrame(items)
            .rename(
                {
                    'redemptionNav': 'redemption',
                    'nominalNAV': 'statistical',
                    'issuanceNav': 'creation',
                }
            )
            .with_columns(col('date').str.to_date())
        )

    _aa_keys = {
        'onDate',
        'top5Stock',
        'otherStock',
        'bond',
        'cd',
        'cashAndBank',
        'mutualFund',
        'goldAndBar',
        'otherAssets',
        'reportDate',
        'top5StockPercent',
        'otherStockPercent',
        'bondPercent',
        'cdPercent',  # Certificate of Deposit?
        'cashAndBankPercent',
        'mutualFundPercent',
        'otherAssetsPercent',
        'goldAndBarPercent',
    }

    @property
    async def _pam_code(self):
        try:
            return self._pam_code_cache
        except AttributeError:
            pass
        home_info = await self.home_info()
        result = self._pam_code_cache = find_value(
            home_info['nextjs_flight'], 'pamCode'
        )
        return result

    async def daily_asset_percentage(
        self, from_date: date, to_date: date
    ) -> Any:
        return await self._json(
            'portfolio-composition/daily-asset-percentage',
            params={
                'pamCode': await self._pam_code,
                'fromDate': from_date.isoformat(),
                'toDate': to_date.isoformat(),
            },
        )

    async def asset_allocation(self) -> dict[str, Any]:
        today = date.today()
        j = await self.daily_asset_percentage(
            from_date=today - timedelta(30), to_date=today
        )
        last_record = j[1]
        self._check_aa_keys(last_record)
        return last_record

    async def cash(self) -> float:
        aa = await self.asset_allocation()
        g = aa.get
        return sum(g(k, 0.0) for k in ('bondPercent', 'cashAndBankPercent'))

    async def home_data(self) -> dict:
        html = await (await _get(self.url)).text()
        return {
            '__REACT_QUERY_STATE__': loads(
                loads(
                    html.rpartition('window.__REACT_QUERY_STATE__ = ')[
                        2
                    ].partition(';\n')[0]
                )
            ),
            '__REACT_REDUX_STATE__': loads(
                loads(
                    html.rpartition('window.__REACT_REDUX_STATE__ = ')[
                        2
                    ].partition(';\n')[0]
                )
            ),
            '__ENV__': loads(
                loads(
                    html.rpartition('window.__ENV__ = ')[2].partition('\n')[0]
                )
            ),
        }

    async def leverage(self) -> float:
        data, cache = await gather(self.home_data(), self.cash())
        genera_data = data['__REACT_REDUX_STATE__']['general']['data']
        if not genera_data['isLeverage']:
            return 1.0 - cache
        data: dict = data['__REACT_QUERY_STATE__']['queries'][9]['state'][
            'data'
        ]
        first = data[next(iter(data))]
        return (
            1.0
            + first['commonUnitRedemptionValueAmount']
            / first['preferredUnitRedemptionValueAmount']
        ) * (1.0 - cache)

    async def portfolios(self) -> dict[str, str]:
        portfolios = await self._json('portfolios')
        return {p['id']: p['name'] for p in portfolios['data']}


if __name__ == '__main__':
    s = Nika('https://ayar.mofidfund.com/')
    run(s.home_info())
