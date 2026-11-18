from asyncio import run
from datetime import date, datetime
from json import JSONDecodeError, loads
from re import DOTALL, MULTILINE, compile as rc
from typing import Any

from polars import Int64, LazyFrame, col

from iranetf.sites._lib import (
    BaseSite,
    LiveNAVPS,
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
    __slots__ = ('_hive_api', '_oak_api', '_pam_code_cache')
    _oak_api: str
    _hive_api: str

    def __init__(self, url: str, portfolio_id: str = ''):
        super().__init__(url, portfolio_id)

    async def _api(self, api_name: str | None = None) -> str:
        cache_key = f'_{api_name}_api'
        try:
            return getattr(self, cache_key)
        except AttributeError:
            pass

        if api_name is None:  # only Literal['oak', 'hive'] are expected
            api_name = 'oak'

        home_info = await self.home_info()
        url = f'https://{find_value(home_info, "apiPath")}/{find_value(home_info, f"{api_name}Path")}/'
        setattr(self, cache_key, url)
        return url

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
        pid = self.portfolio_id
        j = await self._json(
            f'{pid}/{pid}/Nav/GetNavStatistics',
            api_name='hive',
        )
        return {
            **j,
            'creation': int(j['etfIssuanceNav']),
            'redemption': int(j['etfRedemptionNav']),
            'datetime': datetime.fromisoformat(f'{j["etfDate"]} {j["time"]}'),
        }

    async def navps_history(
        self,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
    ) -> LazyFrame:
        if from_date is None:
            from_date = datetime.min
        if to_date is None:
            to_date = datetime.max

        pid = self.portfolio_id
        page_size = 5000
        page_number = 0
        all_items: list[dict] = []

        while True:
            j = await self._json(
                f'{pid}/{pid}/Nav',
                api_name='hive',
                params={
                    'FromDate': from_date.strftime('%m/%d/%Y'),
                    'ToDate': to_date.strftime('%m/%d/%Y'),
                    'PageNumber': page_number,
                    'PageSize': page_size,
                },
            )

            items = j.get('items') or []
            all_items += items

            if not j.get('hasNextPage', False) or not items:
                break
            page_number += 1
        return (
            LazyFrame(all_items)
            .rename(
                {
                    'redemptionNav': 'redemption',
                    'nominalNAV': 'statistical',
                    'issuanceNav': 'creation',
                }
            )
            .with_columns(
                col('date').str.to_date(),
                col('totalUnit').cast(Int64),
                col('totalIssuanceUnit').cast(Int64),
                col('totalRedemptionUnit').cast(Int64),
                col('issuanceUnitToday').cast(Int64),
                col('redemptionUnitToday').cast(Int64),
                col('remainingUnitsCount').cast(Int64),
            )
        )

    @property
    async def _pam_code(self) -> str:
        try:
            return self._pam_code_cache
        except AttributeError:
            pass
        available_funds = await self._available_funds()
        for record in available_funds:
            if record['basketId'] == self.portfolio_id:
                pam_code = self._pam_code_cache = record['pamCode']
                return pam_code

        raise ValueError(
            f'no pamCode found for {self.portfolio_id=} {available_funds=}'
        )

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
            api_name='oak',
        )

    async def portfolio_industries(self) -> LazyFrame:
        return await self._json(
            'portfolio-composition/industry-percentage',
            df=True,
            params={'pamCode': await self._pam_code},
            api_name='oak',
        )

    _aa_keys = {
        'ACCOUNT_RECEIVABLE',
        'BOND',
        'CASH_AND_BANK',
        'DEPOSIT_BROKER',
        'FUND',
        'GOLD_AND_BAR',
        'OPTION',
        'OTHER_ASSET',
        'STOCK_AND_RIGHT_ADVANCE',
    }

    async def asset_allocation(self) -> dict[str, Any]:
        j = await self._json(
            'portfolio-composition/instrument-type-percentage',
            params={'pamCode': await self._pam_code},
            api_name='oak',
        )
        data = {record['assetGroupIdCode']: record['percent'] for record in j}
        self._check_aa_keys(data)
        return data

    async def cash(self) -> float:
        aa = await self.asset_allocation()
        g = aa.get
        return (
            sum(
                g(k, 0.0)
                for k in (
                    'CASH_AND_BANK',
                    'BOND',
                    'ACCOUNT_RECEIVABLE',
                    'DEPOSIT_BROKER',
                )
            )
            / 100.0
        )

    async def _available_funds(self):
        hi = await self.home_info()
        default_fund = find_value(hi, 'defaultFund')
        assert default_fund is not None
        available_funds = find_value(hi, 'availableFunds')
        assert available_funds is not None
        available_funds = available_funds.copy()
        available_funds[0] = default_fund
        return available_funds

    async def portfolios(self) -> dict[str, str]:
        default_fund = await self._available_funds()
        return {
            record['basketId']: record['faName'] for record in default_fund
        }


if __name__ == '__main__':
    s = Nika('https://ayar.mofidfund.com/')
    run(s.home_info())
