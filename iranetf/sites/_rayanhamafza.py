from datetime import datetime
from enum import IntEnum
from re import search
from typing import Any, TypedDict

from jdatetime import datetime as jdatetime
from pandas import DataFrame

from iranetf.sites._lib import BaseSite, LiveNAVPS


def _j2g(s: str) -> datetime:
    return jdatetime(*[int(i) for i in s.split('/')]).togregorian()


class RHNavLight(TypedDict):
    NextTimeInterval: int
    FundId: int
    FundNavId: int
    PurchaseNav: int
    SaleNav: int
    Date: str
    Time: str


class FundType(IntEnum):
    # the values are defined in the first line of public.min.js e.g. in
    # https://tazmin.charismafunds.ir/bundles/js/public.min.js?v=202508170532
    # fundType={simple:1,simpleETF:2,hybrid:3,multiFund:4,multiETF:5};
    SIMPLE = 1
    SIMPLE_ETF = 2
    HYBRID = 3
    MULTI_FUND = 4
    MULTI_ETF = 5


class FundList(TypedDict):
    FundId: int
    FundName: str
    IsDefaultFund: bool


class FundData(TypedDict):
    FundType: FundType
    FundList: list[FundList]


class RayanHamafza(BaseSite):
    _api_path = 'api/data'
    __slots__ = 'fund_id'

    async def _home_info(self) -> dict[str, Any]:
        html = await self._home()
        d = {}
        reg_no_match = search(r'ثبت شده به شماره (\d+) نزد سازمان بورس', html)
        if reg_no_match:
            d['seo_reg_no'] = reg_no_match[1]
        return d

    def __init__(self, url: str):
        url, _, fund_id = url.partition('#')
        self.fund_id = fund_id or '1'
        super().__init__(url)

    async def _json(self, path, **kwa) -> Any:
        return await super()._json(f'{self._api_path}/{path}', **kwa)

    async def live_navps(self) -> LiveNAVPS:
        d: RHNavLight = await self._json(f'NavLight/{self.fund_id}')
        return {
            'creation': d['PurchaseNav'],
            'redemption': d['SaleNav'],
            'date': jdatetime.strptime(
                f'{d["Date"]} {d["Time"]}', '%Y/%m/%d %H:%M:%S'
            ).togregorian(),
        }

    async def navps_history(self) -> DataFrame:
        df: DataFrame = await self._json(
            f'NavPerShare/{self.fund_id}', df=True
        )
        df.columns = ['date', 'creation', 'redemption', 'statistical']
        df['date'] = df['date'].map(_j2g)
        df.set_index('date', inplace=True)
        return df

    async def nav_history(self) -> DataFrame:
        df: DataFrame = await self._json(
            f'DailyNAVChart/{self.fund_id}', df=True
        )
        df.columns = ['nav', 'date', 'creation_navps']
        df['date'] = df['date'].map(_j2g)
        return df

    async def portfolio_industries(self) -> DataFrame:
        return await self._json(f'Industries/{self.fund_id}', df=True)

    _aa_keys = {
        'DepositTodayPercent',
        'TopFiveStockTodayPercent',
        'CashTodayPercent',
        'OtherAssetTodayPercent',
        'BondTodayPercent',
        'OtherStock',
        'JalaliDate',
        'CcdTodayPercent',  # Commodity Certificates of Deposit
    }

    async def asset_allocation(self) -> dict:
        d: dict = await self._json(f'MixAsset/{self.fund_id}')
        self._check_aa_keys(d)
        return {k: v / 100 if type(v) is not str else v for k, v in d.items()}

    async def dividend_history(self) -> DataFrame:
        j: dict = await self._json(f'Profit/{self.fund_id}')
        df = DataFrame(j['data'])
        df['ProfitDate'] = df['ProfitDate'].apply(
            lambda i: jdatetime.strptime(i, format='%Y/%m/%d').togregorian()
        )
        df.set_index('ProfitDate', inplace=True)
        return df

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        return (
            aa['DepositTodayPercent']
            + aa['CashTodayPercent']
            + aa['BondTodayPercent']
        )

    async def fund_data(self) -> FundData:
        fund_data = await self._json('Fund')
        fund_data['FundType'] = FundType(fund_data['FundType'])
        return fund_data
