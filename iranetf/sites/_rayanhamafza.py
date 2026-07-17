from datetime import datetime
from enum import IntEnum
from re import search
from typing import Any, TypedDict

import polars as pl
from jdatetime import datetime as jdatetime

from iranetf.sites._lib import (
    BaseSite,
    LiveNAVPS,
    _jymd_to_greg,
    reg_no_from_home_info,
)


class RHNavLight(TypedDict):
    NextTimeInterval: int
    FundId: int
    FundNavId: int
    PurchaseNav: int
    SaleNav: int
    Date: str
    Time: str


class FundType(IntEnum):
    # fundType={simple:1,simpleETF:2,hybrid:3,multiFund:4,multiETF:5};
    SIMPLE = 1
    SIMPLE_ETF = 2
    HYBRID = 3
    MULTI_FUND = 4
    MULTI_ETF = 5


class FundDataItem(TypedDict):
    FundId: int
    FundName: str
    IsDefaultFund: bool


class FundData(TypedDict):
    FundType: FundType
    FundList: list[FundDataItem]


class BaseRayanHamafza(BaseSite):
    def __init__(self, url: str, portfolio_id: str = '1'):
        assert portfolio_id
        super().__init__(url, portfolio_id)

    _api_path: str
    _navps_history_path: str
    _nav_history_path: str
    _portfolio_industries_path: str
    _asset_allocation_path: str
    _dividend_history_path: str
    _dividend_history_data_key: str | None

    async def _json(self, path, **kwa) -> Any:
        return await super()._json(f'{self._api_path}/{path}', **kwa)

    async def navps_history(self) -> pl.LazyFrame:
        # Pulls the in-memory payload lazily and updates expressions together
        lf: pl.LazyFrame = await self._json(
            f'{self._navps_history_path}{self.portfolio_id}', df=True
        )

        # Uses pl.nth() positional indices to abstract away casing/naming variances
        # between RayanHamafza and RayanHamafza2 JSON payloads.
        return lf.select(
            [
                pl.nth(0)
                .map_elements(_jymd_to_greg, return_dtype=pl.Date)
                .alias('date'),
                pl.nth(1).alias('creation'),
                pl.nth(2).alias('redemption'),
                pl.nth(3).alias('statistical'),
            ]
        )

    async def nav_history(self) -> pl.LazyFrame:
        lf: pl.LazyFrame = await self._json(
            f'{self._nav_history_path}{self.portfolio_id}', df=True
        )

        return lf.select(
            [
                pl.col('column_0').alias('nav'),
                pl.col('column_1')
                .map_elements(_jymd_to_greg, return_dtype=pl.Date)
                .alias('date'),
                pl.col('column_2').alias('creation_navps'),
            ]
        )

    async def portfolio_industries(self) -> pl.LazyFrame:
        return await self._json(
            f'{self._portfolio_industries_path}{self.portfolio_id}', df=True
        )

    async def asset_allocation(self) -> dict:
        d: dict = await self._json(
            f'{self._asset_allocation_path}{self.portfolio_id}'
        )
        self._check_aa_keys(d)
        return {
            k: v / 100 if not isinstance(v, str) else v for k, v in d.items()
        }

    async def dividend_history(self) -> pl.LazyFrame:
        j = await self._json(
            f'{self._dividend_history_path}{self.portfolio_id}'
        )
        data = (
            j if (key := self._dividend_history_data_key) is None else j[key]
        )

        # Enforce clean structural schema safety directly from raw memory sequence
        lf = pl.LazyFrame(data, infer_schema_length=None)

        if isinstance(self, RayanHamafza):
            # Dynamic camelCase/PascalCase adjustment using native expression alias workflows
            schema = lf.collect_schema()
            lf = lf.rename(
                {col: col[0].lower() + col[1:] for col in schema.names()}
            )
        return lf.with_columns(
            pl.col('profitDate')
            .map_elements(
                lambda i: _jymd_to_greg(i) if i is not None else None,
                return_dtype=pl.Date,
            )
            .alias('date')
        )

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        return (
            aa['DepositTodayPercent']
            + aa['CashTodayPercent']
            + aa['BondTodayPercent']
        )

    reg_no = reg_no_from_home_info


class RayanHamafza(BaseRayanHamafza):
    __slots__ = ()
    _api_path = 'api/data'
    _navps_history_path = 'NavPerShare/'
    _nav_history_path = 'DailyNAVChart/'
    _portfolio_industries_path = 'Industries/'
    _asset_allocation_path = 'MixAsset/'
    _dividend_history_path = 'Profit/'
    _dividend_history_data_key = 'data'
    _aa_keys = {
        'DepositTodayPercent',
        'TopFiveStockTodayPercent',
        'CashTodayPercent',
        'OtherAssetTodayPercent',
        'BondTodayPercent',
        'OtherStock',
        'JalaliDate',
        'CcdTodayPercent',
    }

    async def live_navps(self) -> LiveNAVPS:
        d: RHNavLight = await self._json(f'NavLight/{self.portfolio_id}')
        return {
            'creation': d['PurchaseNav'],
            'redemption': d['SaleNav'],
            'date': jdatetime.strptime(
                f'{d["Date"]} {d["Time"]}', '%Y/%m/%d %H:%M:%S'
            ).togregorian(),
        }

    async def _home_info(self) -> dict[str, Any]:
        html = await self._home()
        d = {}
        reg_no_match = search(r'ثبت شده به شماره (\d+) نزد سازمان بورس', html)
        if reg_no_match:
            d['seo_reg_no'] = reg_no_match[1]
        return d

    async def fund_data(self) -> FundData:
        fund_data = await self._json('Fund')
        fund_data['FundType'] = FundType(fund_data['FundType'])
        return fund_data

    async def portfolios(self) -> dict[str, str]:
        fund_data = await self.fund_data()
        return {str(f['FundId']): f['FundName'] for f in fund_data['FundList']}


class FundLiveInfo(TypedDict):
    nextTimeInterval: int
    fundId: int
    fundNavId: int
    purchaseNav: int
    saleNav: int
    date: str
    time: str
    valueDivisionRemain: int
    datetime: datetime


class SiteInfo(TypedDict):
    dsCode: str
    fundCode: str
    submitCode: str
    fundType: int
    defaultFundId: int
    fundName: str
    fundAbbreviation: str
    webSiteUrl: str


class FundItem(TypedDict):
    fundId: int
    fundName: str
    isDefaultFund: bool
    isETF: bool
    logoUrl: str
    tsetmcUrl: str | None


class RayanHamafza2(BaseRayanHamafza):
    __slots__ = ()

    async def _home_info(self) -> dict[str, Any]:
        html = await self._home()
        return {'title': html.partition('<title>')[2].partition('</title>')[0]}

    _api_path = 'api/v1/'
    _navps_history_path = 'public/navPerShare/'
    _nav_history_path = 'public/dailyNav/'
    _portfolio_industries_path = '/public/industries/'
    _aa_keys = {i[0].lower() + i[1:] for i in RayanHamafza._aa_keys}
    _asset_allocation_path = 'public/mixAsset/'
    _dividend_history_path = 'public/fundProfits/'
    _dividend_history_data_key = None

    async def live_navps(self) -> LiveNAVPS:
        d: FundLiveInfo = await self._json(
            f'public/fundLiveInfo/{self.portfolio_id}'
        )
        return {
            'creation': d['purchaseNav'],
            'redemption': d['saleNav'],
            'date': jdatetime.strptime(
                f'{d["date"]} {d["time"]}', '%Y/%m/%d %H:%M:%S'
            ).togregorian(),
        }

    async def cache(self) -> float:
        aa = await self.asset_allocation()
        return (
            aa['depositTodayPercent']
            + aa['cashTodayPercent']
            + aa['bondTodayPercent']
        )

    async def site_info(self) -> SiteInfo:
        site_info = await self._json('public/siteInfo')
        site_info['fundType'] = FundType(site_info['fundType'])
        return site_info

    async def fund_items(self) -> list[FundItem]:
        return await self._json('public/fundItems')

    async def portfolios(self) -> dict[str, str]:
        items = await self.fund_items()
        return {str(f['fundId']): f['fundName'] for f in items}

    async def reg_no(self) -> str:
        si = await self.site_info()
        return si['dsCode']
