from datetime import datetime
from enum import IntEnum
from re import search
from typing import Any, TypedDict

from jdatetime import datetime as jdatetime
from pandas import DataFrame

from iranetf.sites._lib import BaseSite, LiveNAVPS, reg_no_from_home_info


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


class FundDataItem(TypedDict):
    FundId: int
    FundName: str
    IsDefaultFund: bool


class FundData(TypedDict):
    FundType: FundType
    FundList: list[FundDataItem]


class BaseRayanHamafza(BaseSite):
    __slots__ = 'fund_id'

    def __init__(self, url: str):
        url, _, fund_id = url.partition('#')
        self.fund_id = fund_id or '1'
        super().__init__(url)

    _api_path: str
    _navps_history_path: str
    _nav_history_path: str
    _portfolio_industries_path: str
    _asset_allocation_path: str
    _dividend_history_path: str
    _dividend_history_date_col: str
    _dividend_history_data_key: str | None

    async def _json(self, path, **kwa) -> Any:
        return await super()._json(f'{self._api_path}/{path}', **kwa)

    async def navps_history(self) -> DataFrame:
        df: DataFrame = await self._json(
            f'{self._navps_history_path}{self.fund_id}', df=True
        )
        df.columns = ['date', 'creation', 'redemption', 'statistical']
        df['date'] = df['date'].map(_j2g)
        df.set_index('date', inplace=True)
        return df

    async def nav_history(self) -> DataFrame:
        df: DataFrame = await self._json(
            f'{self._nav_history_path}{self.fund_id}', df=True
        )
        df.columns = ['nav', 'date', 'creation_navps']
        df['date'] = df['date'].map(_j2g)
        return df

    async def portfolio_industries(self) -> DataFrame:
        return await self._json(
            f'{self._portfolio_industries_path}{self.fund_id}', df=True
        )

    async def asset_allocation(self) -> dict:
        d: dict = await self._json(
            f'{self._asset_allocation_path}{self.fund_id}'
        )
        self._check_aa_keys(d)
        return {k: v / 100 if type(v) is not str else v for k, v in d.items()}

    async def dividend_history(self) -> DataFrame:
        j = await self._json(f'{self._dividend_history_path}{self.fund_id}')
        if (key := self._dividend_history_data_key) is None:
            data = j
        else:
            data = j[key]
        df = DataFrame(data)
        date_col = self._dividend_history_date_col
        df[date_col] = df[date_col].apply(
            lambda i: jdatetime.strptime(i, format='%Y/%m/%d').togregorian()
        )
        df.set_index(date_col, inplace=True)
        return df

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
    _dividend_history_date_col = 'ProfitDate'
    _dividend_history_data_key = 'data'
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

    async def live_navps(self) -> LiveNAVPS:
        d: RHNavLight = await self._json(f'NavLight/{self.fund_id}')
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
    _dividend_history_date_col = 'profitDate'
    _dividend_history_data_key = None

    async def live_navps(self) -> LiveNAVPS:
        d: FundLiveInfo = await self._json(
            f'public/fundLiveInfo/{self.fund_id}'
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
