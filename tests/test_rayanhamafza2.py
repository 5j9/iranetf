from math import isclose

from numpy import dtype
from pytest_aiohutils import file, files, validate_dict

from iranetf.sites import (
    BaseSite,
    RayanHamafza2,
)
from iranetf.sites._rayanhamafza import FundItem
from tests import assert_navps_history, validate_live_navps

yaqut = RayanHamafza2('https://yaghootfund.ir/')


@file('yaqut_live.json')
async def test_live_navps():
    await validate_live_navps(yaqut)


@file('yaqut_navps_history.json')
async def test_navps_history():
    await assert_navps_history(yaqut)


@file('yaqut.html')
async def test_reg_no():
    assert await yaqut.reg_no() == '11698'


@file('homay_profit.json')
async def test_fund_profit():
    df = await RayanHamafza2('https://www.homayeagah.ir/').dividend_history()
    assert [*df.dtypes.items()] == [
        ('fundId', dtype('int64')),
        ('fundUnit', dtype('int64')),
        ('profitGuaranteeUnit', dtype('int64')),
        ('unitProfit', dtype('int64')),
        ('extraProfit', dtype('int64')),
        ('sumUnitProfit', dtype('int64')),
        ('sumExtraProfit', dtype('int64')),
        ('sumProfitGuarantee', dtype('int64')),
        ('sumAllProfit', dtype('int64')),
    ]
    assert (index := df.index).dtype == 'datetime64[us]'
    assert index.name == 'profitDate'


petro_agah: RayanHamafza2 = BaseSite.from_l18('پتروآگاه')  # type: ignore
auto_agah: RayanHamafza2 = BaseSite.from_l18('اتوآگاه')  # type: ignore


@files('petroagah.json', 'autoagah.json')
async def test_multinav():
    assert type(auto_agah) is RayanHamafza2
    assert petro_agah.url == auto_agah.url
    petro_nav = await petro_agah.live_navps()
    auto_nav = await auto_agah.live_navps()
    assert petro_nav.keys() == auto_nav.keys()
    assert petro_nav != auto_nav


@file('petro_agah_aa.json')
async def test_asset_allocation():
    aa = await petro_agah.asset_allocation()
    assert aa.keys() <= petro_agah._aa_keys
    assert type(aa.pop('jalaliDate')) is str
    assert isclose(sum(aa.values()), 1.0)


@file('petro_agah_aa.json')
async def test_cache():
    cache = await petro_agah.cache()
    assert 0.0 <= cache <= 0.6


@files('petro_agah_aa.json')
async def test_leverage():
    assert type(await petro_agah.leverage()) is float


@file('petro_agah_fund_items.json')
async def test_fund_data():
    items = await petro_agah.fund_items()
    validate_dict(items[0], FundItem)


@file('petro_agah_fund_items.json')
async def test_portfolios():
    ps = await petro_agah.portfolios()
    assert ps['2'] == 'اتو آگاه'
