from math import isclose

from pytest_aiohutils import file, files, validate_dict

from iranetf.sites import (
    BaseSite,
    RayanHamafza2,
)
from iranetf.sites._rayanhamafza import FundItem
from tests import (
    assert_dividend_history,  # Corrected spelling mapping layout to match core utils
    assert_navps_history,
    validate_live_navps,
)

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
    # Safely passes the returned LazyFrame over to your core assertions ruleblock
    assert_dividend_history(
        await RayanHamafza2('https://www.homayeagah.ir/').dividend_history()
    )


petro_agah: RayanHamafza2 = BaseSite.from_l18('پتروآگاه')  # type: ignore
auto_agah: RayanHamafza2 = BaseSite.from_l18('اتوآگاه')  # type: ignore


@files('petroagah.json', 'autoagah.json')
async def test_multinav():
    assert isinstance(auto_agah, RayanHamafza2)
    assert petro_agah.url == auto_agah.url
    petro_nav = await petro_agah.live_navps()
    auto_nav = await auto_agah.live_navps()
    assert petro_nav.keys() == auto_nav.keys()
    assert petro_nav != auto_nav


@file('petro_agah_aa.json')
async def test_asset_allocation():
    aa = await petro_agah.asset_allocation()
    assert aa.keys() <= petro_agah._aa_keys
    assert isinstance(aa.pop('jalaliDate'), str)
    assert isclose(sum(aa.values()), 1.0)


@file('petro_agah_aa.json')
async def test_cache():
    cache = await petro_agah.cache()
    assert 0.0 <= cache <= 0.6


@files('petro_agah_aa.json')
async def test_leverage():
    assert isinstance(await petro_agah.leverage(), float)


@file('petro_agah_fund_items.json')
async def test_fund_data():
    items = await petro_agah.fund_items()
    validate_dict(items[0], FundItem)


@file('petro_agah_fund_items.json')
async def test_portfolios():
    ps = await petro_agah.portfolios()
    assert ps['2'] == 'اتو آگاه'
