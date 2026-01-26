from math import isclose

from numpy import dtype
from pytest_aiohutils import file, files, validate_dict

from iranetf.sites import (
    BaseSite,
    FundData,
    LiveNAVPS,
    RayanHamafza,
)
from tests import assert_navps_history

yaqut = RayanHamafza('https://yaghootfund.ir/')


@file('almas_live.json')
async def test_live_navps():
    validate_dict(await yaqut.live_navps(), LiveNAVPS)


@file('almas_navps_history.json')
async def test_navps_history():
    await assert_navps_history(yaqut)


@file('yaqut.html')
async def test_reg_no():
    assert await yaqut.reg_no() == '11698'


@file('homay_profit.json')
async def test_fund_profit():
    df = await RayanHamafza('https://www.homayeagah.ir/').dividend_history()
    assert [*df.dtypes.items()] == [
        ('FundId', dtype('int64')),
        ('FundApId', dtype('int64')),
        ('FundUnit', dtype('int64')),
        ('ProfitGuaranteeUnit', dtype('int64')),
        ('UnitProfit', dtype('int64')),
        ('ExtraProfit', dtype('int64')),
        ('SumUnitProfit', dtype('int64')),
        ('SumExtraProfit', dtype('int64')),
        ('SumProfitGuarantee', dtype('int64')),
        ('SumAllProfit', dtype('int64')),
    ]
    assert (index := df.index).dtype == 'datetime64[us]'
    assert index.name == 'ProfitDate'


petro_agah: RayanHamafza = BaseSite.from_l18('پتروآگاه')  # type: ignore
auto_agah: RayanHamafza = BaseSite.from_l18('اتوآگاه')  # type: ignore


@files('petroagah.json', 'autoagah.json')
async def test_multinav():
    assert type(auto_agah) is RayanHamafza
    assert petro_agah.url == auto_agah.url
    petro_nav = await petro_agah.live_navps()
    auto_nav = await auto_agah.live_navps()
    assert petro_nav.keys() == auto_nav.keys()
    assert petro_nav != auto_nav


@file('petro_agah_aa.json')
async def test_asset_allocation():
    aa = await petro_agah.asset_allocation()
    assert aa.keys() <= petro_agah._aa_keys
    assert type(aa.pop('JalaliDate')) is str
    assert isclose(sum(aa.values()), 1.0)


@file('petro_agah_aa.json')
async def test_cache():
    cache = await petro_agah.cache()
    assert 0.0 <= cache <= 0.6


@files('petro_agah_aa.json')
async def test_leverage():
    assert type(await petro_agah.leverage()) is float


@file('petro_agah_fund_data.json')
async def test_fund_data():
    validate_dict(await petro_agah.fund_data(), FundData)
