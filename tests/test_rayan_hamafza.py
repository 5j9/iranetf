from math import isclose

from aiohutils.tests import assert_dict_type, file, files
from numpy import dtype

from iranetf import (
    BaseSite,
    LiveNAVPS,
    RayanHamafza,
    RayanHamafzaMultiNAV,
)
from tests import assert_navps_history

rayan = RayanHamafza('https://yaghootfund.ir/')


@file('almas_live.json')
async def test_live_navps():
    assert_dict_type(await rayan.live_navps(), LiveNAVPS)


@file('almas_navps_history.json')
async def test_navps_history():
    await assert_navps_history(rayan)


@file('homay_profit.json')
async def test_fund_profit():
    df = await RayanHamafza('https://www.homayeagah.ir/').dividend_history()
    assert [*df.dtypes.items()] == [
        ('ProfitDate', dtype('<M8[ns]')),
        ('FundUnit', dtype('int64')),
        ('ProfitGuaranteeUnit', dtype('int64')),
        ('UnitProfit', dtype('int64')),
        ('ExtraProfit', dtype('int64')),
        ('SumUnitProfit', dtype('int64')),
        ('SumExtraProfit', dtype('int64')),
        ('SumProfitGuarantee', dtype('int64')),
        ('SUMAllProfit', dtype('int64')),
    ]


petro_agah: RayanHamafzaMultiNAV = BaseSite.from_l18('پتروآگاه')  # type: ignore
auto_agah: RayanHamafzaMultiNAV = BaseSite.from_l18('اتوآگاه')  # type: ignore


@files('petroagah.json', 'autoagah.json')
async def test_multinav():
    assert type(auto_agah) is RayanHamafzaMultiNAV
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
