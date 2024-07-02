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


@files('petroagah.json', 'autoagah.json')
async def test_multinav():
    petro = BaseSite.from_l18('پتروآگاه')
    auto = BaseSite.from_l18('اتوآگاه')
    assert type(auto) is RayanHamafzaMultiNAV
    assert petro.url == auto.url
    petro_nav = await petro.live_navps()
    auto_nav = await auto.live_navps()
    assert petro_nav.keys() == auto_nav.keys()
    assert petro_nav != auto_nav
