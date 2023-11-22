from aiohutils.tests import assert_dict_type, file
from numpy import dtype

from iranetf import (
    BaseSite,
    LeveragedTadbirPardaz,
    LeveragedTadbirPardazLiveNAVPS,
    LiveNAVPS,
    MabnaDP,
    RayanHamafza,
    TadbirPardaz,
    TPLiveNAVPS,
)

tadbir = TadbirPardaz('https://modirfund.ir/')
rayan = RayanHamafza('https://yaghootfund.ir/')


@file('modir_live.json')
async def test_tadbir_live_navps():
    d = await tadbir.live_navps()
    assert_dict_type(d, TPLiveNAVPS)


@file('almas_live.json')
async def test_rayan_live_navps():
    assert_dict_type(await rayan.live_navps(), LiveNAVPS)


async def assert_navps_history(site: BaseSite, has_statistical=True):
    df = await site.navps_history()
    assert df['date'].dtype == dtype('<M8[ns]')
    numeric_types = ('int64', 'float64')
    assert df['issue'].dtype in numeric_types
    assert df['cancel'].dtype in numeric_types
    if has_statistical:
        assert df['statistical'].dtype in numeric_types


@file('modir_navps_history.json')
async def test_navps_history_tadbir():
    await assert_navps_history(tadbir)


@file('almas_navps_history.json')
async def test_navps_history_rayan():
    await assert_navps_history(rayan)


@file('icpfvc_navps_date_space.json')
async def test_navps_date_ends_with_space():
    d = await TadbirPardaz('https://icpfvc.ir/').live_navps()
    assert_dict_type(d, TPLiveNAVPS)


mabna_dp = MabnaDP('https://kianfunds6.ir/')


@file('hamvasn_live.json')
async def test_live_navps_mabna():
    d = await mabna_dp.live_navps()
    assert type(d.pop('date_time')) is str
    assert type(d.pop('statistical_price')) is float
    assert type(d.pop('unit_count')) is int
    assert_dict_type(d, LiveNAVPS)


@file('hamvazn_navps_history.json')
async def test_navps_history_mabna():
    await assert_navps_history(mabna_dp)


ltp = LeveragedTadbirPardaz('https://ahrom.charisma.ir/')


@file('ahrom_live.json')
async def test_live_navps_ltp():
    live = await ltp.live_navps()
    assert_dict_type(live, LeveragedTadbirPardazLiveNAVPS)


@file('ahrom_navps_history.json')
async def test_navps_history_ltp():
    # leveraged ETFs do not have statistical history for preferred shares
    await assert_navps_history(ltp, has_statistical=False)


@file('homay_profit.json')
async def test_rayanhamafza_fund_profit():
    df = await RayanHamafza('https://www.homayeagah.ir/').fund_profit()
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


def test_from_l18():
    assert BaseSite.from_l18('استیل').url == 'https://mofidsectorfund.com/'


@file('tadbir_version.html')
async def test_tadbir_version():
    assert (await tadbir.version()) == '9.2.2'


@file('leveraged_tadbir_version.html')
async def test_leveraged_tadbir_version():
    assert (await ltp.version()) == '9.2.2'


@file('mabna_version.html')
async def test_mabna_version():
    assert (await mabna_dp.version()) == '2.5'


@file('old_mabna_version.html')
async def test_old_mabna_version():
    assert (await MabnaDP('https://gitidamavandfund.ir/').version()) == '2.11'
