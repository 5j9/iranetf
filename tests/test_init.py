from unittest.mock import patch

from aiohutils.tests import assert_dict_type, file, files
from numpy import dtype
from pytest import raises

from iranetf import (
    BaseSite,
    LeveragedTadbirPardaz,
    LeveragedTadbirPardazLiveNAVPS,
    LiveNAVPS,
    MabnaDP,
    RayanHamafza,
    RayanHamafzaMultiNAV,
    TadbirPardaz,
    TadbirPardazMultiNAV,
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
    assert df.index.dtype == dtype('<M8[ns]')
    assert df.index.name == 'date'
    numeric_types = ('int64', 'float64')
    assert df['creation'].dtype in numeric_types
    assert df['redemption'].dtype in numeric_types
    assert (df['redemption'] <= df['creation']).all()
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


def test_from_l18():
    assert BaseSite.from_l18('استیل').url == 'https://mofidsectorfund.com/'


EXPECTED_TP_VER = '9.2.5'


@file('tadbir_version.html')
async def test_tadbir_version():
    assert (await tadbir.version()) == EXPECTED_TP_VER


@file('leveraged_tadbir_version.html')
async def test_leveraged_tadbir_version():
    assert (await ltp.version()) == EXPECTED_TP_VER


@file('mabna_version.html')
async def test_mabna_version():
    assert (await mabna_dp.version()) == '2.15'


@file('old_mabna_version.html')
async def test_old_mabna_version():
    assert (await MabnaDP('https://gitidamavandfund.ir/').version()) == '2.12'


@files('petroagah.json', 'autoagah.json')
async def test_rayanhamafza_multinav():
    petro = BaseSite.from_l18('پتروآگاه')
    auto = BaseSite.from_l18('اتوآگاه')
    assert type(auto) is RayanHamafzaMultiNAV
    assert petro.url == auto.url
    petro_nav = await petro.live_navps()
    auto_nav = await auto.live_navps()
    assert petro_nav.keys() == auto_nav.keys()
    assert petro_nav != auto_nav


@files('still.json', 'khodran.json')
async def test_tadbirpardaz_multinav():
    still = BaseSite.from_l18('استیل')
    khodran = BaseSite.from_l18('خودران')
    assert type(khodran) is TadbirPardazMultiNAV
    assert still.url == khodran.url
    still_nav = await still.live_navps()
    khodran_nav = await khodran.live_navps()
    assert still_nav.keys() == khodran_nav.keys()
    assert still_nav != khodran_nav


async def test_tadbirpardas_multinav_hist_path():
    khodran = BaseSite.from_l18('خودران')
    with patch('iranetf._get', side_effect=NotImplementedError) as get_mock:
        with raises(NotImplementedError):
            await khodran.navps_history()
    get_mock.assert_called_once_with(
        'https://mofidsectorfund.com/Chart/TotalNAV',
        {'type': 'getnavtotal', 'basketId': '3'},
        None,
    )


async def test_tadbirpardas_multinav_live_navps_path():
    khodran = BaseSite.from_l18('خودران')
    with patch('iranetf._get', side_effect=NotImplementedError) as get_mock:
        with raises(NotImplementedError):
            await khodran.live_navps()
    get_mock.assert_called_once_with(
        'https://mofidsectorfund.com/Fund/GetETFNAV',
        {'basketId': '3'},
        None,
    )


@files(
    'tp_dividend_history_1.html',
    'tp_dividend_history_2.html',
    'tp_dividend_history_3.html',
)
async def test_tadbirpardaz_multinav():
    site = BaseSite.from_l18('آفاق')
    assert type(site) is TadbirPardaz
    df = await site.dividend_history()
    assert len(df) >= 22
    assert [*df.dtypes.items()] == [
        ('row', dtype('int64')),
        ('ProfitDate', dtype('<M8[ns]')),
        ('FundUnit', dtype('int64')),
        ('UnitProfit', dtype('int64')),
        ('SUMAllProfit', dtype('int64')),
        ('ProfitPercent', dtype('float64')),
    ]
