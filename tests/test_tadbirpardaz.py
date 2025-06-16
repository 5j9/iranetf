from unittest.mock import patch

from aiohutils.tests import assert_dict_type, file, files
from polars import Datetime, Float64, Int64
from pytest import raises

from iranetf import (
    BaseSite,
    TadbirPardaz,
    TadbirPardazMultiNAV,
    TPLiveNAVPS,
)
from tests import assert_navps_history

tadbir = TadbirPardaz('https://modirfund.ir/')


@file('modir_live.json')
async def test_live_navps():
    d = await tadbir.live_navps()
    assert_dict_type(d, TPLiveNAVPS)


@file('modir_navps_history.json')
async def test_navps_history():
    await assert_navps_history(tadbir)


@file('icpfvc_navps_date_space.json')
async def test_navps_date_ends_with_space():
    d = await TadbirPardaz('https://icpfvc.ir/').live_navps()
    assert_dict_type(d, TPLiveNAVPS)


@files('still.json', 'khodran.json')
async def test_multinav():
    still = BaseSite.from_l18('استیل')
    khodran = BaseSite.from_l18('خودران')
    assert type(khodran) is TadbirPardazMultiNAV
    assert still.url == khodran.url
    still_nav = await still.live_navps()
    khodran_nav = await khodran.live_navps()
    assert still_nav.keys() == khodran_nav.keys()
    assert still_nav != khodran_nav


async def test_multinav_hist_path():
    khodran = BaseSite.from_l18('خودران')
    with patch('iranetf._get', side_effect=NotImplementedError) as get_mock:
        with raises(NotImplementedError):
            await khodran.navps_history()
    get_mock.assert_called_once_with(
        'https://mofidsectorfund.com/Chart/TotalNAV',
        {'type': 'getnavtotal', 'basketId': '3'},
        None,
    )


async def test_multinav_live_navps_path():
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
async def test_dividend_history():
    site = BaseSite.from_l18('آفاق')
    assert type(site) is TadbirPardaz
    df = await site.dividend_history()
    assert len(df) >= 22
    assert df.columns == [
        'row',
        'ProfitDate',
        'FundUnit',
        'UnitProfit',
        'SUMAllProfit',
        'ProfitPercent',
    ]
    assert df.dtypes == [
        Int64,
        Datetime(time_unit='us', time_zone=None),
        Int64,
        Int64,
        Int64,
        Float64,
    ]


EXPECTED_TP_VER = '9.2.5'


@file('tadbir_version.html')
async def test_version():
    assert (await tadbir.version()) == EXPECTED_TP_VER


@file('rouinfund.html')
async def test_info():
    steel: TadbirPardaz = BaseSite.from_l18('استیل')  # type: ignore
    assert await steel.info() == {
        'basketIDs': {
            '1': 'صندوق سرمایه\u200cگذاری بخشی صنایع مفید',
            '2': 'استیل',
            '3': 'خودران',
            '4': 'سیمانو',
            '5': 'اکتان',
            '6': 'دارونو',
            '7': 'معدن',
        },
        'isETFMultiNavMode': True,
        'isEtfMode': False,
        'isLeveragedMode': False,
    }
