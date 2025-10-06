from datetime import date
from unittest.mock import patch

from aiohutils.tests import file, files, validate_dict
from numpy import dtype
from pytest import raises

from iranetf.sites import (
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
    validate_dict(d, TPLiveNAVPS)


@file('modir_navps_history.json')
async def test_navps_history():
    await assert_navps_history(tadbir)


@file('icpfvc_navps_date_space.json')
async def test_navps_date_ends_with_space():
    d = await TadbirPardaz('https://icpfvc.ir/').live_navps()
    validate_dict(d, TPLiveNAVPS)


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
    with patch(
        'iranetf.sites._get', side_effect=NotImplementedError
    ) as get_mock:
        with raises(NotImplementedError):
            await khodran.navps_history()
    get_mock.assert_called_once_with(
        'https://mofidsectorfund.com/Chart/TotalNAV',
        {'type': 'getnavtotal', 'basketId': '3'},
        None,
    )


async def test_multinav_live_navps_path():
    khodran = BaseSite.from_l18('خودران')
    with patch(
        'iranetf.sites._get', side_effect=NotImplementedError
    ) as get_mock:
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
    assert [*df.dtypes.items()] == [
        ('row', dtype('int64')),
        ('FundUnit', dtype('int64')),
        ('UnitProfit', dtype('int64')),
        ('SUMAllProfit', dtype('int64')),
        ('ProfitPercent', dtype('float64')),
    ]
    assert (index := df.index).dtype == 'datetime64[ns]'
    assert index.name == 'ProfitDate'


@files(
    'tp_dividend_history_with_date1.html',
    'tp_dividend_history_with_date2.html',
)
async def test_dividend_history_with_dates():
    site = BaseSite.from_l18('امین یکم')
    assert type(site) is TadbirPardaz
    df = await site.dividend_history(
        from_date=date(2024, 11, 20), to_date=date(2025, 9, 22)
    )
    assert len(df) == 11


EXPECTED_TP_VER = '9.2.5'


@file('tadbir_version.html')
async def test_version():
    assert (await tadbir.version()) == EXPECTED_TP_VER


steel: TadbirPardaz = BaseSite.from_l18('استیل')  # type: ignore


@file('mofidsectorfund.html')
async def test_info():
    assert await steel.home_info() == {
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
        'seo_reg_no': '12150',
        'version': '9.2.2',
    }


@file('mofidsectorfund.html')
async def test_reg_no():
    assert await steel.reg_no() == '12150'


@file('derakhshan_aa.json')
async def test_option_in_asset_allocation(caplog):
    site = TadbirPardaz('https://dafund.ir/')
    await site.asset_allocation()
    assert not caplog.records
