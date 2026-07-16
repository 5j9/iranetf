from datetime import date
from unittest.mock import patch

import polars as pl
from pytest import raises, skip
from pytest_aiohutils import file, files, validate_dict

from iranetf.sites import (
    BaseSite,
    TadbirPardaz,
    TadbirPardazMultiNAV,
    TPLiveNAVPS,
)
from tests import assert_date_column, assert_navps_history, validate_live_navps

tadbir = TadbirPardaz('https://modirfund.ir/')


@file('modir_live.json')
async def test_live_navps():
    await validate_live_navps(tadbir)


@file('modir_navps_history.json')
async def test_navps_history():
    await assert_navps_history(tadbir)


@file('empty_navps_history.json')
async def test_empty_navps_history(test_config):
    if not test_config['OFFLINE_MODE']:
        raise skip('not offline')
    site = TadbirPardaz.from_l18('قلک گلد')
    await assert_navps_history(site)


@file('icpfvc_navps_date_space.json')
async def test_navps_date_ends_with_space():
    d = await TadbirPardaz('https://icpfvc.ir/').live_navps()
    validate_dict(d, TPLiveNAVPS)


@files('still.json', 'khodran.json')
async def test_multinav():
    still = BaseSite.from_l18('استیل')
    khodran = BaseSite.from_l18('خودران')
    assert isinstance(khodran, TadbirPardazMultiNAV)
    assert still.url == khodran.url
    still_nav = await still.live_navps()
    khodran_nav = await khodran.live_navps()
    assert still_nav.keys() == khodran_nav.keys()
    assert still_nav != khodran_nav


async def test_multinav_hist_path():
    khodran = BaseSite.from_l18('خودران')
    with patch(
        'iranetf.sites._lib._get', side_effect=NotImplementedError
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
        'iranetf.sites._lib._get', side_effect=NotImplementedError
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
    assert isinstance(site, TadbirPardaz)

    lf = await site.dividend_history()
    df = lf.collect()

    assert len(df) >= 22

    expected_schema = {
        'row': pl.Int64,
        'fundUnit': pl.Int64,
        'unitProfit': pl.Int64,
        'sumAllProfit': pl.Int64,
        'profitPercent': pl.Float64,
    }

    for col_name, expected_type in expected_schema.items():
        assert df.schema[col_name] == expected_type, (
            f'Mismatched type for {col_name}'
        )

    assert_date_column(df)


@files(
    'tp_dividend_history_with_date1.html',
    'tp_dividend_history_with_date2.html',
)
async def test_dividend_history_with_dates():
    site = BaseSite.from_l18('امین یکم')
    assert isinstance(site, TadbirPardaz)

    lf = await site.dividend_history(
        from_date=date(2024, 11, 20), to_date=date(2025, 9, 22)
    )
    df = lf.collect()
    assert len(df) == 11


@file('tp_invalid_dividend_history.html')
async def test_invalid_dividend_history_value(test_config):
    if not test_config['OFFLINE_MODE']:
        raise skip('not offline')
    site = BaseSite.from_l18('آسان')
    assert isinstance(site, TadbirPardaz)

    lf = await site.dividend_history(
        from_date=date(2025, 1, 19), to_date=date(2025, 1, 19)
    )
    df = lf.collect()
    assert len(df) == 1

    # Replaced index-based .at lookup with type-safe filtering expressions
    target_value = (
        df.filter(pl.col('date') == date(2025, 1, 19))
        .select('profitPercent')
        .item()
    )

    assert target_value == 1.81671169356907e18


EXPECTED_TP_VER = '9.2.5'


@file('tadbir_version.html')
async def test_version():
    assert (await tadbir.version()) == EXPECTED_TP_VER


steel: TadbirPardaz = BaseSite.from_l18('استیل')  # type: ignore


@file('mofidsectorfund.html')
async def test_info():
    assert await steel.home_info() == {
        'basketIDs': {
            '1': 'صندوق سرمایه‌گذاری بخشی صنایع مفید',
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
async def test_portfolios():
    ps = await steel.portfolios()
    assert ps['2'] == 'استیل'


@file('mofidsectorfund.html')
async def test_reg_no():
    assert await steel.reg_no() == '12150'


@file('derakhshan_aa.json')
async def test_option_in_asset_allocation(caplog):
    site = TadbirPardaz('https://dafund.ir/')
    await site.asset_allocation()
    assert not caplog.records


@file('empty_divident_hist.html')
async def test_empty_divident_hist():
    site = TadbirPardaz('https://maskanamfund.ir/')
    dt = date(2025, 10, 29)

    lf = await site.dividend_history(from_date=dt, to_date=dt)
    df = lf.collect()

    assert df.height == 0


@file('navps_history_float.json')
async def test_navps_history_float(test_config):
    if not test_config['OFFLINE_MODE']:
        return
    await TadbirPardaz.from_l18('سها').navps_history()
