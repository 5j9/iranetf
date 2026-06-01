from math import isclose

import polars as pl
from pytest_aiohutils import file, file_map, files

from iranetf.sites import BaseSite, MabnaDP2
from tests import (
    assert_date_column,  # Swapped from assert_date_index
    assert_leveraged_leverage,
    assert_navps_history,
    validate_live_navps,
)

site = MabnaDP2('https://kianfunds10.ir/')


@file('lmdp_home.html')
async def test_from_url():
    s = await BaseSite.from_url(site.url)
    assert s == site
    assert await site.reg_no() == '12433'


@file('lmdp_live.json')
async def test_live_navps():
    await validate_live_navps(site)


@file('lmdp_navps_history.json')
async def test_navps_history():
    await assert_navps_history(site)


@file('lmdp_aa.json')
async def test_asset_allocation():
    aa = await site.asset_allocation()
    assert aa.keys() <= site._aa_keys, aa.keys() - site._aa_keys
    assert isclose(sum(aa.values()), 1.0, abs_tol=0.0000000001)


@file('lmdp_aa.json')
async def test_cache():
    cache = await site.cache()
    assert 0.0 <= cache <= 0.6


@file('home.html')
async def test_home_data():
    d = await site.home_data()
    assert d['__REACT_QUERY_STATE__'].keys() == {'mutations', 'queries'}
    assert d['__REACT_REDUX_STATE__'].keys() == {
        'general',
        'serverSideMeta',
        'testConcurrency',
    }
    assert d['__ENV__'].keys() == {
        'VITE_RUNTIME_THEME_OVERRIDE',
        'VITE_API_GATEWAY',
        'VITE_API_PREFIX',
    }


@files('home.html', 'lmdp_aa.json')
async def test_leverage():
    await assert_leveraged_leverage(site)


hamvazn = MabnaDP2.from_l18('هم وزن')


@file_map(
    ('assets-classification', 'hamvazn_leverage.json'),
    (hamvazn.url, 'hamvazn.html'),
)
async def test_non_leveraged_leverage():
    lev = await hamvazn.leverage()
    assert 0.5 < lev <= 1.0


@file('md2_assets_history.json')
async def test_assets_history():
    site = MabnaDP2('https://gitidamavandfund.ir/')

    # Materialize the lazy data block to introspect structural schema types
    lf = await site.assets_history()
    df = lf.collect()

    assert_date_column(df)

    # Assert exact schema structural matching using clean Polars type mappings
    assert df.schema['date_time'] == pl.Datetime(
        time_unit='ns', time_zone='Asia/Tehran'
    )
    assert df.schema['value'] == pl.Int64


@file('test_alt_home_data_format_mabnadp2.html')
async def test_alt_home_data_format():
    site = await MabnaDP2.from_url('https://kianfunds6.ir/')
    data = await site.home_data()  # type: ignore
    assert data.keys() == {
        '__REACT_QUERY_STATE__',
        '__REACT_REDUX_STATE__',
        '__ENV__',
    }


@file('test_portfolios.json')
async def test_portfolios():
    site = MabnaDP2('https://kianfunds9.ir/')
    portfolios = await site.portfolios()
    for pid, pname in portfolios.items():
        if pid == '11':
            assert pname == 'یوتیلیتی'
            break
    else:
        raise RuntimeError('portfolio with id 11 was not found')
