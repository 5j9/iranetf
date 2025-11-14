from math import isclose

from numpy import dtype
from pytest_aiohutils import file, files, validate_dict

from iranetf.sites import BaseSite, LiveNAVPS, MabnaDP2
from tests import (
    assert_date_index,
    assert_leveraged_leverage,
    assert_navps_history,
)

site = MabnaDP2('https://kianfunds10.ir/')


@file('lmdp_home.html')
async def test_from_url():
    s = await BaseSite.from_url(site.url)
    assert s == site
    assert await site.reg_no() == '12433'


@file('lmdp_live.json')
async def test_live_navps():
    d = await site.live_navps()
    validate_dict(d, LiveNAVPS)


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


@file('md2_assets_history.json')
async def test_assets_history():
    site = MabnaDP2('https://gitidamavandfund.ir/')
    df = await site.assets_history()
    assert_date_index(df)
    assert [*df.dtypes.items()] == [
        (
            'date_time',
            'datetime64[ns, UTC+03:30]',
        ),
        (
            'value',
            dtype('int64'),
        ),
    ]
