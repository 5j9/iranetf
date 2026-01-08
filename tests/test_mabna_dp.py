from math import isclose

from pytest import mark
from pytest_aiohutils import file, validate_dict

from iranetf.sites import LiveNAVPS, MabnaDP
from tests import assert_navps_history

site = MabnaDP.from_l18('رویش همراه')


@file('hamvasn_live.json')
async def test_live_navps_mabna():
    d = await site.live_navps()
    assert type(d.pop('date_time')) is str
    assert type(d.pop('statistical_price')) is float
    assert type(d.pop('unit_count')) is int
    validate_dict(d, LiveNAVPS)


@file('hamvazn_navps_history.json')
async def test_navps_history_mabna():
    await assert_navps_history(site)


@file('mabna_version.html')
async def test_mabna_version():
    assert (await site.version()) == '2.22'
    assert await site.reg_no() == '12436'


@mark.xfail
@file('old_mabna_version.html')
async def test_old_mabna_version():
    assert (await MabnaDP('https://gitidamavandfund.ir/').version()) == '2.21'


@file('mabna_aa.json')
async def test_asset_allocation():
    aa = await site.asset_allocation()
    assert aa.keys() <= site._aa_keys
    assert isclose(sum(aa.values()), 1.0, abs_tol=0.0000000001)


@file('mabna_aa.json')
async def test_cache():
    cache = await site.cache()
    assert 0.0 <= cache <= 0.6
