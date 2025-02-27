from math import isclose

from aiohutils.tests import assert_dict_type, file

from iranetf import LiveNAVPS, MabnaDP
from tests import assert_navps_history

mabna_dp = MabnaDP('https://kianfunds6.ir/')


@file('hamvasn_live.json')
async def test_live_navps_mabna():
    d = await mabna_dp.live_navps()
    assert type(d.pop('date_time')) is str  # type: ignore
    assert type(d.pop('statistical_price')) is float  # type: ignore
    assert type(d.pop('unit_count')) is int  # type: ignore
    assert_dict_type(d, LiveNAVPS)


@file('hamvazn_navps_history.json')
async def test_navps_history_mabna():
    await assert_navps_history(mabna_dp)


@file('mabna_version.html')
async def test_mabna_version():
    assert (await mabna_dp.version()) == '2.15'


@file('old_mabna_version.html')
async def test_old_mabna_version():
    assert (await MabnaDP('https://gitidamavandfund.ir/').version()) == '2.21'


@file('mabna_aa.json')
async def test_asset_allocation():
    aa = await mabna_dp.asset_allocation()
    assert aa.keys() <= mabna_dp._aa_keys
    assert isclose(sum(aa.values()), 1.0, abs_tol=0.0000000001)


@file('mabna_aa.json')
async def test_cache():
    cache = await mabna_dp.cache()
    assert 0.0 <= cache <= 0.6
