from datetime import datetime

from aiohttp_test_utils import file
from numpy import dtype

from iranetf.sites import (
    BaseSite,
    LeveragedTadbirPardaz,
    MabnaDP,
    RayanHamafza,
    TadbirPardaz,
)


async def assert_live(site: BaseSite):
    live = await site.live_navps()
    assert type(live) is dict
    assert type(live['cancel']) is type(live['issue']) is int  # noqa
    assert type(live['date']) == datetime


tadbir = TadbirPardaz('https://modirfund.ir/')
rayan = RayanHamafza('https://yaghootfund.ir/')


@file('modir_live.json')
async def test_tadbir_live_navps():
    await assert_live(tadbir)


@file('almas_live.json')
async def test_rayan_live_navps():
    await assert_live(rayan)


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
    await assert_live(TadbirPardaz('http://www.icpfvc.ir/'))


mabna_dp = MabnaDP('https://kianfunds6.ir/')


@file('hamvasn_live.json')
async def test_live_navps_mabna():
    await assert_live(mabna_dp)


@file('hamvazn_navps_history.json')
async def test_navps_history_mabna():
    await assert_navps_history(mabna_dp)


ltp = LeveragedTadbirPardaz('https://ahrom.charisma.ir/')


@file('ahrom_live.json')
async def test_live_navps_ltp():
    await assert_live(ltp)


@file('ahrom_navps_history.json')
async def test_navps_history_ltp():
    # leveraged ETFs do not have statistical history for preferred shares
    await assert_navps_history(ltp, has_statistical=False)
