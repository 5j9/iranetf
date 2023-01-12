from datetime import datetime

from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from numpy import dtype

from iranetf.sites import RayanHamafza, TadbirPardaz, MabnaDP
from test.aiohttp_test_utils import file


def assert_live(live):
    assert type(live) is dict
    assert len(live) == 3
    assert type(live['cancel']) is type(live['issue']) is int  # noqa
    assert type(live['date']) == datetime


tadbir = TadbirPardaz('https://modirfund.ir/')
rayan = RayanHamafza('http://fardaetf.tadbirfunds.com/')


@file('modir_live.json')
async def test_tadbir_live_navps():
    live = await tadbir.live_navps()
    assert_live(live)


@file('almas_live.json')
async def test_rayan_live_navps():
    live = await rayan.live_navps()
    assert_live(live)


def assert_navps_history(df: DataFrame):
    assert df['date'].dtype ==  dtype('<M8[ns]')
    assert is_numeric_dtype(df['issue'])
    assert is_numeric_dtype(df['cancel'])
    assert is_numeric_dtype(df['statistical'])


@file('modir_navps_history.json')
async def test_navps_history_tadbir():
    df = await tadbir.navps_history()
    assert_navps_history(df)


@file('almas_navps_history.json')
async def test_navps_history_tadbir():
    df = await rayan.navps_history()
    assert_navps_history(df)


@file('icpfvc_navps_date_space.json')
async def test_navps_date_ends_with_space():
    d = await TadbirPardaz('http://www.icpfvc.ir/').live_navps()
    assert isinstance(d['date'], datetime)


mabna_dp = MabnaDP('https://kianfunds6.ir/')


@file('hamvasn_live.json')
async def test_mabnadp_live_navps():
    d = await mabna_dp.live_navps()
    assert isinstance(d['date'], datetime)
    assert isinstance(d['issue'], int)
    assert isinstance(d['cancel'], int)


@file('hamvazn_navps_history.json')
async def test_navps_history_tadbir():
    df = await mabna_dp.navps_history()
    assert_navps_history(df)
