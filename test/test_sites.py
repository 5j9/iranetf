from datetime import datetime

from pandas import DataFrame
from numpy import dtype

from iranetf.sites import RayanHamafza, TadbirPardaz
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
    assert [*df.dtypes.items()] == [
        ('date', dtype('<M8[ns]')),
        ('issue', dtype('int64')),
        ('cancel', dtype('int64')),
        ('statistical', dtype('int64'))]


@file('modir_navps_history.json')
async def test_navps_history_tadbir():
    df = await tadbir.navps_history()
    assert_navps_history(df)


@file('almas_navps_history.json')
async def test_navps_history_tadbir():
    df = await rayan.navps_history()
    assert_navps_history(df)
