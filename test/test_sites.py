from datetime import datetime

from pandas import DataFrame
from numpy import dtype

from iranetf.sites import RayanHamafza, TadbirPardaz
from test import session_patch


def assert_live(live):
    assert type(live) is dict
    assert len(live) == 3
    assert type(live['cancel']) is type(live['issue']) is int  # noqa
    assert type(live['date']) == datetime


tadbir = TadbirPardaz('https://modirfund.ir/')
ryan = RayanHamafza('http://fardaetf.tadbirfunds.com/')


@session_patch('modir_live.json')
async def test_tadbir_live_navps():
    live = await tadbir.live_navps()
    assert_live(live)


@session_patch('almas_live.json')
async def test_rayan_live_navps():
    live = await ryan.live_navps()
    assert_live(live)


def assert_navps_history(df: DataFrame):
    assert [*df.dtypes.items()] == [
        ('date', dtype('<M8[ns]')),
        ('issue', dtype('int64')),
        ('cancel', dtype('int64')),
        ('statistical', dtype('int64'))]


@session_patch('modir_navps_history.json')
async def test_navps_history_tadbir():
    df = await tadbir.navps_history()
    assert_navps_history(df)


@session_patch('almas_navps_history.json')
async def test_navps_history_tadbir():
    df = await ryan.navps_history()
    assert_navps_history(df)
