from aiohutils.tests import init_tests
from polars import Datetime, Series

from iranetf import BaseSite

init_tests()


async def assert_navps_history(site: BaseSite, has_statistical=True):
    df = await site.navps_history()
    date: Series = df['date']
    assert date.dtype == Datetime(time_unit='us', time_zone=None), date.dtype
    assert date.name == 'date'
    assert (date.dt.date() == date).all()
    assert df['creation'].dtype.is_numeric()
    assert df['redemption'].dtype.is_numeric()
    assert (df['redemption'] <= df['creation']).all()
    if has_statistical:
        assert df['statistical'].dtype.is_numeric()


def test_from_l18():
    assert BaseSite.from_l18('استیل').url == 'https://mofidsectorfund.com/'


async def assert_leveraged_leverage(site: BaseSite):
    lev = await site.leverage()
    assert type(lev) is float
    assert lev > 1.0
