from aiohutils.tests import init_tests
from numpy import dtype
from pandas import DatetimeIndex

from iranetf import BaseSite

init_tests()


async def assert_navps_history(site: BaseSite, has_statistical=True):
    df = await site.navps_history()
    index: DatetimeIndex = df.index  # type: ignore
    assert index.dtype == dtype('<M8[ns]'), index.dtype
    assert index.name == 'date'
    assert (index.normalize() == index).all()
    numeric_types = ('int64', 'float64')
    assert df['creation'].dtype in numeric_types
    assert df['redemption'].dtype in numeric_types
    assert (df['redemption'] <= df['creation']).all()
    if has_statistical:
        assert df['statistical'].dtype in numeric_types


def test_from_l18():
    assert BaseSite.from_l18('استیل').url == 'https://mofidsectorfund.com/'


async def assert_leveraged_leverage(site: BaseSite):
    lev = await site.leverage()
    assert type(lev) is float
    assert lev > 1.0
