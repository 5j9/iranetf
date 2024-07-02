from aiohutils.tests import init_tests
from numpy import dtype

from iranetf import BaseSite

init_tests()


async def assert_navps_history(site: BaseSite, has_statistical=True):
    df = await site.navps_history()
    assert df.index.dtype == dtype('<M8[ns]')
    assert df.index.name == 'date'
    numeric_types = ('int64', 'float64')
    assert df['creation'].dtype in numeric_types
    assert df['redemption'].dtype in numeric_types
    assert (df['redemption'] <= df['creation']).all()
    if has_statistical:
        assert df['statistical'].dtype in numeric_types


def test_from_l18():
    assert BaseSite.from_l18('استیل').url == 'https://mofidsectorfund.com/'
