from numpy import dtype
from pandas import DataFrame, DatetimeIndex
from pytest_aiohutils import validate_dict

from iranetf.sites import BaseSite, LiveNAVPS


def assert_date_index(df: DataFrame):
    index: DatetimeIndex = df.index  # type: ignore
    assert index.dtype in ('datetime64[us]', 'datetime64[ns]'), index.dtype
    assert index.name == 'date'
    assert (index.normalize() == index).all()


async def assert_navps_history(site: BaseSite, has_statistical=True):
    df = await site.navps_history()
    assert_date_index(df)
    numeric_types = ('int64', 'float64')
    assert df['creation'].dtype in numeric_types
    assert df['redemption'].dtype in numeric_types
    assert (df['redemption'] <= df['creation']).all()
    if has_statistical:
        assert df['statistical'].dtype in numeric_types


async def validate_live_navps(site: BaseSite):
    d = await site.live_navps()
    validate_dict(d, LiveNAVPS)
    assert d['creation'] > d['redemption']


def test_from_l18():
    assert BaseSite.from_l18('استیل').url == 'https://mofidsectorfund.com/'


async def assert_leveraged_leverage(site: BaseSite):
    lev = await site.leverage()
    assert type(lev) is float
    assert lev > 1.0


def assert_divident_history(df: DataFrame):
    assert [*df.dtypes.items()] == [
        ('fundId', dtype('int64')),
        ('fundUnit', dtype('int64')),
        ('profitGuaranteeUnit', dtype('int64')),
        ('unitProfit', dtype('int64')),
        ('extraProfit', dtype('int64')),
        ('sumUnitProfit', dtype('int64')),
        ('sumExtraProfit', dtype('int64')),
        ('sumProfitGuarantee', dtype('int64')),
        ('sumAllProfit', dtype('int64')),
    ]
    assert (index := df.index).dtype == 'datetime64[us]'
    assert index.name == 'profitDate'
