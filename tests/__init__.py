import polars as pl
from pytest_aiohutils import validate_dict

from iranetf.sites import BaseSite, LiveNAVPS


def assert_date_column(df: pl.DataFrame, col_name: str = 'date'):
    """
    Validates that the given date column contains proper date/datetime types
    and doesn't contain time-of-day offsets.
    """
    assert col_name in df.columns, (
        f"Column '{col_name}' missing from DataFrame"
    )

    # Check that it's mapped to a Polars Datetime or Date structural representation
    dtype = df.schema[col_name]
    assert dtype in (pl.Datetime, pl.Date), (
        f'Unexpected date column dtype: {dtype}'
    )

    # Evaluates the expression directly within the dataframe and pulls out
    # the scalar boolean result using .item()
    all_normalized = df.select(
        (pl.col(col_name).dt.time() == pl.time(0, 0, 0)).all()
    ).item()

    assert all_normalized, 'Date column contains non-normalized time values'


async def assert_navps_history(site: BaseSite, has_statistical=True):
    # Materialize the LazyFrame to run verification checks
    lf = await site.navps_history()
    df = lf.collect()

    assert_date_column(df, col_name='date')

    numeric_types = (pl.Int64, pl.Float64, pl.Int32, pl.Float32)
    assert df.schema['creation'] in numeric_types
    assert df.schema['redemption'] in numeric_types

    # Structural assertion checking matrix ranges
    bounds_check = df.select(pl.col('redemption') <= pl.col('creation'))[
        'redemption'
    ]
    assert bounds_check.all(), (
        'Redemption price found exceeding creation price'
    )

    if has_statistical:
        assert df.schema['statistical'] in numeric_types


async def validate_live_navps(site: BaseSite):
    d = await site.live_navps()
    validate_dict(d, LiveNAVPS)
    assert d['creation'] > d['redemption']


def test_from_l18():
    assert BaseSite.from_l18('استیل').url == 'https://mofidsectorfund.com/'


async def assert_leveraged_leverage(site: BaseSite):
    lev = await site.leverage()
    assert isinstance(lev, float)
    assert lev > 1.0


def assert_dividend_history(df_or_lf: pl.DataFrame | pl.LazyFrame):
    # Handle lazy evaluation input safely
    df = df_or_lf.collect() if isinstance(df_or_lf, pl.LazyFrame) else df_or_lf

    expected_schema = {
        'fundId': pl.Int64,
        'fundUnit': pl.Int64,
        'profitGuaranteeUnit': pl.Int64,
        'unitProfit': pl.Int64,
        'extraProfit': pl.Int64,
        'sumUnitProfit': pl.Int64,
        'sumExtraProfit': pl.Int64,
        'sumProfitGuarantee': pl.Int64,
        'sumAllProfit': pl.Int64,
    }

    # Verify exact column order and type matching across required fields
    for col_name, expected_type in expected_schema.items():
        assert df.schema[col_name] == expected_type, (
            f'Mismatched type for {col_name}'
        )

    assert_date_column(df, col_name='profitDate')
