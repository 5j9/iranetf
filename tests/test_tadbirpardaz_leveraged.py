from datetime import date
from math import isclose

import polars as pl
from pytest_aiohutils import file, files

from iranetf.sites import (
    BaseSite,
    LeveragedTadbirPardaz,
)
from tests import (
    assert_date_column,  # Swapped from assert_date_index
    assert_leveraged_leverage,
    assert_navps_history,
    validate_live_navps,
)
from tests.test_tadbirpardaz import EXPECTED_TP_VER

tavan: LeveragedTadbirPardaz = BaseSite.from_l18('توان')  # type: ignore
ahrom: LeveragedTadbirPardaz = BaseSite.from_l18('اهرم')  # type: ignore


@file('ahrom_live.json')
async def test_live_navps_leveraged():
    await validate_live_navps(ahrom)


@file('ahrom_navps_history.json')
async def test_navps_history_leveraged():
    # leveraged ETFs do not have statistical history for preferred shares
    await assert_navps_history(ahrom, has_statistical=False)


@file('tavan_live.json')
async def test_float_base_units_value():
    nav = await tavan.live_navps()
    assert isinstance(nav['BaseUnitsCancelNAV'], float)


@file('leveraged_tadbir_version.html')
async def test_leveraged_version():
    assert (await ahrom.version()) == EXPECTED_TP_VER


@file('ahrom_aa.json')
async def test_asset_allocation():
    aa = await ahrom.asset_allocation()
    assert aa.keys() <= ahrom._aa_keys
    assert isclose(sum(aa.values()), 1.0)


@file('ahrom_aa.json')
async def test_cache():
    cache = await ahrom.cache()
    assert 0.0 <= cache <= 0.6


@files('ahrom_live.json', 'ahrom_aa.json')
async def test_leverage():
    await assert_leveraged_leverage(ahrom)


@file('duplicate_navps_hist.json')
async def test_pishran_navps_hist():
    site = BaseSite.from_l18('پیشران')
    lf = await site.navps_history()
    df = lf.collect()

    assert len(df) > 5000
    # Polars structural unique check evaluation
    assert df.select(pl.col('date').is_unique().all()).item()


@files(
    'shetab_nav_history_1.html',
    'shetab_nav_history_2.html',
    'shetab_nav_history_3.html',
)
async def test_nav_history():
    site: LeveragedTadbirPardaz = BaseSite.from_l18('شتاب')  # type: ignore
    lf = await site.nav_history(from_=date(2025, 7, 8), to=date(2025, 8, 26))
    df = lf.collect()

    assert_date_column(df)

    expected_schema = {
        'Row': pl.Float64,  # Cleansed via _clean_persian_numeric_expr
        'Issue Price': pl.Float64,
        'Redemption Price': pl.Float64,
        'Statistical Price': pl.Float64,
        'NAV of Premium Units Issued': pl.Float64,
        'NAV of Premium Units Redeemed': pl.Float64,
        'Statistical NAV of Premium Units': pl.Float64,
        'NAV of Normal Units': pl.Float64,
        'Net Asset Value of Fund': pl.Float64,
        'Net Asset Value of Premium Units': pl.Float64,
        'Net Asset Value of Normal Units': pl.Float64,
        'Number of Premium Units Issued': pl.Float64,
        'Number of Premium Units Redeemed': pl.Float64,
        'Number of Normal Units Issued': pl.Float64,
        'Number of Normal Units Redeemed': pl.Float64,
        'Remaining Premium Certificate': pl.Float64,
        'Remaining Normal Certificate': pl.Float64,
        'Total Fund Units': pl.Float64,
        'Leverage Ratio': pl.Float64,
        'Number of Normal Unit Investors': pl.Float64,
        # 'Unnamed_21': pl.String,
    }
    for col_name, expected_type in expected_schema.items():
        assert df.schema[col_name] == expected_type, (
            f'Mismatched type for {col_name}'
        )

    assert len(df) == 50
