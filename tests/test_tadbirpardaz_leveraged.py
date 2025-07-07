from datetime import date
from math import isclose

from aiohutils.tests import assert_dict_type, file, files
from numpy import dtype

from iranetf import (
    BaseSite,
    LeveragedTadbirPardaz,
    LeveragedTadbirPardazLiveNAVPS,
)
from tests import assert_leveraged_leverage, assert_navps_history
from tests.test_tadbirpardaz import EXPECTED_TP_VER

tavan: LeveragedTadbirPardaz = BaseSite.from_l18('توان')  # type: ignore
ahrom: LeveragedTadbirPardaz = BaseSite.from_l18('اهرم')  # type: ignore


@file('ahrom_live.json')
async def test_live_navps_leveraged():
    live = await ahrom.live_navps()
    assert_dict_type(live, LeveragedTadbirPardazLiveNAVPS)


@file('ahrom_navps_history.json')
async def test_navps_history_leveraged():
    # leveraged ETFs do not have statistical history for preferred shares
    await assert_navps_history(ahrom, has_statistical=False)


@file('tavan_live.json')
async def test_float_base_units_value():
    nav = await tavan.live_navps()
    assert type(nav['BaseUnitsCancelNAV']) is float


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
    df = await site.navps_history()
    assert len(df) > 5000
    assert df.index.is_unique


@file('nav_history.xlsx')
async def test_nav_history():
    site: LeveragedTadbirPardaz = BaseSite.from_l18('شتاب')  # type: ignore
    df = await site.nav_history(from_=date(2025, 7, 1), to=date(2025, 7, 4))
    assert df.index.name == 'Date'
    assert df.index.dtype == 'datetime64[ns]'
    assert [*df.dtypes.items()] == [
        ('Number of Investors in Common Units', dtype('int64')),
        ('Leverage Ratio', dtype('float64')),
        ('Unnamed: 2', dtype('float64')),
        ('Total Value of Units (RIAL)', dtype('int64')),
        ('Outstanding Common Certificates', dtype('int64')),
        ('Outstanding Preferred Certificates', dtype('int64')),
        ('Number of Canceled Common Units', dtype('int64')),
        ('Number of Issued Common Units', dtype('int64')),
        ('Number of Canceled Preferred Units', dtype('int64')),
        ('Number of Issued Preferred Units', dtype('int64')),
        ('NAV of Common Units', dtype('float64')),
        ('NAV of Preferred Units', dtype('float64')),
        ('NAV of Fund', dtype('int64')),
        ('Annualized Return of Common Units', dtype('float64')),
        ('Annualized Return of Preferred Units', dtype('float64')),
        ('Annualized Return of Fund', dtype('float64')),
        ('Price of Common Units', dtype('float64')),
        ('Cancellation Price of Preferred Units', dtype('int64')),
        ('Issuance Price of Preferred Units', dtype('int64')),
        ('Unnamed: 19', dtype('float64')),
        ('Unnamed: 21', dtype('float64')),
        ('Row', dtype('int64')),
    ]
    assert len(df) == 4
