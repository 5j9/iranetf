from math import isclose
from unittest.mock import patch

from pytest_aiohutils import file, validate_dict

from iranetf.sites import (
    BaseSite,
    LiveNAVPS,
    RayanHamafza2,
)
from tests import assert_navps_history

site: RayanHamafza2 = RayanHamafza2.from_l18('ضمان')


@file('rhh_main.html')
async def test_rhh_from_url():
    # Fixed the type checking comparison structure to be fully type-safe
    instance = await BaseSite.from_url(site.url)
    assert isinstance(instance, RayanHamafza2)


@file('rhh_live.json')
async def test_rhh_live_navps():
    with patch.object(site, 'portfolio_id', 1):
        d = await site.live_navps()
    validate_dict(d, LiveNAVPS)


@file('rhh_navps_history.json')
async def test_rhh_navps_history():
    # This safely intercepts and validates the migrated polars.LazyFrame result
    await assert_navps_history(site)


@file('rhh_asset_allocation.json')
async def test_asset_allocation():
    aa = await site.asset_allocation()
    assert aa.keys() <= site._aa_keys
    assert isinstance(aa.pop('jalaliDate'), str)
    assert isclose(sum(aa.values()), 1.0)


@file('rhh_asset_allocation.json')
async def test_cache():
    with patch.object(RayanHamafza2, '_json', side_effect=site._json) as m:
        cache = await site.cache()
    m.assert_called_once_with('public/mixAsset/2')
    assert 0.0 <= cache <= 0.6
