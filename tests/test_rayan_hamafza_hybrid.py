from math import isclose
from unittest.mock import patch

from pytest_aiohutils import file, validate_dict

from iranetf.sites import (
    BaseSite,
    LiveNAVPS,
    RayanHamafza,
)
from tests import assert_navps_history

site: RayanHamafza = RayanHamafza.from_l18('ضمان')


@file('rhh_main.html')
async def test_rhh_from_url():
    assert type(await BaseSite.from_url(site.url) is RayanHamafza)


@file('rhh_live.json')
async def test_rhh_live_navps():
    d = await site.live_navps()
    validate_dict(d, LiveNAVPS)


@file('rhh_navps_history.json')
async def test_rhh_navps_history():
    await assert_navps_history(site)


@file('rhh_asset_allocation.json')
async def test_asset_allocation():
    aa = await site.asset_allocation()
    assert aa.keys() <= site._aa_keys
    assert type(aa.pop('JalaliDate')) is str
    assert isclose(sum(aa.values()), 1.0)


@file('rhh_asset_allocation.json')
async def test_cache():
    with patch.object(RayanHamafza, '_json', side_effect=site._json) as m:
        cache = await site.cache()
    m.assert_called_once_with('MixAsset/2')
    assert 0.0 <= cache <= 0.6
