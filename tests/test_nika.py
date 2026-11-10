import pytest
from pytest_aiohutils import file, file_map

from iranetf.sites import Nika
from tests import assert_navps_history, validate_live_navps

_site = Nika.from_l18('آرام')


@pytest.fixture
def aram():  # avoids home_info cache issues by creating new site objects
    return Nika(_site.url, _site.portfolio_id)


@file('aram_live_navps.json')
async def test_live_navps(aram):
    await validate_live_navps(aram)


@file('aram-nav.json')
async def test_navps_history(aram):
    await assert_navps_history(aram)


@file_map(
    (_site.url, 'aram.html'),
    ('instrument-type-percentage', 'instrument-type-percentage.json'),
)
async def test_asset_allocation(aram):
    aa = await aram.asset_allocation()
    pct_sum = sum(v for v in aa.values())
    assert pct_sum >= 95

    cash = await aram.cash()
    assert 0.0 < cash < 0.3


@file('aram.html')
async def test_portfolios(aram):
    assert await aram.portfolios() == {'11': 'صندوق سرمایه\u200cگذاری آرام'}
