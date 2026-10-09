from pytest import raises
from pytest_aiohutils import file, file_map

from iranetf.sites import Nika
from tests import assert_date_column, assert_navps_history

aram = Nika('https://aram.mofidfund.com/')


async def test_live_navps():
    with raises(NotImplementedError):
        await aram.live_navps()


@file('aram.html')
async def test_navps_history():
    await assert_navps_history(aram)


@file('aram.html')
async def test_assets_history():
    ah = await aram.assets_history()
    df = ah.collect()
    assert df.columns == [
        'date',
        'creation',
        'redemption',
        'statistical',
        'totalUnit',
        'totalIssuanceUnit',
        'totalRedemptionUnit',
        'issuanceUnitToday',
        'redemptionUnitToday',
        'redemptionNetAssetValue',
        'fundType',
        'totalInvestor',
        'remainingUnitsCount',
        'diffCancelNavAndExhibitiveNAV',
        'diffCancelNavAndExhibitiveNAVPercent',
    ]
    assert_date_column(df)


@file_map(
    (aram.url, 'aram.html'),
    ('daily-asset-percentage', 'daily_asset_percentage.json'),
)
async def test_asset_allocation():
    aa = await aram.asset_allocation()
    pct_sum = sum(v for (k, v) in aa.items() if k.endswith('Percent'))
    assert pct_sum >= 95
    cash = await aram.cash()
    assert 0.0 < cash < 0.3


@file('aram.html')
async def test_portfolios():
    assert await aram.portfolios() == {'1': 'صندوق سرمایه\u200cگذاری آرام'}
