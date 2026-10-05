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
        'diffCancelNavAndExhibitiveNAV',
        'diffCancelNavAndExhibitiveNAVPercent',
    ]
    assert_date_column(df)


@file_map(
    (aram.url, 'aram.html'),
    ('daily-asset-percentage', 'daily_asset_percentage.json'),
)
async def test_asset_allocation():
    cash = await aram.cash()
    assert 0.0 < cash < 0.6
