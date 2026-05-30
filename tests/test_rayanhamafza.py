from math import isclose

import polars as pl
from pytest_aiohutils import file, files, validate_dict

from iranetf.sites import (
    BaseSite,
    FundData,
    RayanHamafza,
)
from tests import (
    assert_dividend_history,  # Corrected spelling to match core test utils file
    assert_navps_history,
    validate_live_navps,
)

roz = RayanHamafza('https://roz.fund/')


@file('roz_live.json')
async def test_live_navps():
    await validate_live_navps(roz)


@file('roz_navps_history.json')
async def test_navps_history():
    await assert_navps_history(roz)


@file('roz.html')
async def test_reg_no():
    assert await roz.reg_no() == '12452'


@file('toranj_profit.json')
async def test_fund_profit():
    # Returns a polars.LazyFrame or polars.DataFrame depending on the scraper design
    df_or_lf = await RayanHamafza('https://toranj.fund/').dividend_history()
    df = df_or_lf.collect()

    # Enforced type safety checks on schema configurations instead of in-place pandas popping
    assert df.schema['fundApId'] == pl.Int64
    clean_df = df.drop('fundApId')

    assert_dividend_history(clean_df)


feleza: RayanHamafza = BaseSite.from_l18('فلزا')  # type: ignore
ppadash: RayanHamafza = BaseSite.from_l18('پتروپاداش')  # type: ignore


@files('feleza.json', 'ppadash.json')
async def test_multinav():
    assert isinstance(ppadash, RayanHamafza)
    assert feleza.url == ppadash.url
    petro_nav = await feleza.live_navps()
    auto_nav = await ppadash.live_navps()
    assert petro_nav.keys() == auto_nav.keys()
    assert petro_nav != auto_nav


@file('ppadash_aa.json')
async def test_asset_allocation():
    aa = await feleza.asset_allocation()
    assert aa.keys() <= feleza._aa_keys
    assert isinstance(aa.pop('JalaliDate'), str)
    assert isclose(sum(aa.values()), 1.0)


@file('ppadash_aa.json')
async def test_cache():
    cache = await feleza.cache()
    assert 0.0 <= cache <= 0.6


@files('ppadash_aa.json')
async def test_leverage():
    assert isinstance(await feleza.leverage(), float)


@file('ppadash_fund_data.json')
async def test_fund_data():
    validate_dict(await feleza.fund_data(), FundData)


@file('ppadash_fund_data.json')
async def test_portfolios():
    ps = await feleza.portfolios()
    assert ps['2'] == 'صندوق فلزا'
