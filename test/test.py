from unittest.mock import patch

from numpy import dtype
# noinspection PyProtectedMember
from iranetf import funds, _YK, _loads, fund_portfolio_report_latest,\
    funds_deviation_week_month, funds_trade_price, fund_trade_info, companies


get_patcher = patch(
    'iranetf._api_json', side_effect=NotImplementedError(
        'tests should not call get_content without patching'))


def setup_module():
    get_patcher.start()


def teardown_module():
    get_patcher.stop()


def patch_get_content(filename):
    with open(f'{__file__}/../testdata/{filename}', 'r', encoding='utf8') as f:
        text = f.read()
    return patch('iranetf._api_json', lambda _: _loads(text.translate(_YK)))


@patch_get_content('getFunds.json')
def test_get_funds():
    df = funds()
    assert len(df) == 75
    # NameDisplay needs to be stripped
    # noinspection PyUnresolvedReferences
    assert (df.NameDisplay.str.strip() == df.NameDisplay).all()


@patch_get_content('latestPortfolio1497.json')
def test_fund_portfolio_report_latest():
    df = fund_portfolio_report_latest(1497)
    assert len(df.columns) == 34


@patch_get_content('fundPriceAndNavDeviation.json')
def test_funds_deviation_week_month():
    week, month = funds_deviation_week_month()
    assert [*week.dtypes.items()] == [
        ('symbol', dtype('O')),
        ('fundType', dtype('O')),
        ('from', dtype('O')),
        ('to', dtype('O')),
        ('data', dtype('float64'))]


@patch_get_content('tradePrice.json')
def test_funds_trade_price():
    df = funds_trade_price()
    assert [*df.dtypes.items()] == [
        ('symbol', dtype('O')),
        ('tradePrice', dtype('uint32')),
        ('priceDiff', dtype('float64')),
        ('nav', dtype('uint32')),
        ('navDiff', dtype('float64')),
        ('priceAndNavDiff', dtype('float64')),
        ('fundType', dtype('O'))]


@patch_get_content('GetCompanyStockTradeInfo1437_1.json')
def test_get_company_stock_trade_info():
    df = fund_trade_info(1437, 1)
    assert [*df.dtypes.items()] == [
        ('Id', dtype('int64')),
        ('TsetmcId', dtype('O')),
        ('IrCode', dtype('O')),
        ('Symbol', dtype('O')),
        ('CompanyName', dtype('O')),
        ('LastUpdateTime', dtype('O')),
        ('OpenPrice', dtype('float64')),
        ('ClosePrice', dtype('float64')),
        ('TradePrice', dtype('float64')),
        ('TradeQuantity', dtype('float64')),
        ('TradesVolume', dtype('float64')),
        ('TradesPrice', dtype('float64')),
        ('MinPrice', dtype('float64')),
        ('MaxPrice', dtype('float64')),
        ('YesterdayPrice', dtype('float64')),
        ('EPS', dtype('float64')),
        ('MaxAllowedPrice', dtype('float64')),
        ('MinAllowedPrice', dtype('float64')),
        ('NAV', dtype('float64')),
        ('IndustryCode', dtype('O')),
        ('Date', 'datetime64[ns, UTC]'),
        ('CompanyId', dtype('int64')),
        ('CreateDate', dtype('O')),
        ('UpdateDate', dtype('O')),
        ('CreateBy', dtype('O')),
        ('UpdateBy', dtype('O'))]


@patch_get_content('company.json')
def test_companies():
    df = companies()
    assert len(df) == 2063
    assert [*df.dtypes.items()] == [
        ('Id', dtype('int64')),
        ('Name', dtype('O')),
        ('NamePersian', dtype('O')),
        ('NameDisplay', dtype('O')),
        ('SymbolLong', dtype('O')),
        ('Symbol', dtype('O')),
        ('SymbolCode', dtype('O')),
        ('SymbolFiveCode', dtype('O')),
        ('CompanyCode', dtype('O')),
        ('CompanyFourCode', dtype('O')),
        ('IndustryCode', dtype('O')),
        ('IndustryName', dtype('O')),
        ('SubIndustryCode', dtype('O')),
        ('SubIndustryName', dtype('O')),
        ('Market', dtype('O')),
        ('BoardCode', dtype('O')),
        ('TsetmcId', dtype('O')),
        ('Url', dtype('O')),
        ('StartDate', dtype('O')),
        ('Manager', dtype('O')),
        ('Labels', dtype('O')),
        ('IndustryId', dtype('O')),
        ('IsShare', dtype('bool')),
        ('IsFund', dtype('bool')),
        ('IsSpecial', dtype('O')),
        ('IsETF', dtype('bool')),
        ('FundType', dtype('float64')),
        ('FundTypeDesc', dtype('O')),
        ('TradeType', dtype('float64')),
        ('TradeTypeDesc', dtype('O')),
        ('SiteType', dtype('float64')),
        ('SiteTypeDesc', dtype('O')),
        ('hasReturn', dtype('O')),
        ('CreateDate', dtype('O')),
        ('UpdateDate', dtype('O')),
        ('CreateBy', dtype('float64')),
        ('UpdateBy', dtype('float64')),
        ('IsActive', dtype('bool')),
        ('IsArchive', dtype('bool'))]
