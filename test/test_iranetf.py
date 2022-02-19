from numpy import dtype

from iranetf import funds, fund_portfolio_report_latest,\
    funds_deviation_week_month, funds_trade_price, fund_trade_info, companies
from test import offline_mode, get_patch


def setup_module():
    offline_mode.start()


def teardown_module():
    offline_mode.stop()


@get_patch('getFunds.json')
def test_funds():
    df = funds()
    assert len(df) > 80
    # NameDisplay needs to be stripped
    assert (df.NameDisplay.str.strip() == df.NameDisplay).all()
    assert [*df.dtypes.items()] == [
        ('Id', dtype('int64')),
        ('Name', dtype('O')),
        ('NamePersian', dtype('O')),
        ('NameDisplay', 'string[python]'),
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
        ('FundType', dtype('int64')),
        ('FundTypeDesc', dtype('O')),
        ('TradeType', dtype('int64')),
        ('TradeTypeDesc', dtype('O')),
        ('SiteType', dtype('float64')),
        ('SiteTypeDesc', dtype('O')),
        ('hasReturn', dtype('O')),
        ('CreateDate', 'datetime64[ns, UTC]'),
        ('UpdateDate', 'datetime64[ns, UTC]'),
        ('CreateBy', dtype('float64')),
        ('UpdateBy', dtype('int64')),
        ('IsActive', dtype('bool')),
        ('IsArchive', dtype('bool'))]


@get_patch('latestPortfolio1497.json')
def test_fund_portfolio_report_latest():
    df = fund_portfolio_report_latest(1497)
    assert [*df.dtypes.items()] == [
        ('Id', dtype('int64')),
        ('CompanyId', dtype('int64')),
        ('CompanySymbol', dtype('O')),
        ('CompanyNamePersian', dtype('O')),
        ('CompanyIndustryCode', dtype('O')),
        ('CompanyIndustryName', dtype('O')),
        ('CompanySubIndustryCode', dtype('O')),
        ('CompanySubIndustryName', dtype('O')),
        ('Type', dtype('int64')),
        ('ChangeType', dtype('int64')),
        ('TypeDesc', dtype('O')),
        ('FundPortfolioReportId', dtype('int64')),
        ('FundPortfolioReportFromDate', dtype('O')),
        ('FundPortfolioReportToDate', dtype('O')),
        ('FundPortfolioReportFundCompanySymbol', dtype('O')),
        ('FundPortfolioReportCompanyId', dtype('int64')),
        ('FundPortfolioReportFundCompanyNamePersian', dtype('O')),
        ('BuyCount', dtype('int64')),
        ('BuyValue', dtype('float64')),
        ('BuyValuePercent', dtype('float64')),
        ('SellCount', dtype('int64')),
        ('SellValue', dtype('float64')),
        ('SellValuePercent', dtype('float64')),
        ('StartCount', dtype('int64')),
        ('StartValue', dtype('float64')),
        ('StartValuePercent', dtype('float64')),
        ('EndCount', dtype('int64')),
        ('EndValue', dtype('float64')),
        ('EndValuePercent', dtype('float64')),
        ('ChangeCount', dtype('int64')),
        ('ChangePercent', dtype('float64')),
        ('ReturnPercent', dtype('float64')),
        ('BuyUnitValue', dtype('float64')),
        ('SellUnitValue', dtype('float64')),
        ('CreateDate', dtype('O')),
        ('UpdateDate', dtype('O')),
        ('CreateBy', dtype('int64')),
        ('UpdateBy', dtype('O'))]


@get_patch('fundPriceAndNavDeviation.json')
def test_funds_deviation_week_month():
    week, month = funds_deviation_week_month()
    assert [*week.dtypes.items()] == [
        ('symbol', dtype('O')),
        ('fundType', dtype('O')),
        ('from', dtype('O')),
        ('to', dtype('O')),
        ('data', dtype('float64'))]


@get_patch('tradePrice.json')
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


@get_patch('GetCompanyStockTradeInfo1437_1.json')
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


@get_patch('company.json')
def test_companies():
    df = companies()
    assert len(df) > 2000
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
