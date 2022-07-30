from numpy import dtype
from pandas import CategoricalDtype, Int64Dtype, StringDtype

from iranetf.ravest import funds, fund_portfolio_report_latest,\
    funds_deviation_week_month, funds_trade_price, fund_trade_info, companies
from test.aiohttp_test_utils import file


string = StringDtype()
Int64 = Int64Dtype()


@file('getFunds.json')
async def test_funds():
    df = await funds()
    assert len(df) > 80
    # NameDisplay needs to be stripped
    # Pycharm false positive:
    # noinspection PyUnresolvedReferences
    assert (df.NameDisplay.str.strip() == df.NameDisplay).all()
    assert [*df.dtypes.items()] == [
        ('Id', dtype('int64')),
        ('Name', dtype('O')),
        ('NamePersian', dtype('O')),
        ('NameDisplay', string),
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
        ('TsetmcId', dtype('int64')),
        ('Url', string),
        ('StartDate', dtype('<M8[ns]')),
        ('Manager', dtype('O')),
        ('Labels', string),
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
        ('CreateDate', dtype('<M8[ns]')),
        ('UpdateDate', dtype('<M8[ns]')),
        ('CreateBy', dtype('float64')),
        ('UpdateBy', dtype('int64')),
        ('IsActive', dtype('bool')),
        ('IsArchive', dtype('bool'))]


@file('latestPortfolio1497.json')
async def test_fund_portfolio_report_latest():
    df = await fund_portfolio_report_latest(1497)
    assert [*df.dtypes.items()] == [
        ('Id', dtype('int64')),
        ('CompanyId', 'Int64'),
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


@file('fundPriceAndNavDeviation.json')
async def test_funds_deviation_week_month():
    week, month = await funds_deviation_week_month()
    assert [*week.dtypes.items()] == [
        ('symbol', dtype('O')),
        ('fundType', dtype('O')),
        ('from', dtype('O')),
        ('to', dtype('O')),
        ('data', dtype('float64'))]


@file('tradePrice.json')
async def test_funds_trade_price():
    df = await funds_trade_price()
    assert [*df.dtypes.items()] == [
        ('symbol', string),
        ('tradePrice', dtype('float64')),
        ('priceDiff', dtype('float64')),
        ('nav', dtype('float64')),
        ('navDiff', dtype('float64')),
        ('priceAndNavDiff', dtype('float64')),
        ('fundType', CategoricalDtype(
            categories=['ETC', 'ETF', 'FixedIncome', 'Mix'], ordered=False
        ))]


@file('GetCompanyStockTradeInfo1437_1.json')
async def test_get_company_stock_trade_info():
    df = await fund_trade_info(1437, 1)
    assert [*df.dtypes.items()] == [
        ('Id', dtype('int64')),
        ('TsetmcId', Int64),
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
        ('Date', dtype('<M8[ns]')),
        ('CompanyId', dtype('int64')),
        ('CreateDate', dtype('O')),
        ('UpdateDate', dtype('O')),
        ('CreateBy', dtype('O')),
        ('UpdateBy', dtype('O'))]


@file('company.json')
async def test_companies():
    df = await companies()
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
        ('TsetmcId', Int64),
        ('Url', dtype('O')),
        ('StartDate', dtype('<M8[ns]')),
        ('Manager', dtype('O')),
        ('Labels', string),
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
