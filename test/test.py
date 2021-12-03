from unittest.mock import patch

from numpy import dtype
# noinspection PyProtectedMember
from iranetf import get_funds, _YK, _loads, fund_portfolio_report_latest,\
    funds_deviation_week_month, funds_trade_price, get_company_stock_trade_info


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
    df = get_funds()
    assert len(df) == 75


@patch_get_content('latestPortfolio1497.json')
def test_fund_portfolio_report_latest():
    df = fund_portfolio_report_latest(1497)
    assert len(df.columns) == 34


@patch_get_content('fundPriceAndNavDeviation.json')
def test_funds_deviation_week_month():
    week, month = funds_deviation_week_month()
    assert week.columns.tolist() == [
        'symbol', 'fundType', 'from', 'to', 'data']


@patch_get_content('tradePrice.json')
def test_funds_trade_price():
    df = funds_trade_price()
    assert df.dtypes.tolist() == [
        dtype('O'),
        dtype('uint32'),
        dtype('float64'),
        dtype('uint32'),
        dtype('float64'),
        dtype('float64'),
        dtype('O')]


@patch_get_content('GetCompanyStockTradeInfo1437_1.json')
def test_get_company_stock_trade_info():
    df = get_company_stock_trade_info(1437, 1)
    assert df.dtypes.to_dict() == {
        'Id': dtype('int64'),
        'TsetmcId': dtype('O'),
        'IrCode': dtype('O'),
        'Symbol': dtype('O'),
        'CompanyName': dtype('O'),
        'LastUpdateTime': dtype('O'),
        'OpenPrice': dtype('float64'),
        'ClosePrice': dtype('float64'),
        'TradePrice': dtype('float64'),
        'TradeQuantity': dtype('float64'),
        'TradesVolume': dtype('float64'),
        'TradesPrice': dtype('float64'),
        'MinPrice': dtype('float64'),
        'MaxPrice': dtype('float64'),
        'YesterdayPrice': dtype('float64'),
        'EPS': dtype('float64'),
        'MaxAllowedPrice': dtype('float64'),
        'MinAllowedPrice': dtype('float64'),
        'NAV': dtype('float64'),
        'IndustryCode': dtype('O'),
        'Date': 'datetime64[ns, UTC]',
        'CompanyId': dtype('int64'),
        'CreateDate': dtype('O'),
        'UpdateDate': dtype('O'),
        'CreateBy': dtype('O'),
        'UpdateBy': dtype('O')}
