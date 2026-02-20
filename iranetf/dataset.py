__version__ = '0.29.1.dev1'
import logging as _logging
from asyncio import gather as _gather
from json import JSONDecodeError as _JSONDecodeError
from logging import (
    error as _error,
    exception as _excepton,
    info as _info,
    warning as _warning,
)
from pathlib import Path as _Path

import pandas as _pd
from aiohttp import (
    ClientConnectorError as _ClientConnectorError,
    ClientOSError as _ClientOSError,
    ClientResponseError as _ClientResponseError,
    ServerDisconnectedError as _ServerDisconnectedError,
    ServerTimeoutError as _ServerTimeoutError,
    TooManyRedirects as _TooManyRedirects,
)
from pandas import (
    NA as _NA,
    DataFrame as _DataFrame,
    Series as _Series,
    concat as _concat,
    read_csv as _read_csv,
)
from tsetmc.instruments import (
    Instrument as _Instrument,
    search as _tsetmc_search,
)

import iranetf
from iranetf import sites as _sites
from iranetf.sites import (
    BaseSite as _BaseSite,
    BaseTadbirPardaz as _BaseTadbirPardaz,
    FundType as _FundType,
    LeveragedTadbirPardaz as _LeveragedTadbirPardaz,
    MabnaDP2 as _MabnaDP2,
    RayanHamafza as _RayanHamafza,
    TadbirPardaz as _TadbirPardaz,
)

_ETF_TYPES = {  # numbers are according to fipiran
    6: 'Stock',
    4: 'Fixed',
    7: 'Mixed',
    5: 'Commodity',
    17: 'FOF',
    18: 'REIT',
    21: 'Sector',
    22: 'Leveraged',
    23: 'Index',
    24: 'Guarantee',
}


_DATASET_PATH = _Path(__file__).parent / 'dataset.csv'


def _make_site(row) -> _BaseSite:
    type_str = row['siteType']
    site_class = getattr(_sites, type_str)
    return site_class(row['url'])


def load_dataset(*, site=True, inst=False) -> _DataFrame:
    """Load dataset.csv as a DataFrame.

    If site is True, convert url and siteType columns to site object.
    """
    df = _read_csv(
        _DATASET_PATH,
        encoding='utf-8-sig',
        low_memory=False,
        lineterminator='\n',
        dtype={
            'l18': 'string',
            'name': 'string',
            'type': _pd.CategoricalDtype([*_ETF_TYPES.values()]),
            'insCode': 'string',
            'regNo': 'string',
            'url': 'string',
            'siteType': 'category',
            'dps_interval': 'Int8',
        },
    )

    if site:
        df['site'] = df[df['siteType'].notna()].apply(_make_site, axis=1)

    if inst:
        df['inst'] = df['insCode'].apply(_Instrument)  # type: ignore

    return df


def save_dataset(ds: _DataFrame):
    ds[
        [  # sort columns
            'l18',
            'name',
            'type',
            'insCode',
            'regNo',
            'url',
            'siteType',
            'dps_interval',
        ]
    ].sort_values('l18').to_csv(
        _DATASET_PATH, lineterminator='\n', encoding='utf-8-sig', index=False
    )


async def _check_validity(site: _BaseSite, retry=0) -> tuple[str, str] | None:
    try:
        await site.live_navps()
    except (
        TimeoutError,
        _JSONDecodeError,
        _ClientConnectorError,
        _ServerTimeoutError,
        _ClientOSError,
        _TooManyRedirects,
        _ServerDisconnectedError,
        _ClientResponseError,
    ):
        if retry > 0:
            return await _check_validity(site, retry - 1)
        return None
    last_url = site.last_response.url  # to avoid redirected URLs
    return f'{last_url.scheme}://{last_url.host}/', type(site).__name__


# sorted from most common to least common
SITE_TYPES = (_RayanHamafza, _TadbirPardaz, _LeveragedTadbirPardaz, _MabnaDP2)


async def _url_type(domain: str) -> tuple:
    coros = [
        _check_validity(SiteType(f'http://{domain}/'), 2)
        for SiteType in SITE_TYPES
    ]
    results = await _gather(*coros)

    for result in results:
        if result is not None:
            return result

    _warning(f'_url_type failed for {domain}')
    return None, None


async def _add_url_and_type(
    fipiran_df: _DataFrame, known_domains: _Series | None
):
    domains_to_be_checked = fipiran_df['domain'][~fipiran_df['domain'].isna()]
    if known_domains is not None:
        domains_to_be_checked = domains_to_be_checked[
            ~domains_to_be_checked.isin(known_domains)
        ]

    _info(f'checking site types of {len(domains_to_be_checked)} domains')
    if domains_to_be_checked.empty:
        return

    # there will be a lot of redirection warnings, let's silent them
    _logging.disable()  # to disable redirection warnings
    list_of_tuples = await _gather(
        *[_url_type(d) for d in domains_to_be_checked]
    )
    _logging.disable(_logging.NOTSET)

    url, site_type = zip(*list_of_tuples)
    fipiran_df.loc[:, ['url', 'siteType']] = _DataFrame(
        {'url': url, 'siteType': site_type}, index=domains_to_be_checked.index
    )


async def _add_ins_code(new_items: _DataFrame) -> None:
    names_without_code = new_items[new_items['insCode'].isna()].name
    if names_without_code.empty:
        return
    _info('searching names on tsetmc to find their insCode')
    results = await _gather(
        *[_tsetmc_search(name) for name in names_without_code]
    )
    ins_codes = [(None if len(r) != 1 else r[0]['insCode']) for r in results]
    new_items.loc[names_without_code.index, 'insCode'] = ins_codes


async def _fipiran_data(ds: _DataFrame) -> _DataFrame:
    import fipiran.funds

    _info('await fipiran.funds.funds()')
    fipiran_df = await fipiran.funds.funds()

    reg_not_in_fipiran = ds[~ds['regNo'].isin(fipiran_df['regNo'])]
    if not reg_not_in_fipiran.empty:
        _warning(
            f'Some dataset rows were not found on fipiran:\n{reg_not_in_fipiran}'
        )

    df = fipiran_df[
        (fipiran_df['typeOfInvest'] == 'Negotiable')
        # 11: 'Market Maker', 12: 'VC', 13: 'Project', 14: 'Land and building',
        # 16: 'PE'
        & ~(fipiran_df['fundType'].isin((11, 12, 13, 14, 16)))
        & fipiran_df['isCompleted']
    ]

    df = df[
        [
            'regNo',
            'smallSymbolName',
            'name',
            'fundType',
            'websiteAddress',
            'insCode',
        ]
    ]

    df.rename(
        columns={
            'fundType': 'type',
            'websiteAddress': 'domain',
            'smallSymbolName': 'l18',
        },
        copy=False,
        inplace=True,
        errors='raise',
    )

    df['type'] = df['type'].replace(_ETF_TYPES)

    return df


async def _tsetmc_dataset() -> _DataFrame:
    from tsetmc.dataset import LazyDS, update

    _info('await tsetmc.dataset.update()')
    await update()

    df = LazyDS.df
    df.drop(columns=['l30', 'isin', 'cisin'], inplace=True)
    return df


def _add_new_items_to_ds(new_items: _DataFrame, ds: _DataFrame) -> _DataFrame:
    if new_items.empty:
        return ds
    new_with_code = new_items[new_items['insCode'].notna()]
    if not new_with_code.empty:
        ds = _concat(
            [ds, new_with_code.set_index('insCode').drop(columns=['domain'])]
        )
    else:
        _info('new_with_code is empty!')
    return ds


async def _update_existing_rows_using_fipiran(
    ds: _DataFrame, fipiran_df: _DataFrame, check_existing_sites: bool
) -> _DataFrame:
    """Note: ds index will be set to insCode."""
    await _add_url_and_type(
        fipiran_df,
        known_domains=None
        if check_existing_sites
        else ds['url'].str.extract('//(.*)/')[0],
    )

    # to update existing urls and names
    # NA values in regNo cause error later due to duplication
    regno = ds[~ds['regNo'].isna()].set_index('regNo')
    regno['domain'] = None
    regno.update(fipiran_df.set_index('regNo'))

    ds.set_index('insCode', inplace=True)
    # Do not overwrite MultiNAV type and URL.
    regno.set_index('insCode', inplace=True)
    ds.update(regno, overwrite=False)

    # Update ds types using fipiran values
    # ds['type'] = regno['type'] will create NA values in type column.
    common_indices = regno.index.intersection(ds.index)
    ds.loc[common_indices, 'type'] = regno.loc[common_indices, 'type']

    # use domain as URL for those who do not have any URL
    common_indices = ds[ds['url'].isna()].index.intersection(regno.index)
    ds.loc[common_indices, 'url'] = (
        'http://' + regno.loc[common_indices, 'domain'] + '/'
    )
    return ds


async def update_dataset(*, check_existing_sites=False) -> _DataFrame:
    """Update dataset and return newly found that could not be added."""
    ds = load_dataset(site=False)
    fipiran_df = await _fipiran_data(ds)
    ds = await _update_existing_rows_using_fipiran(
        ds, fipiran_df, check_existing_sites
    )

    new_items = fipiran_df[~fipiran_df['regNo'].isin(ds['regNo'])]

    tsetmc_df = await _tsetmc_dataset()
    await _add_ins_code(new_items)
    ds = _add_new_items_to_ds(new_items, ds)

    # update all data, old or new, using tsetmc_df
    ds.update(tsetmc_df)

    ds.reset_index(inplace=True)
    save_dataset(ds)

    return new_items[new_items['insCode'].isna()]


def _log_errors(func):
    async def wrapper(arg):
        try:
            return await func(arg)
        except OSError as e:
            _error(f'{e!r} on {arg}')
        except Exception as e:
            _excepton(f'Exception occurred during checking of {arg}: {e}')
            return None

    return wrapper


@_log_errors
async def _check_site_type(site: _BaseSite) -> None:
    detected = await _BaseSite.from_url(site.url)
    if type(detected) is not type(site):
        _error(
            f'Detected site type for {site.url} is {type(detected).__name__},'
            f' but dataset site type is {type(site).__name__}.'
        )
    if isinstance(site, _MabnaDP2):
        data = await site.home_data()
        portfolio_ids = data['__REACT_REDUX_STATE__']['general']['data'][
            'portfolioIds'
        ]
        if site.portfolio_id not in portfolio_ids:
            _error(f'site.portfolio_id not in portfolio_ids for {site}')


@_log_errors
async def _check_reg_no(row):
    ds_reg_no = row.regNo
    if ds_reg_no is _NA:  # todo: remove this after adding regNo for all
        return
    site: _BaseSite = row.site
    actual_reg_no = await site.reg_no()
    if ds_reg_no == actual_reg_no:
        return
    _error(f'regNo mismatch:\n {site.url=}\n {ds_reg_no=}\n {actual_reg_no=}')


_url_symbols: dict[str, dict[str, int]] = {}


@_log_errors
async def _collect_symbol_counts(site: _BaseSite):
    if (url := site.url) in _url_symbols:
        _url_symbols[url]['actual_count'] += 1
        return

    # set an initual value early to avoid race conditions
    _url_symbols[url] = {
        'expected_count': 1,
        'actual_count': 1,
    }

    if isinstance(site, _RayanHamafza):
        fund_data = await site.fund_data()
        if fund_data['FundType'] is _FundType.HYBRID:
            return  # expected_count == 1
        _url_symbols[url]['expected_count'] = len(fund_data['FundList'])
        return

    if isinstance(site, _BaseTadbirPardaz):
        home_info = await site.home_info()
        if home_info['isETFMultiNavMode']:
            home_info['basketIDs'].pop('1', None)  # ignore the overall basket
            _url_symbols[url]['expected_count'] = len(home_info['basketIDs'])
        return

    # for all other site types, assume site is not multi-nav


def _check_symbol_counts():
    """this function should be called after _gather_site_symbol_counts has been run for all sites"""
    for url, counts in _url_symbols.items():
        if counts['expected_count'] == counts['actual_count']:
            continue
        _error(f'{url=} symbol counts do not match: {counts}')


async def check_dataset(live=False):
    ds = load_dataset(site=False)
    assert ds['l18'].is_unique
    assert ds['name'].is_unique, ds['name'][ds['name'].duplicated()]
    assert ds['type'].unique().isin(_ETF_TYPES.values()).all()  # type: ignore
    assert ds['insCode'].is_unique
    assert ds['url'].is_unique
    reg_numbers = ds['regNo']
    known_reg_numbers = reg_numbers[reg_numbers.notna()]
    assert known_reg_numbers.is_unique, ds[known_reg_numbers.duplicated()]

    if not live:
        return

    ds['site'] = ds[ds['siteType'].notna()].apply(_make_site, axis=1)

    rows = [*ds.itertuples()]
    sites: list[_BaseSite] = [row.site for row in rows]  # type: ignore
    check_site_coros = [_check_site_type(s) for s in sites]
    check_reg_no_coros = [_check_reg_no(r) for r in rows]
    collect_symbol_counts_coros = [_collect_symbol_counts(s) for s in sites]

    orig_ssl = iranetf.ssl
    iranetf.ssl = False  # many sites fail ssl verification
    try:
        await _gather(*check_site_coros)
        await _gather(*check_reg_no_coros)
        await _gather(*collect_symbol_counts_coros)
    finally:
        iranetf.ssl = orig_ssl

    _check_symbol_counts()

    if not (no_site := ds[ds['site'].isna()]).empty:
        _warning(
            f'some dataset entries have no associated site:\n{no_site["l18"]}'
        )
