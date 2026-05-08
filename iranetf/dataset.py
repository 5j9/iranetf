__version__ = '0.29.1.dev1'
from asyncio import gather as _gather, sleep as _sleep
from contextlib import contextmanager as _contextmanager
from logging import Logger as _Logger
from pathlib import Path as _Path

import pandas as _pd
from aiohttp import (
    ClientConnectorDNSError as _ClientConnectorDNSError,
    ClientConnectorError as _ClientConnectorError,
    ClientError as _ClientError,
    ClientResponseError as _ClientResponseError,
    ServerDisconnectedError as _ServerDisconnectedError,
)
from aiohutils import logger as _aiohutils_logger
from pandas import (
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
from iranetf import (
    RegNoError as _RegNoError,
    logger as _logger,
    sites as _sites,
)
from iranetf.sites import (
    BaseSite as _BaseSite,
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
    yk_tt = str.maketrans({'ي': 'ی', 'ك': 'ک'})
    ds['l18'] = ds['l18'].str.translate(yk_tt)
    ds['name'] = ds['name'].str.translate(yk_tt)

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


def _log_and_retry(func):
    retry = 3

    async def wrapper(*args):
        nonlocal retry
        arg = args[0]
        while True:
            try:
                return await func(*args)
            # ClientConnectorError is usually raise after
            # OSError(22, 'The semaphore timeout period has expired', None, 121, None))
            except (
                _ClientConnectorDNSError,
                _ClientConnectorError,
                _ServerDisconnectedError,
            ) as e:
                if retry <= 0:
                    _logger.error(f'{type(e).__name__} for {arg}')
                    return
                retry -= 1
                _logger.debug(f'retrying {type(e).__name__} for {arg}')
                await _sleep(2)
                continue
            except _ClientResponseError as e:
                if e.status == 429 and retry > 0:
                    await _sleep(5)
                    retry -= 1
                    continue
                _logger.error(f'status {e.status} on {arg}')
                return
            except (
                OSError,
                _ClientError,
            ) as e:
                _logger.error(f'{e!r} on {arg}')
                return
            except Exception as e:
                _logger.exception(f'{e!r} on {arg}')
                return

    return wrapper


@_log_and_retry
async def _check_validity(site: _BaseSite) -> tuple[str, str] | None:
    await site.live_navps()
    last_url = site.last_response.url  # to avoid redirected URLs
    return f'{last_url.scheme}://{last_url.host}/', type(site).__name__


# sorted from most common to least common
SITE_TYPES = (_RayanHamafza, _TadbirPardaz, _LeveragedTadbirPardaz, _MabnaDP2)


@_contextmanager
def set_level(logger: _Logger, level: str | int):
    old = logger.level
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old)


async def _url_type(domain: str) -> tuple:
    coros = [
        _check_validity(SiteType(f'http://{domain}/'))
        for SiteType in SITE_TYPES
    ]
    results = await _gather(*coros)

    for result in results:
        if result is not None:
            return result

    _logger.warning(f'_url_type failed for {domain}')
    return None, None


async def _add_url_and_type(
    fipiran_df: _DataFrame, known_domains: _Series | None
):
    domains_to_be_checked = fipiran_df['domain'][~fipiran_df['domain'].isna()]
    if known_domains is not None:
        domains_to_be_checked = domains_to_be_checked[
            ~domains_to_be_checked.isin(known_domains)
        ]

    _logger.info(
        f'checking site types of {len(domains_to_be_checked)} domains'
    )
    if domains_to_be_checked.empty:
        return

    # there will be a lot of redirection warnings, let's silent them
    with set_level(_aiohutils_logger, 'ERROR'):
        list_of_tuples = await _gather(
            *[_url_type(d) for d in domains_to_be_checked]
        )

    url, site_type = zip(*list_of_tuples)
    fipiran_df.loc[:, ['url', 'siteType']] = _DataFrame(
        {'url': url, 'siteType': site_type}, index=domains_to_be_checked.index
    )


async def _add_ins_code(new_items: _DataFrame) -> None:
    names_without_code = new_items[new_items['insCode'].isna()].name
    if names_without_code.empty:
        return
    _logger.info('searching names on tsetmc to find their insCode')
    results = await _gather(
        *[_tsetmc_search(name) for name in names_without_code]
    )
    ins_codes = [(None if len(r) != 1 else r[0]['insCode']) for r in results]
    new_items.loc[names_without_code.index, 'insCode'] = ins_codes


async def _fipiran_data(ds: _DataFrame) -> _DataFrame:
    import fipiran.funds

    _logger.info('await fipiran.funds.funds()')
    fipiran_df = await fipiran.funds.funds()

    reg_not_in_fipiran = ds[~ds['regNo'].isin(fipiran_df['regNo'])]
    if not reg_not_in_fipiran.empty:
        _logger.warning(
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
        inplace=True,
        errors='raise',
    )

    df['type'] = df['type'].replace(_ETF_TYPES)

    return df


async def _tsetmc_dataset() -> _DataFrame:
    from tsetmc.dataset import LazyDS, update

    _logger.info('await tsetmc.dataset.update()')
    await update()

    df = LazyDS.df
    df.drop(columns=['l30', 'isin', 'cisin'], inplace=True)
    return df


def _add_new_items_to_ds(new_items: _DataFrame, ds: _DataFrame) -> _DataFrame:
    ds.set_index('insCode', inplace=True)
    if new_items.empty:
        return ds
    new_with_code = new_items[new_items['insCode'].notna()]
    if not new_with_code.empty:
        ds = _concat(
            [ds, new_with_code.set_index('insCode').drop(columns=['domain'])]
        )
    else:
        _logger.info('new_with_code is empty!')
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

    ds.set_index('regNo', inplace=True)
    df = fipiran_df.set_index('regNo')
    ds['domain'] = None
    # to add fipiran urls and names to ds
    ds.update(df, overwrite=False)

    # ds['type'] = fipiran_df['type'] will create NA values in type column.
    common_regno = df.index.intersection(ds.index)
    ds.loc[common_regno, 'type'] = df.loc[common_regno, 'type']

    # use domain as URL for those who do not have any URL
    na_urls = ds[ds['url'].isna()].index
    ds.loc[na_urls, 'url'] = 'http://' + ds.loc[na_urls, 'domain'] + '/'
    ds.reset_index(inplace=True)
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


@_log_and_retry
async def _check_site_type(site: _BaseSite) -> None:
    detected = await _BaseSite.from_url(site.url)
    if type(detected) is not type(site):
        _logger.error(
            f'Detected site type for {site.url} is {type(detected).__name__},'
            f' but dataset site type is {type(site).__name__}.'
        )
    if isinstance(site, _MabnaDP2):
        portfolios = await site.portfolios()
        if site.portfolio_id not in portfolios:
            _logger.error(f'site.portfolio_id not in portfolio_ids for {site}')


@_log_and_retry
async def _check_reg_no(site: _BaseSite, ds_reg_no: str):
    try:
        site_reg_no = await site.reg_no()
    except _RegNoError:
        _logger.error(f'RegNoError on {site}')
        return
    if ds_reg_no == site_reg_no:
        return
    _logger.error(f'{site_reg_no=} != {ds_reg_no=}')


@_log_and_retry
async def _check_portfolio_counts(site: _BaseSite, dataset_ids: set[str]):
    """
    Fetch portfolio counts from website and validate exact match with dataset.

    Ensures:
    1. Every portfolio ID from website exists in dataset
    2. Every portfolio ID from dataset exists on website
    3. Counts match exactly
    """
    site_portfolios = (
        await site.portfolios()
    )  # dict: {'2': 'Gold', '11': 'Silver'}
    site_ids = site_portfolios.keys()

    # Get dataset info for this site
    url = site.url

    if dataset_ids == {''}:
        dataset_ids = {'1'}

    # Validation 2: IDs must match exactly (set equality)
    if site_ids == dataset_ids:
        return
    _logger.error(f'{url}: Portfolio ID mismatch! {dataset_ids=} {site_ids=}')


async def check_dataset(live=False):
    ds = load_dataset(site=False)
    assert ds['l18'].is_unique
    assert ds['name'].is_unique, ds['name'][ds['name'].duplicated()]
    assert ds['type'].unique().isin(_ETF_TYPES.values()).all()  # type: ignore
    assert ds['insCode'].is_unique
    assert ds['url'].is_unique
    assert not ds['regNo'].isna().any()

    # same regNo -> same base url
    ds[['base_url', 'portfolio_id']] = ds['url'].str.partition('#')[[0, 2]]
    assert (
        ds.groupby('regNo').filter(lambda g: g['base_url'].nunique() > 1).empty
    )

    if not live:
        return
    ds = ds.join(
        ds.groupby('base_url')['portfolio_id']
        .agg(list)
        .rename('portfolio_ids'),
        on='base_url',
    )

    ds['site'] = ds[ds['siteType'].notna()].apply(_make_site, axis=1)

    check_site_coros = [_check_site_type(s) for s in ds['site']]
    check_reg_no_coros = [
        _check_reg_no(site, reg)
        for (site, reg) in zip(ds['site'], ds['regNo'])
    ]

    unique_site_pids = ds.loc[
        ~ds['base_url'].duplicated(), ['site', 'portfolio_ids']
    ]
    collect_symbol_counts_coros = [
        _check_portfolio_counts(s, set(dataset_ids))
        for (s, dataset_ids) in zip(
            unique_site_pids['site'], unique_site_pids['portfolio_ids']
        )
    ]

    orig_ssl = iranetf.ssl
    iranetf.ssl = False  # many sites fail ssl verification
    try:
        await _gather(*check_site_coros)
        await _gather(*check_reg_no_coros)
        await _gather(*collect_symbol_counts_coros)
    finally:
        iranetf.ssl = orig_ssl

    if not (no_site := ds[ds['site'].isna()]).empty:
        _logger.warning(
            f'some dataset entries have no associated site:\n{no_site["l18"]}'
        )
