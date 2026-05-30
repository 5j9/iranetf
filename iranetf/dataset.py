from __future__ import annotations as _

from asyncio import gather as _gather, sleep as _sleep
from contextlib import contextmanager as _contextmanager
from json import JSONDecodeError
from logging import Logger as _Logger
from pathlib import Path as _Path

import polars as pl
from aiohttp import (
    ClientConnectorDNSError as _ClientConnectorDNSError,
    ClientConnectorError as _ClientConnectorError,
    ClientError as _ClientError,
    ClientResponseError as _ClientResponseError,
    ServerDisconnectedError as _ServerDisconnectedError,
)
from aiohutils import logger as _aiohutils_logger
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


def _make_site(row: dict) -> _BaseSite:
    type_str = row['siteType']
    site_class = getattr(_sites, type_str)
    return site_class(row['url'])


def read_dataset(*, site=True, inst=False) -> pl.LazyFrame:
    """Load dataset.csv as a LazyFrame.

    If site is True, convert url and siteType columns to site object.
    """
    # Enforces explicit schema and maps categorization values natively
    lf = pl.scan_csv(
        _DATASET_PATH,
        encoding='utf8',  # Polars strips the BOM automatically under "utf8"
        null_values=[''],
        schema={  # Changed from 'dtypes' to 'schema'
            'l18': pl.String,
            'name': pl.String,
            'type': pl.String,
            'insCode': pl.String,
            'regNo': pl.String,
            'url': pl.String,
            'siteType': pl.String,
            'dps_interval': pl.Int8,
        },
    )

    if site or inst:
        # Materialize momentarily to handle row-level complex custom Python Object mappings
        df = lf.collect()

        if site:
            # Map row dict structures via an explicit vectorized approach instead of axis=1
            df = df.with_columns(
                pl.struct(['siteType', 'url'])
                .map_elements(
                    lambda r: (
                        _make_site(r)
                        if r.get('siteType') is not None
                        else None
                    ),
                    return_dtype=pl.Object,
                )
                .alias('site')
            )

        if inst:
            df = df.with_columns(
                pl.col('insCode')
                .map_elements(
                    lambda c: _Instrument(c) if c is not None else None,
                    return_dtype=pl.Object,
                )
                .alias('inst')
            )
        return df.lazy()

    return lf


def write_dataset(ds: pl.LazyFrame | pl.DataFrame):
    """
    Collects the LazyFrame pipeline data and writes it back cleanly to disk.
    """
    df = ds.collect() if isinstance(ds, pl.LazyFrame) else ds

    # Fast, vectorized text translations in Polars
    df = df.with_columns(
        [
            pl.col('l18').str.replace_all('ي', 'ی').str.replace_all('ك', 'ک'),
            pl.col('name').str.replace_all('ي', 'ی').str.replace_all('ك', 'ک'),
        ]
    )

    columns_order = [
        'l18',
        'name',
        'type',
        'insCode',
        'regNo',
        'url',
        'siteType',
        'dps_interval',
    ]

    # Extract only matching dataset columns and sort purely via vector chainss
    df.select(columns_order).sort('l18').write_csv(
        _DATASET_PATH,
        include_bom=True,  # This replaces encoding='utf-8-sig' to protect Persian characters
    )


def _log_and_retry(func):
    retry = 3

    async def wrapper(*args):
        nonlocal retry
        arg = args[0]
        while True:
            try:
                return await func(*args)
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
            except (OSError, _ClientError) as e:
                _logger.error(f'{e!r} on {arg}')
                return
            except Exception as e:
                _logger.exception(f'{e!r} on {arg}')
                return

    return wrapper


@_log_and_retry
async def _check_validity(site: _BaseSite) -> tuple[str, str] | None:
    try:
        await site.live_navps()
    except JSONDecodeError:
        return
    last_url = site.last_response.url
    return f'{last_url.scheme}://{last_url.host}/', type(site).__name__


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

    _logger.warning(f'failed for {domain}')
    return None, None


async def _add_url_and_type(
    fipiran_lf: pl.LazyFrame, known_domains: list[str] | None
):
    fipiran_df = fipiran_lf.collect()

    # Filter domains using vector syntax
    domains_filter = fipiran_df['domain'].is_not_null()
    if known_domains is not None:
        domains_filter = domains_filter & (
            ~fipiran_df['domain'].is_in(known_domains)
        )

    domains_to_be_checked = fipiran_df.filter(domains_filter)[
        'domain'
    ].to_list()

    _logger.info(
        f'checking site types of {len(domains_to_be_checked)} domains'
    )
    if not domains_to_be_checked:
        return fipiran_df.lazy()

    with set_level(_aiohutils_logger, 'ERROR'):
        list_of_tuples = await _gather(
            *[_url_type(d) for d in domains_to_be_checked]
        )

    url_list, site_type_list = zip(*list_of_tuples)

    # Map back changes using a side table join instead of index-dependent .loc modifications
    updates_df = pl.DataFrame(
        {
            'domain': domains_to_be_checked,
            'url_new': url_list,
            'siteType_new': site_type_list,
        }
    )

    res_df = (
        fipiran_df.join(updates_df, on='domain', how='left')
        .with_columns(
            [
                pl.col('url_new').alias('url'),
                pl.col('siteType_new').alias('siteType'),
            ]
        )
        .drop(['url_new', 'siteType_new'])
    )

    return res_df.lazy()


async def _add_ins_code(new_items: pl.DataFrame) -> pl.DataFrame:
    names_without_code = new_items.filter(pl.col('insCode').is_null())[
        'name'
    ].to_list()
    if not names_without_code:
        return new_items

    _logger.info('searching names on tsetmc to find their insCode')
    results = await _gather(
        *[_tsetmc_search(name) for name in names_without_code]
    )
    ins_codes = [(None if len(r) != 1 else r[0]['insCode']) for r in results]

    codes_map = pl.DataFrame(
        {'name': names_without_code, 'insCode_new': ins_codes}
    )
    return (
        new_items.join(codes_map, on='name', how='left')
        .with_columns(pl.coalesce(['insCode_new', 'insCode']).alias('insCode'))
        .drop('insCode_new')
    )


async def _fipiran_data(ds: pl.LazyFrame) -> pl.LazyFrame:
    import fipiran.funds

    _logger.info('await fipiran.funds.funds()')
    # Use global inference scope for any incoming external dynamic dataframes
    fipiran_pd_df = await fipiran.funds.funds()
    fipiran_df = pl.from_pandas(fipiran_pd_df)

    ds_collected = ds.collect()
    reg_not_in_fipiran = ds_collected.filter(
        ~pl.col('regNo').is_in(fipiran_df['regNo'])
    )

    if not reg_not_in_fipiran.is_empty():
        _logger.warning(
            f'Some dataset rows were not found on fipiran:\n{reg_not_in_fipiran}'
        )

    df = fipiran_df.filter(
        (pl.col('typeOfInvest') == 'Negotiable')
        & ~(pl.col('fundType').is_in([11, 12, 13, 14, 16]))
        & pl.col('isCompleted')
    ).select(
        [
            pl.col('regNo'),
            pl.col('smallSymbolName').alias('l18'),
            pl.col('name'),
            pl.col('fundType').alias('type'),
            pl.col('websiteAddress').alias('domain'),
            pl.col('insCode'),
        ]
    )

    # Map mapping transformations via high performance native replacement steps
    df = df.with_columns(
        pl.col('type').replace(_ETF_TYPES, default=pl.col('type'))
    )
    return df.lazy()


async def _tsetmc_dataset() -> pl.LazyFrame:
    from tsetmc.dataset import LazyDS, update

    _logger.info('await tsetmc.dataset.update()')
    await update()
    lf = pl.from_pandas(LazyDS.df, include_index=True).lazy()
    return lf.drop(['l30', 'isin', 'cisin'])


def _add_new_items_to_ds(
    new_items: pl.DataFrame, ds: pl.DataFrame
) -> pl.DataFrame:
    if max(new_items.shape) == 0:
        return ds

    new_with_code = new_items.filter(pl.col('insCode').is_not_null()).drop(
        'domain'
    )
    if max(new_with_code.shape) > 0:
        # Align column structures dynamically and concatenate
        return pl.concat([ds, new_with_code], how='diagonal_relaxed')

    _logger.info('new_with_code is empty!')
    return ds


async def _update_existing_rows_using_fipiran(
    ds: pl.DataFrame, fipiran_df: pl.DataFrame, update_existing: bool
) -> pl.DataFrame:

    known_domains = None
    if not update_existing:
        known_domains = (
            ds.filter(pl.col('url').is_not_null())['url']
            .str.extract(r'//([^/]+)/')
            .drop_nulls()
            .to_list()
        )

    fipiran_lazy = await _add_url_and_type(fipiran_df.lazy(), known_domains)
    fipiran_df = fipiran_lazy.collect()

    # Join data streams using relational keys instead of legacy index overrides
    joined = ds.join(
        fipiran_df.select(['regNo', 'domain', 'type', 'url', 'siteType']),
        on='regNo',
        how='left',
        suffix='_fip',
    )

    # Coalesce values to overwrite fields safely without adding extra rows
    ds_updated = joined.with_columns(
        [
            pl.coalesce(['url', 'url_fip']).alias('url'),
            pl.coalesce(['siteType', 'siteType_fip']).alias('siteType'),
            pl.coalesce(['type_fip', 'type']).alias('type'),
            pl.col('domain'),
        ]
    ).drop(['url_fip', 'siteType_fip', 'type_fip'])

    # Build fallbacks if the primary URL structures are missing
    ds_updated = ds_updated.with_columns(
        pl.when(pl.col('url').is_null() & pl.col('domain').is_not_null())
        .then(pl.lit('http://') + pl.col('domain') + pl.lit('/'))
        .otherwise(pl.col('url'))
        .alias('url')
    )
    return ds_updated


async def update_dataset(*, update_existing=False) -> pl.DataFrame:
    """Update dataset and return newly found that could not be added."""
    ds = read_dataset(site=False).collect()
    fipiran_df = (await _fipiran_data(ds.lazy())).collect()

    ds = await _update_existing_rows_using_fipiran(
        ds, fipiran_df, update_existing
    )
    new_items = fipiran_df.filter(~pl.col('regNo').is_in(ds['regNo']))

    tsetmc_df = (await _tsetmc_dataset()).collect()
    new_items = await _add_ins_code(new_items)
    ds = _add_new_items_to_ds(new_items, ds)

    # Perform a left join update to bring over updated data tracks from TSETMC
    ds = ds.join(
        tsetmc_df,
        how='left',
        suffix='_tsetmc',
        left_on='insCode',
        right_on='ins_code',
    )

    # Coalesce tracking changes updates
    update_cols = [
        c for c in tsetmc_df.columns if c in ds.columns and c != 'insCode'
    ]
    for col in update_cols:
        ds = ds.with_columns(
            pl.coalesce([f'{col}_tsetmc', col]).alias(col)
        ).drop(f'{col}_tsetmc')

    write_dataset(ds)
    return new_items.filter(pl.col('insCode').is_null())


@_log_and_retry
async def _new_site_type(site: _BaseSite) -> str | None:
    new_site_type = type(await _BaseSite.from_url(site.url)).__name__
    if new_site_type != type(site).__name__:
        _logger.warning(
            f'Detected site type for {site.url} is {new_site_type},'
            f' but dataset site type is {type(site).__name__}.'
        )
        return new_site_type


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
    site_portfolios = await site.portfolios()
    site_ids = site_portfolios.keys()
    url = site.url

    if dataset_ids == {''}:
        dataset_ids = {'1'}

    if site_ids == dataset_ids:
        return
    _logger.error(f'{url}: Portfolio ID mismatch! {dataset_ids=} {site_ids=}')


async def check_dataset(live=False):
    ds = read_dataset(site=False).collect()

    # Guardrail Match: All validation checks collapsed to true single boolean scalars
    assert ds['l18'].is_unique().all()
    assert ds['name'].is_unique().all()
    assert ds['type'].is_in(list(_ETF_TYPES.values())).all()
    assert ds['insCode'].is_unique().all()
    assert ds['url'].is_unique().all()
    assert (ds['siteType'].is_not_null()).all(), 'siteType contains NA'
    assert (ds['regNo'].is_not_null()).all()

    # Split the URL string exactly once by the '#' character into a struct
    # containing 'field_0' and 'field_1'
    ds = (
        ds.with_columns(
            pl.col('url').str.split_exact('#', 1).alias('url_parts')
        )
        .with_columns(
            [
                # field_0 is everything before the '#'
                pl.col('url_parts').struct.field('field_0').alias('base_url'),
                # field_1 is everything after. If there was no '#', field_1 is null,
                # so we fill it with ''
                pl.col('url_parts')
                .struct.field('field_1')
                .fill_null('')
                .alias('portfolio_id'),
            ]
        )
        .drop('url_parts')
    )

    # Replaces Pandas groupby lambda filtering blocks with native relational expressions
    grouped_check = (
        ds.group_by('regNo')
        .agg(pl.col('base_url').n_unique().alias('cnt'))
        .filter(pl.col('cnt') > 1)
    )
    assert grouped_check.is_empty()

    if not live:
        return

    # Build list aggregations natively
    agg_pids = ds.group_by('base_url').agg(
        pl.col('portfolio_id').alias('portfolio_ids')
    )
    ds = ds.join(agg_pids, on='base_url', how='left')

    # Safely apply object mappings to generate your site list properties
    ds = ds.with_columns(
        pl.struct(['siteType', 'url'])
        .map_elements(lambda r: _make_site(r), return_dtype=pl.Object)
        .alias('site')
    )

    check_site_coros = [_new_site_type(s) for s in ds['site']]
    check_reg_no_coros = [
        _check_reg_no(site, reg)
        for (site, reg) in zip(ds['site'], ds['regNo'])
    ]

    unique_site_pids = ds.unique(subset=['base_url']).select(
        ['site', 'portfolio_ids']
    )
    collect_symbol_counts_coros = [
        _check_portfolio_counts(s, set(dataset_ids))
        for (s, dataset_ids) in zip(
            unique_site_pids['site'].to_list(),
            unique_site_pids['portfolio_ids'].to_list(),
        )
    ]

    orig_ssl = iranetf.ssl
    iranetf.ssl = False
    try:
        new_site_types = await _gather(*check_site_coros)
        await _gather(*check_reg_no_coros)
        await _gather(*collect_symbol_counts_coros)
    finally:
        iranetf.ssl = orig_ssl

    # Dynamically update the specific row contents without altering unassigned blocks
    if any(st is not None for st in new_site_types):
        updates = pl.DataFrame({'l18': ds['l18'], 'new_st': new_site_types})
        ds = (
            ds.join(updates, on='l18', how='left')
            .with_columns(
                pl.coalesce(['new_st', 'siteType']).alias('siteType')
            )
            .drop('new_st')
        )
        write_dataset(ds)

    no_site = ds.filter(pl.col('site').is_null())
    if max(no_site.shape) > 0:
        _logger.warning(
            f'some dataset entries have no associated site:\n{no_site["l18"].to_list()}'
        )
