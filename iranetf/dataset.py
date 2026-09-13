from __future__ import annotations as _

from asyncio import gather as _gather, sleep as _sleep
from contextlib import contextmanager as _contextmanager
from json import JSONDecodeError
from logging import Logger as _Logger
from pathlib import Path as _Path
from re import compile as _re_compile

from aiohttp import (
    ClientConnectorDNSError as _ClientConnectorDNSError,
    ClientConnectorError as _ClientConnectorError,
    ClientError as _ClientError,
    ClientResponseError as _ClientResponseError,
    ServerDisconnectedError as _ServerDisconnectedError,
)
from aiohutils import logger as _aiohutils_logger
from polars import (
    DataFrame as _DataFrame,
    Int8 as _Int8,
    LazyFrame as _LazyFrame,
    Object as _Object,
    String as _String,
    coalesce as _coalesce,
    col as _col,
    concat as _concat,
    lit as _lit,
    scan_csv as _scan_csv,
    struct as _struct,
    when as _when,
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
    RayanHamafza2 as _RayanHamafza2,
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
    type_str = row['site_type']
    site_class = getattr(_sites, type_str)

    pid = row['portfolio_id']
    if pid:  # Filters out None, "", etc.
        return site_class(url=row['url'], portfolio_id=pid)

    # Use the site class default portfolio_id (e.g. '1' or '')
    return site_class(url=row['url'])


def scan_dataset() -> _LazyFrame:
    """Load dataset.csv as a LazyFrame with site and inst structures pre-configured."""
    return _scan_csv(
        _DATASET_PATH,
        encoding='utf8',
        schema={
            'l18': _String,
            'name': _String,
            'type': _String,
            'ins_code': _String,
            'reg_no': _String,
            'url': _String,
            'portfolio_id': _String,
            'site_type': _String,
            'dps_interval': _Int8,
            'group_id': _Int8,
        },
    ).with_columns(
        _struct(['site_type', 'url', 'portfolio_id'])
        .map_elements(
            lambda r: (
                _make_site(r) if r.get('site_type') is not None else None
            ),
            return_dtype=_Object,
        )
        .alias('site'),
        _col('ins_code')
        .map_elements(
            lambda c: _Instrument(c) if c is not None else None,
            return_dtype=_Object,
        )
        .alias('inst'),
    )


def sink_dataset(ds: _LazyFrame):
    """
    Processes the LazyFrame pipeline and streams it directly to disk.
    """
    # 1. Fast, vectorized text translations handled lazily
    ds = ds.with_columns(
        [
            _col('l18').str.replace_all('ي', 'ی').str.replace_all('ك', 'ک'),
            _col('name').str.replace_all('ي', 'ی').str.replace_all('ك', 'ک'),
        ]
    )

    columns_order = [
        'l18',
        'name',
        'type',
        'ins_code',
        'reg_no',
        'url',
        'portfolio_id',
        'site_type',
        'dps_interval',
        'group_id',
    ]

    # 2. Select columns, sort, and stream directly to the CSV file
    ds.select(columns_order).sort('l18').sink_csv(
        _DATASET_PATH,
        include_bom=True,  # Protects Persian characters
    )


def _log_and_retry(func):
    async def wrapper(*args):
        retry = 3
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
                    _logger.error(
                        f'{func.__name__}: {type(e).__name__} for {arg}'
                    )
                    return
                retry -= 1
                _logger.debug(
                    f'{func.__name__}: retrying {type(e).__name__} for {arg}'
                )
                await _sleep(2)
                continue
            except _ClientResponseError as e:
                if e.status == 429 and retry > 0:
                    await _sleep(5)
                    retry -= 1
                    continue
                _logger.error(
                    f'{func.__name__}: status {e.status} on {e.request_info.url}'
                )
                return
            except TimeoutError:
                if retry > 0:
                    retry -= 1
                    continue
                _logger.error(f'{func.__name__}: TimeoutError on {arg}')
                return
            except (OSError, _ClientError) as e:
                _logger.error(f'{func.__name__}: {e!r} on {arg}')
                return
            except Exception:
                _logger.exception(f'{func.__name__}: {arg=}')
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


SITE_TYPES = (_RayanHamafza2, _TadbirPardaz, _LeveragedTadbirPardaz, _MabnaDP2)


@_contextmanager
def set_level(logger: _Logger, level: str | int):
    old = logger.level
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old)


async def _url_type(domain: str) -> tuple[str | None, str | None]:
    with set_level(_logger, 'CRITICAL'):
        for coro in (
            _check_validity(site_type(f'{protocol}://{domain}/'))
            for protocol in ('https', 'http')
            for site_type in SITE_TYPES
        ):
            try:
                result = await coro
            except OSError:
                continue

            if result is not None:
                return result

    _logger.warning(f'failed for {domain}')
    return None, None


_URL_HOST_RE = _re_compile(r'//([^/]+)/')


async def _check_ds_then_fipiran(
    ds_url: str | None, fip_domain: str | None, update_existing: bool
) -> tuple[str | None, str | None]:
    """Check DS URL first (if available), falling back sequentially to FIPIRAN domain."""
    ds_domain = None
    if ds_url is not None:
        match = _URL_HOST_RE.search(ds_url)
        if match:
            ds_domain = match.group(1)

    same_domain = (
        ds_domain is not None
        and fip_domain is not None
        and ds_domain.lower() == fip_domain.lower()
    )

    # 1. Check existing DS URL if present
    if ds_url is not None:
        if update_existing or not same_domain:
            res = await _url_type(ds_domain) if ds_domain else (None, None)
            if res[0] is not None:
                return res

    # 2. Check FIPIRAN domain as fallback or primary if DS URL absent.
    #    Skip if the FIPIRAN domain is the same as the DS domain (already
    #    covered by step 1 regardless of update_existing).
    if fip_domain is not None and not same_domain:
        res = await _url_type(fip_domain)
        if res[0] is not None:
            return res

    return None, None


def _add_ds_url(
    fipiran_df: _DataFrame,
    ds: _DataFrame,
) -> _DataFrame:
    """Return FIPIRAN DataFrame with corresponding ds_url attached based on identity rules."""
    ds_aliased = ds.select(
        'reg_no',
        'group_id',
        _col('url').alias('ds_url_primary'),
    )

    # 1. Primary Match: (reg_no, group_id)
    joined = fipiran_df.join(
        ds_aliased,
        on=['reg_no', 'group_id'],
        how='left',
    )

    # 2. Fallback Match: unique reg_no in both datasets
    ds_unique = ds.filter(_col('reg_no').is_unique()).select(
        'reg_no', _col('url').alias('ds_url_fallback')
    )
    fip_unique_reg_nos = fipiran_df.filter(_col('reg_no').is_unique()).select(
        'reg_no'
    )

    fallback_map = fip_unique_reg_nos.join(ds_unique, on='reg_no', how='inner')

    joined = joined.join(fallback_map, on='reg_no', how='left')

    return joined.with_columns(
        _coalesce(['ds_url_primary', 'ds_url_fallback']).alias('ds_url')
    ).drop(['ds_url_primary', 'ds_url_fallback'])


async def _add_url_and_type(
    fipiran_df: _DataFrame, ds: _DataFrame, update_existing: bool
) -> _LazyFrame:
    """Validate URLs/domains per-row and map back results to FIPIRAN DataFrame."""
    fipiran_with_ds = _add_ds_url(fipiran_df, ds)

    # Assign an explicit unique index to map results 1-to-1 without domain collisions
    indexed_df = fipiran_with_ds.with_row_index('__row_id')
    rows = indexed_df.select('__row_id', 'ds_url', 'domain').to_dicts()

    _logger.info(f'checking site types of {len(rows)} FIPIRAN rows')

    with set_level(_aiohutils_logger, 'ERROR'):
        results = await _gather(
            *[
                _check_ds_then_fipiran(
                    r['ds_url'], r['domain'], update_existing
                )
                for r in rows
            ]
        )

    row_ids = [r['__row_id'] for r in rows]
    urls, site_types = zip(*results) if results else ([], [])

    updates_df = _DataFrame(
        {
            '__row_id': row_ids,
            'url_new': urls,
            'site_type_new': site_types,
        }
    )

    res_df = (
        indexed_df.join(updates_df, on='__row_id', how='left')
        .with_columns(
            [
                _col('url_new').alias('url'),
                _col('site_type_new').alias('site_type'),
            ]
        )
        .drop(['__row_id', 'url_new', 'site_type_new'])
    )

    return res_df.lazy()


async def _add_ins_code(new_items: _DataFrame) -> _DataFrame:
    names_without_code = new_items.filter(_col('ins_code').is_null())[
        'name'
    ].to_list()
    if not names_without_code:
        return new_items

    _logger.info('searching names on tsetmc to find their ins_code')
    results = await _gather(
        *[_tsetmc_search(name) for name in names_without_code]
    )
    ins_codes = [(None if len(r) != 1 else r[0]['insCode']) for r in results]

    codes_map = _DataFrame(
        {'name': names_without_code, 'ins_code_new': ins_codes}
    )
    return (
        new_items.join(codes_map, on='name', how='left')
        .with_columns(
            _coalesce(['ins_code_new', 'ins_code']).alias('ins_code')
        )
        .drop('ins_code_new')
    )


async def _fipiran_data(ds: _LazyFrame) -> _LazyFrame:
    import fipiran.funds

    _logger.info('await fipiran.funds.funds()')
    # Use global inference scope for any incoming external dynamic dataframes
    fipiran_df = (
        (await fipiran.funds.funds())
        .rename(
            {'regNo': 'reg_no', 'insCode': 'ins_code', 'groupId': 'group_id'}
        )
        .collect()
    )

    ds_collected = ds.collect()
    reg_not_in_fipiran = ds_collected.filter(
        ~_col('reg_no').is_in(fipiran_df['reg_no'])
    )

    if not reg_not_in_fipiran.is_empty():
        _logger.warning(
            f'Some dataset rows were not found on fipiran:\n{reg_not_in_fipiran}'
        )

    df = fipiran_df.filter(
        (_col('typeOfInvest') == 'Negotiable')
        & ~(_col('fundType').is_in([11, 12, 13, 14, 16]))
        & _col('isCompleted')
    ).select(
        'reg_no',
        _col('smallSymbolName').alias('l18'),
        'name',
        _col('fundType').alias('type'),
        _col('websiteAddress').alias('domain'),
        'ins_code',
        'group_id',
    )

    # Map mapping transformations via high performance native replacement steps
    df = df.with_columns(
        _col('type').replace(_ETF_TYPES, default=_col('type'))
    )
    return df.lazy()


async def _tsetmc_dataset() -> _LazyFrame:
    from tsetmc.dataset import lazy_ds, update

    _logger.info('await tsetmc.dataset.update()')
    await update()
    lf = lazy_ds.lf
    return lf.drop('l30', 'isin', 'cisin')


def _add_new_items_to_ds(new_items: _DataFrame, ds: _DataFrame) -> _DataFrame:
    if max(new_items.shape) == 0:
        return ds

    new_with_code = new_items.filter(_col('ins_code').is_not_null()).drop(
        'domain'
    )
    if max(new_with_code.shape) > 0:
        # Align column structures dynamically and concatenate
        return _concat([ds, new_with_code], how='diagonal_relaxed')

    _logger.info('new_with_code is empty!')
    return ds


async def _update_existing_rows_using_fipiran(
    ds: _DataFrame, fipiran_df: _DataFrame, update_existing: bool
) -> _DataFrame:

    fipiran_lazy = await _add_url_and_type(fipiran_df, ds, update_existing)
    fipiran_df = fipiran_lazy.collect()

    update_columns = ['type', 'url', 'site_type']

    # 1. Primary match: (reg_no, group_id) - includes domain from fipiran_df
    joined = ds.join(
        fipiran_df.select('reg_no', 'group_id', 'domain', *update_columns),
        on=['reg_no', 'group_id'],
        how='left',
        suffix='_fip',
    )

    # 2. Extract globally unique reg_nos across both DataFrames
    ds_unique = ds.filter(_col('reg_no').is_unique()).select('reg_no')
    fip_unique = fipiran_df.filter(_col('reg_no').is_unique())

    # Inner join unique sets (including group_id, domain as fallback)
    fipiran_unique = fip_unique.join(
        ds_unique, on='reg_no', how='inner'
    ).select(
        'reg_no',
        _col('group_id').alias('group_id_unique'),
        _col('domain').alias('domain_unique'),
        *[_col(c).alias(f'{c}_unique') for c in update_columns],
    )

    # 3. Fallback match: reg_no only
    joined = joined.join(fipiran_unique, on='reg_no', how='left')

    # 4. Priority Coalesce with explicit per-column precedence preserving working DS URLs
    coalesce_exprs = [
        # url: Fipiran validated result wins -> existing DS URL
        _coalesce(['url_fip', 'url_unique', 'url']).alias('url'),
        # site_type: Fipiran validated result wins -> existing DS site_type
        _coalesce(['site_type_fip', 'site_type_unique', 'site_type']).alias(
            'site_type'
        ),
        # type: Fipiran (reg_no, group_id) → Fipiran (reg_no) → DS
        _coalesce(['type_fip', 'type_unique', 'type']).alias('type'),
        # domain: Fipiran (reg_no, group_id) → Fipiran (reg_no)
        _coalesce(['domain', 'domain_unique']).alias('domain'),
        # group_id: use Fipiran value when reg_no is unique in both datasets
        _coalesce(['group_id_unique', 'group_id']).alias('group_id'),
    ]

    drop_cols = (
        [f'{col}_fip' for col in update_columns]
        + [f'{col}_unique' for col in update_columns]
        + ['domain_unique', 'group_id_unique']
    )

    ds_updated = joined.with_columns(coalesce_exprs).drop(drop_cols)

    # 5. Build URL fallback using domain when URL is missing
    ds_updated = ds_updated.with_columns(
        _when(_col('url').is_null() & _col('domain').is_not_null())
        .then(_lit('http://') + _col('domain') + _lit('/'))
        .otherwise(_col('url'))
        .alias('url')
    )

    return ds_updated


async def update_dataset(*, update_existing=False) -> _DataFrame:
    """Update dataset and return newly found that could not be added."""
    ds = scan_dataset().drop('site', 'inst').collect()
    fipiran_df = (await _fipiran_data(ds.lazy())).collect()

    ds = await _update_existing_rows_using_fipiran(
        ds, fipiran_df, update_existing
    )
    new_items = fipiran_df.filter(~_col('reg_no').is_in(ds['reg_no']))

    tsetmc_df = (await _tsetmc_dataset()).collect()
    new_items = await _add_ins_code(new_items)
    ds = _add_new_items_to_ds(new_items, ds)

    # Perform a left join update to bring over updated data tracks from TSETMC
    ds = ds.join(tsetmc_df, how='left', suffix='_tsetmc', on='ins_code')

    # Coalesce tracking changes updates
    update_cols = [
        c for c in tsetmc_df.columns if c in ds.columns and c != 'ins_code'
    ]
    for col in update_cols:
        ds = ds.with_columns(
            _coalesce([f'{col}_tsetmc', col]).alias(col)
        ).drop(f'{col}_tsetmc')

    sink_dataset(ds.lazy())
    return new_items.filter(_col('ins_code').is_null())


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


def _assert_url_invariants(ds: _DataFrame):
    # Assert that URLs are clean and do not contain old-style metadata fragments
    assert not ds['url'].str.contains('#').any(), (
        "URLs must not contain '#' fragments"
    )
    duplicates = ds.filter(_struct(['url', 'portfolio_id']).is_duplicated())
    if not duplicates.is_empty():
        _logger.error(
            f'found duplicate (url, portolio_id):\n'
            f'{duplicates.select("l18", "url", "portfolio_id")}'
        )
        raise AssertionError(
            'Duplicate combinations of url and portfolio_id found!'
        )


@_log_and_retry
async def _check_portfolio_counts(site: _BaseSite, dataset_ids: set[str]):
    site_portfolios = await site.portfolios()
    site_ids = site_portfolios.keys()
    url = site.url

    if dataset_ids == {None}:
        dataset_ids = {'1'}

    if site_ids == dataset_ids:
        return
    _logger.error(f'{url}: Portfolio ID mismatch! {dataset_ids=} {site_ids=}')


async def check_dataset(live=False):
    ds = scan_dataset().drop('inst').collect()
    _assert_static_invariants(ds)

    if not live:
        return

    ds = _attach_portfolio_ids(ds)

    new_site_types = await _run_live_checks(ds)
    ds = _apply_site_type_updates(ds, new_site_types)
    _warn_about_missing_sites(ds)


def _assert_static_invariants(ds):
    _assert_url_invariants(ds)

    assert ds['l18'].is_unique().all(), ds.filter(ds['l18'].is_duplicated())
    assert ds['name'].is_unique().all()
    assert ds['type'].is_in(list(_ETF_TYPES.values())).all()
    assert ds['ins_code'].is_unique().all()

    assert (ds['site_type'].is_not_null()).all(), 'site_type contains nulls'
    assert (ds['reg_no'].is_not_null()).all()

    _assert_reg_no_to_single_url(ds)


def _assert_reg_no_to_single_url(ds):
    """Assert that each reg_no maps to only one URL."""
    grouped_check = (
        ds.group_by('reg_no')
        .agg(_col('url').n_unique().alias('cnt'))
        .filter(_col('cnt') > 1)
    )
    assert grouped_check.is_empty(), grouped_check


def _attach_portfolio_ids(ds):
    agg_pids = ds.group_by('url').agg(
        _col('portfolio_id').alias('portfolio_ids')
    )
    return ds.join(agg_pids, on='url', how='left')


async def _run_live_checks(ds):
    check_site_coros = [_new_site_type(s) for s in ds['site']]
    check_reg_no_coros = [
        _check_reg_no(site, reg)
        for (site, reg) in zip(ds['site'], ds['reg_no'])
    ]

    unique_site_pids = ds.unique(subset=['url']).select(
        'site', 'portfolio_ids'
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

    return new_site_types


def _apply_site_type_updates(ds, new_site_types):
    """Dynamically update rows where a fresh site_type was discovered."""
    if not any(st is not None for st in new_site_types):
        return ds

    updates = _DataFrame({'l18': ds['l18'], 'new_st': new_site_types})
    ds = (
        ds.join(updates, on='l18', how='left')
        .with_columns(_coalesce(['new_st', 'site_type']).alias('site_type'))
        .drop('new_st')
    )
    sink_dataset(ds.lazy())
    return ds


def _warn_about_missing_sites(ds):
    no_site = ds.filter(_col('site').is_null())
    if not no_site.is_empty():
        _logger.warning(
            f'some dataset entries have no associated site:\n'
            f'{no_site["l18"].to_list()}'
        )
