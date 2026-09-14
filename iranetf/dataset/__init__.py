from __future__ import annotations as _

from asyncio import (
    gather as _gather,
    sleep as _sleep,
)
from contextlib import contextmanager as _contextmanager
from logging import Logger as _Logger
from pathlib import Path as _Path

from aiohttp import (
    ClientConnectorDNSError as _ClientConnectorDNSError,
    ClientConnectorError as _ClientConnectorError,
    ClientError as _ClientError,
    ClientResponseError as _ClientResponseError,
    ServerDisconnectedError as _ServerDisconnectedError,
)
from polars import (
    DataFrame as _DataFrame,
    Int8 as _Int8,
    LazyFrame as _LazyFrame,
    Object as _Object,
    String as _String,
    coalesce as _coalesce,
    col as _col,
    scan_csv as _scan_csv,
    struct as _struct,
)
from tsetmc.instruments import (
    Instrument as _Instrument,
)

import iranetf
from iranetf import (
    RegNoError as _RegNoError,
    logger as _logger,
    sites as _sites,
)
from iranetf.sites import (
    BaseSite as _BaseSite,
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


@_contextmanager
def _set_level(logger: _Logger, level: str | int):
    old = logger.level
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old)


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
