from asyncio import (
    as_completed as _as_completed,
    create_task as _create_task,
    gather as _gather,
)
from json import JSONDecodeError as _JSONDecodeError
from re import compile as _compile

from aiohutils import logger as _aiohutils_logger
from polars import (
    DataFrame as _DataFrame,
    LazyFrame as _LazyFrame,
    coalesce as _coalesce,
    col as _col,
    concat as _concat,
    lit as _lit,
    when as _when,
)
from tsetmc.instruments import (
    search as _tsetmc_search,
)

from iranetf import logger as _logger
from iranetf.dataset import (
    _ETF_TYPES,
    _log_and_retry,
    _set_level,
    scan_dataset as _scan_dataset,
    sink_dataset as _sink_dataset,
)
from iranetf.sites import (
    BaseSite as _BaseSite,
    LeveragedTadbirPardaz as _LeveragedTadbirPardaz,
    MabnaDP2 as _MabnaDP2,
    RayanHamafza2 as _RayanHamafza2,
    TadbirPardaz as _TadbirPardaz,
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


_URL_HOST_RE = _compile(r'//([^/]+)/')
SITE_TYPES = (_RayanHamafza2, _TadbirPardaz, _LeveragedTadbirPardaz, _MabnaDP2)


@_log_and_retry
async def _check_validity(site: _BaseSite) -> tuple[str, str] | None:
    try:
        await site.live_navps()
    except _JSONDecodeError:
        return
    last_url = site.last_response.url
    return f'{last_url.scheme}://{last_url.host}/', type(site).__name__


async def _url_type(domain: str) -> tuple[str | None, str | None]:
    with _set_level(_logger, 'CRITICAL'):
        for protocol in ('https', 'http'):
            tasks = [
                _create_task(
                    _check_validity(site_type(f'{protocol}://{domain}/'))
                )
                for site_type in SITE_TYPES
            ]

            try:
                for task in _as_completed(tasks):
                    try:
                        result = await task
                    except OSError:
                        continue

                    if result is not None:
                        return result

            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await _gather(*tasks, return_exceptions=True)

    _logger.warning(f'failed for {domain}')
    return None, None


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


async def _add_url_and_type(
    fipiran_df: _DataFrame, ds: _DataFrame, update_existing: bool
) -> _LazyFrame:
    """Validate URLs/domains per-row and map back results to FIPIRAN DataFrame."""
    fipiran_with_ds = _add_ds_url(fipiran_df, ds)

    # Assign an explicit unique index to map results 1-to-1 without domain collisions
    indexed_df = fipiran_with_ds.with_row_index('__row_id')
    rows = indexed_df.select('__row_id', 'ds_url', 'domain').to_dicts()

    _logger.info(f'checking site types of {len(rows)} FIPIRAN rows')

    with _set_level(_aiohutils_logger, 'ERROR'):
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


async def _tsetmc_dataset() -> _LazyFrame:
    from tsetmc.dataset import lazy_ds, update

    _logger.info('await tsetmc.dataset.update()')
    await update()
    lf = lazy_ds.lf
    return lf.drop('l30', 'isin', 'cisin')


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


async def update_dataset(*, update_existing=False) -> _DataFrame:
    """Update dataset and return newly found that could not be added."""
    ds = _scan_dataset().drop('site', 'inst').collect()
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

    _sink_dataset(ds.lazy())
    return new_items.filter(_col('ins_code').is_null())
