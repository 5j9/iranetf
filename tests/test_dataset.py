from __future__ import annotations

from types import SimpleNamespace

import polars as pl
import pytest

# Adjust to your actual module path.
from iranetf.dataset.update import (
    _add_ds_url,
    _check_ds_then_fipiran,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _site(name: str, host: str) -> tuple[str, str]:
    """Build the (url, site_type) tuple that _url_type returns on success."""
    return f'http://{host}/', name


def _fit_height(base: dict, height: int) -> dict:
    """Pad any single-element (or shorter) columns with their last value so
    every column has exactly ``height`` rows. Lets tests override only the
    columns they care about without tripping Polars' shape check."""
    out = {}
    for k, v in base.items():
        v = list(v)
        if len(v) == height:
            out[k] = v
        elif len(v) == 1:
            out[k] = v * height
        else:
            raise ValueError(
                f'column {k!r} has length {len(v)}, cannot fit to {height}'
            )
    return out


@pytest.fixture
def mock_url_type(monkeypatch):
    ns = SimpleNamespace(table={}, calls=[])

    async def fake_url_type(domain: str):
        ns.calls.append(domain)
        result = ns.table.get(domain)
        return result if result is not None else (None, None)

    monkeypatch.setattr(
        'iranetf.dataset.update._url_type', fake_url_type, raising=True
    )
    return ns


# ---------------------------------------------------------------------------
# _check_ds_then_fipiran tests
# ---------------------------------------------------------------------------


async def test_new_fipiran_row_no_ds_url(mock_url_type):
    """No DS URL at all -> FIPIRAN domain is checked exactly once."""
    mock_url_type.table['fip.example'] = _site('TadbirPardaz', 'fip.example')

    result = await _check_ds_then_fipiran(None, 'fip.example', False)

    assert result == ('http://fip.example/', 'TadbirPardaz')
    assert mock_url_type.calls == ['fip.example']


async def test_ds_url_only_no_fip_domain(mock_url_type):
    """DS URL present, no FIPIRAN domain -> only DS checked."""
    mock_url_type.table['ds.example'] = _site('MabnaDP2', 'ds.example')

    result = await _check_ds_then_fipiran(
        'http://ds.example/foo', None, update_existing=True
    )

    assert result == ('http://ds.example/', 'MabnaDP2')
    assert mock_url_type.calls == ['ds.example']


async def test_same_domain_update_existing_false_skips_both(mock_url_type):
    """Same domain + update_existing=False -> nothing is checked."""
    mock_url_type.table['same.example'] = _site('TadbirPardaz', 'same.example')

    result = await _check_ds_then_fipiran(
        'http://same.example/x', 'same.example', update_existing=False
    )

    assert result == (None, None)
    assert mock_url_type.calls == []


async def test_same_domain_update_existing_true_checks_ds_once(mock_url_type):
    """Same domain + update_existing=True -> DS checked once, FIPIRAN skipped."""
    mock_url_type.table['same.example'] = _site('TadbirPardaz', 'same.example')

    result = await _check_ds_then_fipiran(
        'http://same.example/x', 'same.example', update_existing=True
    )

    assert result == ('http://same.example/', 'TadbirPardaz')
    assert mock_url_type.calls == ['same.example']  # exactly once


async def test_same_domain_update_existing_true_ds_fails_does_not_retry(
    mock_url_type,
):
    """Same domain + update_existing=True + DS failure -> FIPIRAN NOT re-checked."""
    mock_url_type.table['same.example'] = None  # DS check fails

    result = await _check_ds_then_fipiran(
        'http://same.example/x', 'same.example', update_existing=True
    )

    assert result == (None, None)
    assert mock_url_type.calls == ['same.example']  # not twice


async def test_different_domains_ds_succeeds(mock_url_type):
    """Different domains -> DS checked first, succeeds, FIPIRAN not consulted."""
    mock_url_type.table['ds.example'] = _site('MabnaDP2', 'ds.example')

    result = await _check_ds_then_fipiran(
        'http://ds.example/x', 'fip.example', update_existing=False
    )

    assert result == ('http://ds.example/', 'MabnaDP2')
    assert mock_url_type.calls == ['ds.example']


async def test_different_domains_ds_fails_fip_succeeds(mock_url_type):
    """Different domains, DS fails -> FIPIRAN attempted in order."""
    mock_url_type.table['ds.example'] = None
    mock_url_type.table['fip.example'] = _site('TadbirPardaz', 'fip.example')

    result = await _check_ds_then_fipiran(
        'http://ds.example/x', 'fip.example', update_existing=False
    )

    assert result == ('http://fip.example/', 'TadbirPardaz')
    assert mock_url_type.calls == ['ds.example', 'fip.example']


async def test_different_domains_update_existing_true_order(mock_url_type):
    """Same as above but with update_existing=True: DS then FIPIRAN."""
    mock_url_type.table['ds.example'] = None
    mock_url_type.table['fip.example'] = _site('RayanHamafza2', 'fip.example')

    result = await _check_ds_then_fipiran(
        'http://ds.example/x', 'fip.example', update_existing=True
    )

    assert result == ('http://fip.example/', 'RayanHamafza2')
    assert mock_url_type.calls == ['ds.example', 'fip.example']


async def test_both_fail(mock_url_type):
    """Different domains, both fail -> (None, None), both tried in order."""
    mock_url_type.table['ds.example'] = None
    mock_url_type.table['fip.example'] = None

    result = await _check_ds_then_fipiran(
        'http://ds.example/x', 'fip.example', update_existing=False
    )

    assert result == (None, None)
    assert mock_url_type.calls == ['ds.example', 'fip.example']


async def test_unparseable_ds_url_skips_ds_check(mock_url_type):
    """If the DS URL has no //host/ pattern, DS domain is None; skip DS check."""
    mock_url_type.table['fip.example'] = _site('TadbirPardaz', 'fip.example')

    result = await _check_ds_then_fipiran(
        'not-a-url', 'fip.example', update_existing=True
    )

    # ds_domain is None -> DS branch skipped even with update_existing=True,
    # then FIPIRAN is checked.
    assert result == ('http://fip.example/', 'TadbirPardaz')
    assert mock_url_type.calls == ['fip.example']


# ---------------------------------------------------------------------------
# _add_ds_url tests
# ---------------------------------------------------------------------------
def _fipiran_df(**overrides) -> pl.DataFrame:
    """Minimal FIPIRAN-shaped frame used by _add_ds_url."""
    base = {
        'reg_no': ['R1'],
        'group_id': [1],
        'l18': ['AAA'],
    }
    base.update(overrides)
    # Row count is dictated by the longest column the caller passed.
    height = max(len(list(v)) for v in base.values())
    return pl.DataFrame(_fit_height(base, height))


def _ds_df(**overrides) -> pl.DataFrame:
    """Minimal dataset-shaped frame used by _add_ds_url."""
    base = {
        'reg_no': ['R1'],
        'group_id': [1],
        'url': ['http://ds.example/'],
    }
    base.update(overrides)
    height = max(len(list(v)) for v in base.values())
    return pl.DataFrame(_fit_height(base, height))


def test_add_ds_url_primary_match():
    """(reg_no, group_id) match takes precedence over any fallback."""
    fip = _fipiran_df(reg_no=['R1', 'R2'], group_id=[1, 2])
    ds = _ds_df(
        reg_no=['R1', 'R2'],
        group_id=[1, 2],
        url=['http://primary.example/', 'http://other.example/'],
    )
    out = _add_ds_url(fip, ds).sort('reg_no')
    assert out['ds_url'].to_list() == [
        'http://primary.example/',
        'http://other.example/',
    ]


def test_add_ds_url_primary_match_takes_precedence_over_fallback():
    """
    When (reg_no, group_id) matches, the fallback (unique reg_no) must NOT
    overwrite the primary result, even if the fallback row would also match.
    """
    # DS has two rows with same reg_no but different group_ids -> reg_no is
    # NOT unique in DS, so fallback path must not contribute anything.
    fip = _fipiran_df(reg_no=['R1'], group_id=[1])
    ds = _ds_df(
        reg_no=['R1', 'R1'],
        group_id=[1, 99],
        url=['http://primary.example/', 'http://decoy.example/'],
    )
    out = _add_ds_url(fip, ds)
    assert out['ds_url'].to_list() == ['http://primary.example/']


def test_add_ds_url_unique_reg_no_fallback():
    """
    Primary match fails (group_id differs), but reg_no is unique in both
    datasets -> fallback supplies the DS URL.
    """
    fip = _fipiran_df(reg_no=['R1'], group_id=[7])  # group_id mismatch
    ds = _ds_df(
        reg_no=['R1'],
        group_id=[1],
        url=['http://fallback.example/'],
    )
    out = _add_ds_url(fip, ds)
    assert out['ds_url'].to_list() == ['http://fallback.example/']


def test_add_ds_url_non_unique_reg_no_no_fallback():
    """
    reg_no is duplicated in the FIPIRAN frame -> fallback must be suppressed
    and no ds_url attached even if primary (reg_no, group_id) fails.
    """
    fip = _fipiran_df(reg_no=['R1', 'R1'], group_id=[7, 8])
    ds = _ds_df(
        reg_no=['R1'],
        group_id=[1],
        url=['http://fallback.example/'],
    )
    out = _add_ds_url(fip, ds).sort('group_id')
    assert out['ds_url'].to_list() == [None, None]


def test_add_ds_url_non_unique_reg_no_in_ds_no_fallback():
    """
    reg_no is duplicated in the DS frame -> fallback must be suppressed.
    """
    fip = _fipiran_df(reg_no=['R1'], group_id=[7])
    ds = _ds_df(
        reg_no=['R1', 'R1'],
        group_id=[1, 2],
        url=['http://a.example/', 'http://b.example/'],
    )
    out = _add_ds_url(fip, ds)
    assert out['ds_url'].to_list() == [None]


def test_add_ds_url_no_match_returns_null():
    """Completely disjoint reg_no -> ds_url is null."""
    fip = _fipiran_df(reg_no=['R1'], group_id=[1])
    ds = _ds_df(reg_no=['R9'], group_id=[1], url=['http://x.example/'])
    out = _add_ds_url(fip, ds)
    assert out['ds_url'].to_list() == [None]


def test_add_ds_url_preserves_fipiran_columns():
    """Output keeps all FIPIRAN columns and adds a single ds_url column."""
    fip = _fipiran_df(reg_no=['R1'], group_id=[1], l18=['AAA'])
    ds = _ds_df()
    out = _add_ds_url(fip, ds)
    assert set(out.columns) == {'reg_no', 'group_id', 'l18', 'ds_url'}
    assert out['l18'].to_list() == ['AAA']


def test_add_ds_url_multiple_rows_mixed_matching():
    """Mix of primary matches, fallback matches, and misses in one frame."""
    fip = _fipiran_df(
        reg_no=['R1', 'R2', 'R3'],
        group_id=[1, 2, 3],
        l18=['AAA', 'BBB', 'CCC'],
    )
    ds = _ds_df(
        reg_no=['R1', 'R2'],
        group_id=[1, 99],  # R2 group_id differs -> fallback path
        url=['http://primary.example/', 'http://fallback.example/'],
    )
    out = _add_ds_url(fip, ds).sort('reg_no')
    assert out['ds_url'].to_list() == [
        'http://primary.example/',
        'http://fallback.example/',
        None,  # R3 has no counterpart at all
    ]


def test_add_ds_url_null_urls_and_domains_not_applicable():
    """
    _add_ds_url only operates on reg_no/group_id/url; nulls in unrelated
    columns must not affect the result.
    """
    fip = _fipiran_df(reg_no=['R1'], group_id=[1], l18=[None])
    ds = _ds_df(reg_no=['R1'], group_id=[1], url=[None])
    out = _add_ds_url(fip, ds)
    # A null DS url still gets picked up as the ds_url value (no filtering).
    assert out['ds_url'].to_list() == [None]
