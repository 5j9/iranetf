from __future__ import annotations as _

from abc import abstractmethod
from datetime import datetime
from json import loads
from logging import warning
from typing import Any, Protocol, Self, TypedDict, runtime_checkable

import polars as pl

from iranetf import RegNoError, _get


class LiveNAVPS(TypedDict):
    creation: int
    redemption: int
    date: datetime


def comma_int(s: str) -> int:
    return int(s.replace(',', ''))


async def _read(url: str) -> bytes:
    return await (await _get(url)).read()


@runtime_checkable
class BaseSite(Protocol):
    __slots__ = '_home_info_cache', 'last_response', 'url'

    # Changed from DataFrame to LazyFrame for consistent lazy pipeline integration
    _aa_keys: set[str]

    def __init__(self, url: str):
        assert url[-1] == '/', f'the url must end with `/` {url=}'
        self.url = url

    def __repr__(self):
        return f"{type(self).__name__}('{self.url}')"

    def __eq__(self, value):
        if not isinstance(value, BaseSite):
            return NotImplemented
        if value.url == self.url and type(value) is type(self):
            return True
        return False

    async def _json(
        self,
        path: str,
        *,
        params: dict | None = None,
        cookies: dict | None = None,
        df: bool = False,
    ) -> Any:
        r = await _get(self.url + path, params, cookies)
        self.last_response = r
        content = await r.read()
        j = loads(content)
        if df is True:
            # Implements the direct LazyFrame instantiation guardrail safely from memory
            return pl.LazyFrame(j, infer_schema_length=None)
        return j

    async def live_navps(self) -> LiveNAVPS: ...

    async def navps_history(self) -> pl.LazyFrame: ...

    async def cache(self) -> float: ...

    _l18_to_site_map: dict

    @classmethod
    def from_l18(cls, l18: str) -> Self:
        """
        Loads the dataset into an in-memory dictionary lookup cache on first call,
        leaving no lingering dataset objects on the class namespace.
        """
        try:
            return cls._l18_to_site_map[l18]
        except KeyError as e:
            raise KeyError(
                f"l18 value '{l18}' not found or invalid in dataset."
            ) from e
        except AttributeError:
            from iranetf.dataset import read_dataset

            df = read_dataset(site=True).select(['l18', 'site']).collect()
            mp = cls._l18_to_site_map = dict(
                zip(df['l18'].to_list(), df['site'].to_list())
            )
            return mp[l18]

    def _check_aa_keys(self, d: dict):
        if d.keys() <= self._aa_keys:
            return
        warning(
            f'Unknown asset allocation keys on {self!r}: {d.keys() - self._aa_keys}'
        )

    @staticmethod
    async def from_url(url: str):
        import iranetf.sites as sites

        content = await _read(url)
        rfind = content.rfind

        if rfind(b'<div class="tadbirLogo"></div>') != -1:
            tp_site = sites.TadbirPardaz(url)
            info = await tp_site.home_info()
            if info['isLeveragedMode']:
                return sites.LeveragedTadbirPardaz(url)
            if info['isETFMultiNavMode']:
                return sites.TadbirPardazMultiNAV(url + '#2')
            return tp_site

        if rfind(b'<!-- Rayanhamafza Front-End Team -->') != -1:
            return sites.RayanHamafza2(url)

        if rfind(b'Rayan Ham Afza') != -1:
            return sites.RayanHamafza(url)

        if rfind(b'://mabnadp.com') != -1:
            assert rfind(rb'/api/v2') != -1, 'Unknown MabnaDP site type.'
            return sites.MabnaDP2(url)

        raise ValueError(f'Could not determine site type for {url}.')

    async def leverage(self) -> float:
        return 1.0 - await self.cache()

    async def _home(self) -> str:
        return (await _read(self.url)).decode()

    @abstractmethod
    async def _home_info(self) -> dict[str, Any]: ...

    async def home_info(self) -> dict[str, Any]:
        try:
            return self._home_info_cache
        except AttributeError:
            i = self._home_info_cache = await self._home_info()
            return i

    async def reg_no(self) -> str: ...

    async def portfolios(self) -> dict[str, str]:
        """Return a dict mapping portfolio id to portfolio name."""
        ...


async def reg_no_from_home_info(self: BaseSite) -> str:
    home_info = await self.home_info()
    try:
        return home_info['seo_reg_no']
    except KeyError:
        raise RegNoError('"seo_reg_no" not found in home_info') from None
