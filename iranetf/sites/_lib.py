from __future__ import annotations as _

from abc import ABC, abstractmethod
from datetime import datetime
from json import loads
from logging import warning
from typing import Any, TypedDict

from pandas import DataFrame

import iranetf
import iranetf.sites
from iranetf import _get


class LiveNAVPS(TypedDict):
    creation: int
    redemption: int
    date: datetime


def comma_int(s: str) -> int:
    return int(s.replace(',', ''))


async def _read(url: str) -> bytes:
    return await (await _get(url)).read()


class BaseSite(ABC):
    __slots__ = '_home_info_cache', 'last_response', 'url'

    ds: DataFrame
    _aa_keys: set

    def __init__(self, url: str):
        assert url[-1] == '/', 'the url must end with `/`'
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
            return DataFrame(j, copy=False)
        return j

    @abstractmethod
    async def live_navps(self) -> LiveNAVPS: ...

    @abstractmethod
    async def navps_history(self) -> DataFrame: ...

    @abstractmethod
    async def cache(self) -> float: ...

    @classmethod
    def from_l18(cls, l18: str) -> iranetf.sites.AnySite:
        try:
            ds = cls.ds
        except AttributeError:
            from iranetf.dataset import load_dataset

            ds = cls.ds = load_dataset(site=True).set_index('l18')
        return ds.loc[l18, 'site']  # type: ignore

    def _check_aa_keys(self, d: dict):
        if d.keys() <= self._aa_keys:
            return
        warning(
            f'Unknown asset allocation keys on {self!r}: {d.keys() - self._aa_keys}'
        )

    @staticmethod
    async def from_url(url: str) -> iranetf.sites.AnySite:
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

        if rfind(b'Rayan Ham Afza') != -1:
            return sites.RayanHamafza(url)

        if rfind(b'://mabnadp.com') != -1:
            if rfind(rb'/api/v1/') != -1:
                return sites.MabnaDP(url)
            assert rfind(rb'/api/v2/') != -1, 'Uknown MabnaDP site type.'
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

    async def reg_no(self) -> str:
        return (await self.home_info())['seo_reg_no']
