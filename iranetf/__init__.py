__version__ = '0.10.0'

from datetime import datetime as _datetime

from aiohttp import (
    ClientResponse as _ClientResponse,
    ClientSession as _ClientSession,
    ClientTimeout as _ClientTimeout,
)
from jdatetime import datetime as _jdatetime

SESSION : _ClientSession | None = None


class Session:

    def __new__(cls, *args, **kwargs) -> _ClientSession:
        global SESSION
        if 'timeout' not in kwargs:
            kwargs['timeout'] = _ClientTimeout(
                total=60., sock_connect=10., sock_read=10.)
        SESSION = _ClientSession(**kwargs)
        return SESSION


SSL = None


async def _get(url: str) -> _ClientResponse:
    return await SESSION.get(url, ssl=SSL)


async def _read(url: str) -> bytes:
    return await (await _get(url)).read()


def _j2g(s: str) -> _datetime:
    return _jdatetime(*[int(i) for i in s.split('/')]).togregorian()
