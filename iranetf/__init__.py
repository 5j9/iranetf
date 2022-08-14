__version__ = '0.7.1.dev0'

from datetime import datetime as _datetime

from jdatetime import datetime as _jdatetime
from aiohttp import ClientSession as _ClientSession, \
    ClientTimeout as _ClientTimeout, ClientResponse as _ClientResponse


SESSION : _ClientSession | None = None


class Session:

    def __new__(cls, *args, **kwargs) -> _ClientSession:
        global SESSION
        if 'timeout' not in kwargs:
            kwargs['timeout'] = _ClientTimeout(
                total=60., sock_connect=10., sock_read=10.)
        SESSION = _ClientSession(**kwargs)
        return SESSION


async def _get(url: str) -> _ClientResponse:
    return await SESSION.get(url)


async def _read(url: str) -> bytes:
    return await (await _get(url)).read()


def _j2g(s: str) -> _datetime:
    return _jdatetime(*[int(i) for i in s.split('/')]).togregorian()
