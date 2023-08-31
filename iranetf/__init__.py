__version__ = '0.16.0'

from datetime import datetime as _datetime

from aiohttp import ClientResponse as _ClientResponse
from aiohutils.session import SessionManager
from jdatetime import datetime as _jdatetime

session_manager = SessionManager()


SSL = None


async def _get(url: str) -> _ClientResponse:
    return await session_manager.get(url, ssl=SSL)


async def _read(url: str) -> bytes:
    return await (await _get(url)).read()


def _j2g(s: str) -> _datetime:
    return _jdatetime(*[int(i) for i in s.split('/')]).togregorian()
