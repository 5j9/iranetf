__version__ = '4.1.2.dev1'

from logging import getLogger as _get_logger

from aiohttp import (
    ClientResponse as _ClientResponse,
)
from aiohttp.client import ClientTimeout as _ClientTimeout
from aiohutils.session import SessionManager

session_manager = SessionManager(timeout=_ClientTimeout(5.0))
logger = _get_logger(__name__)


ssl: bool = False  # as horrible as this is, many sites fail ssl verification


async def _get(
    url: str, params: dict | None = None, cookies: dict | None = None
) -> _ClientResponse:
    return await session_manager.request(
        'get', url, ssl=ssl, cookies=cookies, params=params
    )
