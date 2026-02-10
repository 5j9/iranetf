__version__ = '2.0.4.dev1'

from aiohttp import (
    ClientResponse as _ClientResponse,
)
from aiohutils.session import SessionManager

session_manager = SessionManager()


ssl: bool = False  # as horrible as this is, many sites fail ssl verification


async def _get(
    url: str, params: dict | None = None, cookies: dict | None = None
) -> _ClientResponse:
    return await session_manager.request(
        'get', url, ssl=ssl, cookies=cookies, params=params
    )
