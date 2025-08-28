__version__ = '0.30.0'

import pandas as _pd
from aiohttp import (
    ClientResponse as _ClientResponse,
)
from aiohutils.session import SessionManager

_pd.options.mode.copy_on_write = True
_pd.options.future.infer_string = True  # type: ignore
_pd.options.future.no_silent_downcasting = True  # type: ignore

session_manager = SessionManager()


ssl: bool = False  # as horrible as this is, many sites fail ssl verification


async def _get(
    url: str, params: dict | None = None, cookies: dict | None = None
) -> _ClientResponse:
    return await session_manager.get(
        url, ssl=ssl, cookies=cookies, params=params
    )
