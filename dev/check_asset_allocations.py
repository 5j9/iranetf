from asyncio import as_completed, run
from logging import getLogger

from aiohttp import ClientConnectorDNSError, ClientResponseError

from iranetf import load_dataset

ds = load_dataset()
logger = getLogger(__name__)


async def main():
    coros = [s.asset_allocation() for s in ds.site]
    for future in as_completed(coros):
        try:
            await future
        except (ClientResponseError, ClientConnectorDNSError) as e:
            logger.error(f'---\n{e!r}\n----')
            continue


run(main())
