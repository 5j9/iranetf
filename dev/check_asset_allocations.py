from asyncio import as_completed, run

from aiohttp import ClientConnectorDNSError, ClientResponseError

from dev import logger
from iranetf.dataset import scan_dataset

ds = scan_dataset()


async def main():
    coros = [
        s.asset_allocation() for s in ds.select('site').collect().to_series()
    ]
    for future in as_completed(coros):
        try:
            await future
        except (ClientResponseError, ClientConnectorDNSError) as e:
            logger.error(f'---\n{e!r}\n----')
            continue


run(main())
