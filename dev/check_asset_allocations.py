from asyncio import as_completed, run

from aiohttp import ClientResponseError

from iranetf import load_dataset

ds = load_dataset()


async def main():
    coros = [s.asset_allocation() for s in ds.site]
    for future in as_completed(coros):
        try:
            await future
        except ClientResponseError:
            continue


run(main())
