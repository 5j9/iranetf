from asyncio import gather, run

from iranetf.dataset import read_dataset

ds = read_dataset()


async def main():
    ds['cache'] = await gather(  # type: ignore
        *[s.cache() for s in ds['site']], return_exceptions=True
    )


run(main())
print(ds)
