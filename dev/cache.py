from asyncio import gather, run

from iranetf.dataset import load_dataset

ds = load_dataset()


async def main():
    ds['cache'] = await gather(
        *[s.cache() for s in ds['site']], return_exceptions=True
    )


run(main())
print(ds)
