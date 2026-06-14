from asyncio import gather, run

from iranetf.dataset import scan_dataset

ds = scan_dataset()


async def main():
    ds['cache'] = await gather(  # type: ignore
        *[s.cache() for s in ds.select('site').collect().to_series()],
        return_exceptions=True,
    )


run(main())
print(ds)
