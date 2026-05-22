from asyncio import gather, run

from dev import logger
from iranetf.dataset import read_dataset, write_dataset
from iranetf.sites import BaseSite


async def get_regno(site: BaseSite):
    try:
        return await site.reg_no()
    except Exception as e:
        logger.error(f'{e!r} on {site}')


async def main():
    ds = read_dataset()

    no_regno = ds['regNo'].isna()
    coros = ds['site'][no_regno].map(get_regno)
    regnos = await gather(*coros)
    ds.loc[no_regno, 'regNo'] = regnos

    write_dataset(ds)


run(main())
