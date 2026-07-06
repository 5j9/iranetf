import asyncio

from polars import DataFrame, col

from dev import logger
from iranetf.dataset import scan_dataset, sink_dataset
from iranetf.sites import BaseSite


async def get_reg_no(site: BaseSite):
    try:
        return await site.reg_no()
    except Exception as e:
        logger.error(f'{e!r} on {site}')


async def main():
    # 1. Scan the dataset (returns a LazyFrame)
    lf = scan_dataset()

    # 2. Filter for rows where 'reg_no' is null and collect only the 'site' column
    # We use .collect() here because we need the actual values to kick off the coroutines
    fetch_df = (
        lf.filter(col('reg_no').is_null()).select('site', 'l18').collect()
    )
    sites_to_fetch = fetch_df.get_column('site')

    coros = [get_reg_no(site) for site in sites_to_fetch]
    reg_nos = await asyncio.gather(*coros)

    # Create a temporary DataFrame mapping the sites to their new reg_nos
    mapping_df = DataFrame(
        {'site': sites_to_fetch, 'new_reg_no': reg_nos, 'l18': fetch_df['l18']}
    )

    # 4. Join the mapping back to the original LazyFrame and fill the holes
    lf = (
        lf.join(mapping_df.lazy(), on='l18', how='left')
        .with_columns(col('reg_no').fill_null(col('new_reg_no')))
        .drop('new_reg_no')
    )

    # 5. Pass the updated LazyFrame to the writer
    sink_dataset(lf)


asyncio.run(main())
