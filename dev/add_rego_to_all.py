import asyncio

from polars import DataFrame, col

from dev import logger
from iranetf.dataset import scan_dataset, sink_dataset
from iranetf.sites import BaseSite


async def get_regno(site: BaseSite):
    try:
        return await site.reg_no()
    except Exception as e:
        logger.error(f'{e!r} on {site}')


async def main():
    # 1. Scan the dataset (returns a LazyFrame)
    lf = scan_dataset()

    # 2. Filter for rows where 'regNo' is null and collect only the 'site' column
    # We use .collect() here because we need the actual values to kick off the coroutines
    fetch_df = (
        lf.filter(col('regNo').is_null()).select('site', 'l18').collect()
    )
    sites_to_fetch = fetch_df.get_column('site')

    coros = [get_regno(site) for site in sites_to_fetch]
    regnos = await asyncio.gather(*coros)

    # Create a temporary DataFrame mapping the sites to their new regNos
    mapping_df = DataFrame(
        {'site': sites_to_fetch, 'new_regNo': regnos, 'l18': fetch_df['l18']}
    )

    # 4. Join the mapping back to the original LazyFrame and fill the holes
    lf = (
        lf.join(mapping_df.lazy(), on='l18', how='left')
        .with_columns(col('regNo').fill_null(col('new_regNo')))
        .drop('new_regNo')
    )

    # 5. Pass the updated LazyFrame to the writer
    sink_dataset(lf)


asyncio.run(main())
