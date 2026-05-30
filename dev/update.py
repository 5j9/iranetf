from asyncio import run

import polars as pl

import iranetf
from dev import logger
from iranetf.dataset import update_dataset


async def main() -> pl.DataFrame:
    iranetf.ssl = False  # tolerate weak ssl certs
    res = await update_dataset(update_existing=False)
    return res.collect() if isinstance(res, pl.LazyFrame) else res


unrecognized_df = run(main())

if unrecognized_df.height > 0:
    logger.info('See ~unadded_etfs.html for remaining ETFs')
    with open('~unadded_etfs.html', 'w', encoding='utf8') as f:
        f.write('<head><meta charset="UTF-8"></head>\n')
        # _repr_html_() safely outputs a clean, readable standard HTML <table> string
        f.write(unrecognized_df._repr_html_())
else:
    logger.info('No new ETFs')
