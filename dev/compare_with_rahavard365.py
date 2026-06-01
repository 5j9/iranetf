from asyncio import run

import polars as pl

from iranetf.dataset import read_dataset
from iranetf.rahavard365 import etfs

ds = read_dataset(site=False)


rlf = run(etfs(short_name=True))

missing_in_ds = rlf.filter(
    ~pl.col('short_name').is_in(ds.select(pl.col('l18')).collect().to_series())
).select('short_name')

print(missing_in_ds.collect())
