from asyncio import run

from iranetf.dataset import scan_dataset
from iranetf.rahavard365 import etfs

ds = scan_dataset()
etfs_lf = run(etfs(short_name=True))

missing_in_ds = etfs_lf.join(
    ds.select('l18'), left_on='short_name', right_on='l18', how='anti'
).select('short_name')


print(missing_in_ds.collect())
