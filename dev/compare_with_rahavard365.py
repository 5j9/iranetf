from asyncio import run

from iranetf.dataset import read_dataset
from iranetf.rahavard365 import etfs

ds = read_dataset(site=False)


rlf = run(etfs(short_name=True))

missing_in_ds = rlf.join(
    ds.select('l18'), left_on='short_name', right_on='l18', how='anti'
).select('short_name')


print(missing_in_ds.collect())
