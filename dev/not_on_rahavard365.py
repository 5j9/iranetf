from asyncio import run

from iranetf import load_dataset
from iranetf.rahavard365 import etfs

ds = load_dataset(site=False)


rdf = run(etfs(short_name=True))

missing_in_ds = rdf['short_name'][~rdf['short_name'].isin(ds['symbol'])]
print(missing_in_ds)
