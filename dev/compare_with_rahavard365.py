from asyncio import run

from dev import logger
from iranetf.dataset import read_dataset
from iranetf.rahavard365 import etfs

ds = read_dataset(site=False)


rdf = run(etfs(short_name=True))

missing_in_ds = rdf['short_name'][~rdf['short_name'].isin(ds['l18'])]
logger.info(missing_in_ds)
