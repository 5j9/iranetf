from asyncio import as_completed, run
from collections import defaultdict

from aiohttp import ClientConnectorCertificateError

from iranetf.dataset import load_dataset
from iranetf.sites import RayanHamafza

ds = load_dataset()


async def check_version(site):
    version = await site.version()
    return (site, version)


versions = defaultdict(set)


async def main():
    # RayanHamafza does not have version
    sites = [s for s in ds.site if not isinstance(s, RayanHamafza)]
    coros = [check_version(s) for s in sites]
    for future in as_completed(coros):
        try:
            site, version = await future
        except ClientConnectorCertificateError:
            continue
        versions[type(site).__name__].add(version)
        print(f'{version} on {site}')
    print(f'versions:\n{versions}')


run(main())
