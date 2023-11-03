from asyncio import as_completed, run

from aiohttp import ClientConnectorCertificateError

from iranetf import MabnaDP, RayanHamafza, load_dataset

ds = load_dataset()


async def check_version(site):
    version = await site.version()
    return (site, version)


async def main():
    # RayanHamafza does not have version
    sites = [s for s in ds.site if type(s) is not RayanHamafza]
    coros = [check_version(s) for s in sites]
    for future in as_completed(coros):
        try:
            site, version = await future
        except ClientConnectorCertificateError:
            continue
        if type(site) is MabnaDP:
            if version == '2.5':
                continue
        else:  # isinstance(s, BaseTadbirPardaz)
            if version == '9.2.2':
                continue
        print(f'{version} on {site}')


run(main())
