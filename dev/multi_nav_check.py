from asyncio import gather, run

from iranetf import TadbirPardaz, load_dataset


async def check(site: TadbirPardaz):
    info = await site.info()
    try:
        assert info['isETFMultiNavMode'] is False
    except AssertionError:
        print('Unexpected multi-nav mode on', site.url)


async def main():
    ds = load_dataset()
    tp = ds[ds['siteType'] == 'TadbirPardaz']
    site: TadbirPardaz
    await gather(*[check(site) for site in tp['site']])


run(main())
