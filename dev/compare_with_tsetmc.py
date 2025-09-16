from asyncio import run

from pandas import concat
from tsetmc import Flow
from tsetmc.funds import commodity_etfs, etfs
from tsetmc.instruments import Instrument

from iranetf.dataset import load_dataset

ignore_desc = {
    'نوع صندوق : خصوصی',
    'نوع صندوق :  جسورانه',
    'نوع صندوق:  جسورانه',
}


async def is_valid(l18: str) -> bool:
    inst = await Instrument.from_l18(l18)
    info = await inst.info()
    desc = info['faraDesc']
    if desc in ignore_desc:
        return False
    print(f'{l18=!r} {desc=!r}')
    return True


async def main():
    ds = load_dataset(site=False)

    # note: some etfs do not show up in any of the flows; reason: unknown
    df1 = await etfs(flow=Flow.BOURSE)
    df2 = await etfs(flow=Flow.OTC)
    df3 = await commodity_etfs()
    df = concat([df1, df2, df3], ignore_index=True)

    ds_set = set(ds['l18'])
    df_set = set(df['instrument.lVal18AFC'])

    print(f'{ds_set - df_set = }')

    not_in_ds = [i for i in df_set - ds_set if await is_valid(i)]

    print(f'{not_in_ds = }')


run(main())
