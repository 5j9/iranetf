from asyncio import run

from pandas import concat
from tsetmc.funds import commodity_etfs, etfs

from iranetf import load_dataset


async def main():
    ds = load_dataset(site=False)

    df1 = await etfs(flow=1)
    df2 = await etfs(flow=2)
    df3 = await commodity_etfs()
    df = concat([df1, df2, df3], ignore_index=True)

    ds_set = set(ds['l18'])
    df_set = set(df['instrument.lVal18AFC'])

    print(f'{ds_set - df_set = }')
    print(f'{df_set - ds_set = }')


run(main())
