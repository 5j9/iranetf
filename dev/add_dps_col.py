from asyncio import run

from fipiran.funds import funds

from iranetf.dataset import load_dataset, save_dataset


async def main():
    ds = load_dataset(site=False)
    ds.set_index('l18', inplace=True)
    df = await funds()
    df = df[
        (df['dividendIntervalPeriod'] > 0)
        & (df['typeOfInvest'] == 'Negotiable')
    ]
    ds = ds.join(
        df[['smallSymbolName', 'dividendIntervalPeriod']].set_index(
            'smallSymbolName'
        )
    )
    ds.rename(columns={'dividendIntervalPeriod': 'dps_interval'}, inplace=True)
    ds.reset_index(inplace=True)
    save_dataset(ds)


run(main())
