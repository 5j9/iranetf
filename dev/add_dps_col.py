from asyncio import run

import polars as pl
from fipiran.funds import funds

from iranetf.dataset import scan_dataset, sink_dataset


async def main():
    ds = scan_dataset()
    funds_lf = await funds()

    df_filtered = funds_lf.filter(
        (pl.col('dividendIntervalPeriod') > 0)
        & (pl.col('typeOfInvest') == 'Negotiable')
    ).select(
        [
            pl.col('smallSymbolName').alias('l18'),
            pl.col('dividendIntervalPeriod').alias('dps_interval'),
        ]
    )

    ds = ds.join(df_filtered, on='l18', how='left')
    sink_dataset(ds)


run(main())
