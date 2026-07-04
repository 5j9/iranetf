from asyncio import run

from polars import col, concat
from tsetmc import Flow
from tsetmc.funds import commodity_etfs, etfs
from tsetmc.instruments import Instrument

from dev import logger
from iranetf.dataset import scan_dataset

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
    logger.info(f'{l18=!r} {desc=!r}')
    return True


async def main():
    ds = scan_dataset()

    # Load all data as Polars LazyFrames
    lf1 = await etfs(flow=Flow.BOURSE)
    lf2 = await etfs(flow=Flow.OTC)
    lf3 = await commodity_etfs()

    lf = concat([lf1, lf2, lf3])

    # Anti-join to find instruments not in ds
    not_in_ds = (
        lf.join(
            ds.select(col('l18')),
            left_on='instrument.lVal18AFC',
            right_on='l18',
            how='anti',
        )
        .select('instrument.lVal18AFC')
        .unique()
    )

    instruments_to_check = (
        not_in_ds.select('instrument.lVal18AFC').collect().to_series()
    )

    valid_instruments = [
        inst for inst in instruments_to_check if await is_valid(inst)
    ]

    logger.info(f'{valid_instruments = }')
    logger.info(f'Count: {len(valid_instruments)} instruments not in dataset')


run(main())
