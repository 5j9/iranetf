from asyncio import run

from iranetf import Session
from iranetf.sites import update_dataset

async def main():
    async with Session():
        return await update_dataset()


unrecognized_df = run(main())

if not unrecognized_df.empty:
    print('check unrecognized_df')
    breakpoint()
