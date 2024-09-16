from asyncio import run

from iranetf import check_dataset


async def main():
    await check_dataset(live=True)


run(main())
