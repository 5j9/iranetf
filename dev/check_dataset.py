from asyncio import run

import dev  # noqa: F401
from iranetf.dataset import check_dataset


async def main():
    await check_dataset(live=True)


run(main())
