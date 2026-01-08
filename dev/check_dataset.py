import logging
from asyncio import run

from rich.logging import RichHandler

from iranetf.dataset import check_dataset

logging.basicConfig(
    level='NOTSET',
    format='%(message)s',
    datefmt='[%X]',
    handlers=[
        RichHandler(
            show_path=True,  # Keep the path on the right
            enable_link_path=True,  # Specifically enables terminal hyperlinks
            rich_tracebacks=True,
        )
    ],
)


async def main():
    await check_dataset(live=True)


run(main())
