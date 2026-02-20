import logging

from rich.logging import RichHandler

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
