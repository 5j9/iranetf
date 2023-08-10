import logging
from asyncio import run

import config

import iranetf
from iranetf.sites import update_dataset

logging.getLogger().setLevel(logging.INFO)


async def main():
    iranetf.SSL = False  # tolerate week ssl certs
    return await update_dataset(
        check_existing_sites=config.check_existing_sites,
    )


unrecognized_df = run(main())

if not unrecognized_df.empty:
    print('See ~unadded_etfs.html for remaining ETFs')
    with open('~unadded_etfs.html', 'w', encoding='utf8') as f:
        f.write('<head><meta charset="UTF-8"></head>')
        unrecognized_df.to_html(f)
else:
    print('No new ETFs')
