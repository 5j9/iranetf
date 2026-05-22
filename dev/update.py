from asyncio import run

import iranetf
from dev import logger
from iranetf.dataset import update_dataset


async def main():
    iranetf.ssl = False  # tolerate week ssl certs
    # Note: check_existing_sites will significantly increase the update time
    df = await update_dataset(update_existing=False)
    return df


unrecognized_df = run(main())

if not unrecognized_df.empty:
    logger.info('See ~unadded_etfs.html for remaining ETFs')
    with open('~unadded_etfs.html', 'w', encoding='utf8') as f:
        f.write('<head><meta charset="UTF-8"></head>')
        unrecognized_df.to_html(f)
else:
    logger.info('No new ETFs')
