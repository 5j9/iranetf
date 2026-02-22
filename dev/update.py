from asyncio import run

import iranetf
from dev import config
from iranetf.dataset import update_dataset


async def main():
    iranetf.ssl = False  # tolerate week ssl certs
    df = await update_dataset(
        check_existing_sites=config.check_existing_sites,
    )
    return df


unrecognized_df = run(main())

if not unrecognized_df.empty:
    print('See ~unadded_etfs.html for remaining ETFs')
    with open('~unadded_etfs.html', 'w', encoding='utf8') as f:
        f.write('<head><meta charset="UTF-8"></head>')
        unrecognized_df.to_html(f)
else:
    print('No new ETFs')
