from asyncio import run

import iranetf
from iranetf.sites import update_dataset


async def main():
    iranetf.SSL = False  # tolerate week ssl certs
    async with iranetf.Session():
        return await update_dataset()


unrecognized_df = run(main())

if not unrecognized_df.empty:
    print('See ~unadded_etfs.html for remaining ETFs')
    with open('~unadded_etfs.html', 'w', encoding='utf8') as f:
        f.write('<head><meta charset="UTF-8"></head>')
        unrecognized_df.to_html(f)
else:
    print('No new ETFs')
