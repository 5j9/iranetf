from json import loads

import polars as pl
from aiohutils.session import SessionManager

session_manager = SessionManager()

HOME = 'https://rahavard365.com/'
API = f'{HOME}api/v2/'


async def api(path: str) -> dict:
    r = await session_manager.request('get', f'{API}{path}')
    return (await r.json())['data']


class Rahavard365:
    __slots__ = 'asset_id'

    def __init__(self, asset_id: int | str):
        self.asset_id = asset_id

    async def specification(self) -> dict:
        return await api(f'asset/{self.asset_id}/specification')

    async def values(self) -> tuple[dict, pl.LazyFrame]:
        r = await session_manager.request(
            'get', f'{HOME}asset/{self.asset_id}/values'
        )
        text = await r.text()
        start = text.find('var layoutModel = ') + 18
        end = text.find(';', start)
        j = loads(text[start:end])

        # Migrated storage container logic to native Polars memory allocation
        if (funds := j.pop('funds', None)) is not None:
            j['funds'] = pl.LazyFrame(funds)
        return j


async def etfs(short_name=False) -> pl.LazyFrame:
    data = await api('market-data/etf-funds')
    if short_name:
        for item in data:
            specification = await Rahavard365(item['asset_id']).specification()
            item['short_name'] = specification['instruments'][0]['short_name']

    # Directly structuralizes the data payload table
    return pl.LazyFrame(data)
