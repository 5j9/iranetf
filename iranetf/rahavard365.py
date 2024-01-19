from aiohutils.session import SessionManager
from pandas import DataFrame

session_manager = SessionManager()

API = 'https://rahavard365.com/api/v2/'


async def api(path: str) -> dict:
    r = await session_manager.get(f'{API}{path}')
    return (await r.json())['data']


class Rahavard365:
    __slots__ = 'asset_id'

    def __init__(self, asset_id: int | str):
        self.asset_id = asset_id

    async def specification(self) -> dict:
        return await api(f'asset/{self.asset_id}/specification')


async def etfs(short_name=False) -> DataFrame:
    data = await api('market-data/etf-funds')
    if short_name:
        for item in data:
            specification = await Rahavard365(item['asset_id']).specification()
            item['short_name'] = specification['instruments'][0]['short_name']
    return DataFrame(data)
