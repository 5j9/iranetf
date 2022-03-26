from asyncio import new_event_loop

from pytest import fixture

import iranetf
from test import RECORD_MODE, OFFLINE_MODE, FakeResponse


@fixture(scope='session')
def event_loop():
    loop = new_event_loop()
    yield loop
    loop.close()


@fixture(scope='session', autouse=True)
async def session():
    if OFFLINE_MODE:

        class FakeSession:
            @staticmethod
            async def get(_):
                return FakeResponse()

        iranetf.SESSION = FakeSession()
        yield
        return

    session = iranetf.Session()

    if RECORD_MODE:

        async def recording_get(*args, **kwargs):
            resp = await session.get(*args, **kwargs)
            content = await resp.read()
            with open(FakeResponse.file, 'wb') as f:
                f.write(content)
            return resp

        session.get = recording_get

    yield
    await session.close()
