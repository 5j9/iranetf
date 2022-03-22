from unittest.mock import patch

import iranetf


RECORD_MODE = False
OFFLINE_MODE = True and not RECORD_MODE


class FakeResponse:

    def __init__(self, content):
        self.content = content

    async def read(self):
        return self.content


class FakeSession:

    def __init__(self, filename):
        self.filename = filename

    async def get(self, url):
        file = f'{__file__}/../testdata/{self.filename}'

        if OFFLINE_MODE:
            with open(file, 'rb') as f:
                content = f.read()
        else:
            async with iranetf.Session() as s:
                content = await (await s.get(url)).read()
            if RECORD_MODE:
                with open(file, 'wb') as f:
                    f.write(content)

        return FakeResponse(content)


def session_patch(filename):
    return patch('iranetf.SESSION', FakeSession(filename))
