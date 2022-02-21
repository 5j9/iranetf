from unittest.mock import patch

import iranetf


RECORD_MODE = False
OFFLINE_MODE = True and not RECORD_MODE


def session_patch(filename):

    async def _fake_session_get(url: str) -> str | bytes:
        file = f'{__file__}/../testdata/{filename}'

        if OFFLINE_MODE:
            with open(file, 'rb') as f:
                content = f.read()
        else:
            async with iranetf.Session() as s:
                content = await (await s.get(url)).read()
            if RECORD_MODE:
                with open(file, 'wb') as f:
                    f.write(content)

        return content

    return patch('iranetf._session_get', _fake_session_get)
