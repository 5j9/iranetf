from unittest.mock import patch


RECORD_MODE = False
OFFLINE_MODE = True and not RECORD_MODE


class FakeResponse:

    file = ''

    async def read(self):
        with open(self.file, 'rb') as f:
            content = f.read()
        return content


# todo: rename to file?
def session_patch(filename):
    return patch.object(FakeResponse, 'file', f'{__file__}/../testdata/{filename}')
