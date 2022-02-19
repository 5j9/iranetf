from unittest.mock import patch

# noinspection PyProtectedMember
from iranetf import _YK, _loads, _get as _original_get


RECORD_MODE = False
OFFLINE_MODE = True and not RECORD_MODE


offline_mode = patch(
    'iranetf._get',
    side_effect=NotImplementedError(
        'iranetf._get should not be called without proper patch'))


class FakeResponse:

    def __init__(self, content: bytes):
        self.content = content


def get_patch(filename):
    if RECORD_MODE:
        def _get_recorder(*args, **kwargs):
            resp = _original_get(*args, **kwargs)
            content = resp.content
            with open(f'{__file__}/../testdata/{filename}', 'wb') as f:
                f.write(content)
            return resp
        return patch('iranetf._get', _get_recorder)

    if not OFFLINE_MODE:
        return patch('iranetf._get', _original_get)

    with open(f'{__file__}/../testdata/{filename}', 'rb') as f:
        content = f.read()

    def fake_get(*_, **__):
        return FakeResponse(content)

    return patch('iranetf._get', fake_get)
