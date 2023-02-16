import iranetf

# noinspection PyUnresolvedReferences
from aiohttp_test_utils import event_loop
from aiohttp_test_utils import session_fixture_factory


session = session_fixture_factory(iranetf)
