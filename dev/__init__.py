import logging

from rich._log_render import LogRender
from rich.logging import RichHandler

# Store original method
_original_log_render_call = LogRender.__call__


# workaround for https://github.com/Textualize/rich/issues/4093
def _patched_log_render_call(
    self,
    console,
    renderables,
    log_time=None,
    time_format=None,
    level='',
    path=None,
    line_no=None,
    link_path=None,
):
    """Patched version that fixes Windows file links."""

    # Fix Windows paths for the link
    if link_path:
        # Convert backslashes to forward slashes
        link_path = link_path.replace('\\', '/')
        # Add the third slash (will be used in the f-string below)
        # The original code does: f"link file://{link_path}"
        # We need: f"link file:///{link_path}"
        # So we prepend a slash to link_path
        link_path = '/' + link_path

    # Call original with modified link_path
    return _original_log_render_call(
        self,
        console,
        renderables,
        log_time=log_time,
        time_format=time_format,
        level=level,
        path=path,
        line_no=line_no,
        link_path=link_path,
    )


LogRender.__call__ = _patched_log_render_call  # type: ignore

# Now configure logging normally
logging.basicConfig(
    level='NOTSET',
    format='%(message)s',
    datefmt='[%d %X]',
    handlers=[
        RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
        )
    ],
)

logger = logging.getLogger(__name__)
