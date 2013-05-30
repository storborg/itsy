import logging

import urlparse
import urllib

from . import Task
from .client import Client
from .document import Document

log = logging.getLogger(__name__)


def test_handler(handler, url):
    log.warn("Testing handler: %s", handler.__name__)
    t = Task(url=url, document_type=None, referer=None)
    log.warn("  Fetching url: %s", url)
    client = Client()
    raw = client.get(url, None)
    doc = Document(t, raw)
    log.warn("  Applying handler...")
    new_tasks = handler(t, doc)
    if new_tasks:
        log.warn("  Yielded new tasks:")
        for new_task in new_tasks:
            log.warn("    %r", new_task)
    else:
        log.warn("  Did not yield new tasks.")


def strip_param(url, key):
    """
    Strip a GET parameter from a URL.
    """
    o = urlparse.urlparse(url)
    params = urlparse.parse_qs(o[4])
    if key in params:
        del params[key]
    new_url = urlparse.urlunparse((o.scheme,
                                   o.netloc,
                                   o.path,
                                   o.params,
                                   urllib.urlencode(params),
                                   o.fragment))
    return new_url
