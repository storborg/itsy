import urlparse
import urllib

from . import Task
from .client import Client
from .document import Document


def test_handler(handler, url):
    print "Testing handler: %s" % handler.__name__
    t = Task(url=url, document_type=None, referer=None)
    print "  Fetching url: %s" % url
    client = Client()
    raw = client.get(url, None)
    doc = Document(t, raw)
    print "  Applying handler..."
    new_tasks = handler(t, doc)
    if new_tasks:
        print "  Yielded new tasks:"
        for new_task in new_tasks:
            print "    %r" % new_task
    else:
        print "  Did not yield new tasks."


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
