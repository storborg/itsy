from datetime import datetime
from decimal import Decimal

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


def calendar_date(s):
    s = s.strip()
    return datetime.strptime(s, '%b %d, %Y')


def currency(s):
    s = s.strip()
    s = s.strip('$')
    s = s.replace(',', '')
    return Decimal(s)
