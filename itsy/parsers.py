from decimal import Decimal
from datetime import datetime


def string(els):
    print "  string(%r)" % els
    if isinstance(els, basestring):
        return els.strip()
    return ''.join(el.text_content().strip() for el in els)


def integer(els):
    print "  int(%r)" % els
    s = string(els)
    return int(s)


def calendar_date(els):
    print "  calendar_date(%r)" % els
    s = string(els)
    return datetime.strptime(s, '%b %d, %Y')


def currency(els):
    print "  currency(%r)" % els
    s = string(els)
    s = s.strip('$')
    s = s.replace(',', '')
    return Decimal(s)


def href(els):
    print "  href(%r)" % els
    assert len(els) == 1
    el = els[0]
    return el.attrib['href']
