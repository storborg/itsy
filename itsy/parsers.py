from decimal import Decimal, InvalidOperation
from datetime import datetime

import iso8601


def string(els):
    print "  string(%r)" % els
    if isinstance(els, basestring):
        return els.strip()
    return ''.join(el.text_content().strip() for el in els)


def integer_single(s):
    return int(s)


def integer(els):
    print "  int(%r)" % els
    s = string(els)
    for chunk in s.split():
        try:
            return integer_single(chunk)
        except ValueError:
            pass
    raise ValueError("couldn't find an integer here")


def calendar_date(els):
    print "  calendar_date(%r)" % els
    s = string(els)
    return datetime.strptime(s, '%b %d, %Y')


def isodate(els):
    s = string(els)
    return iso8601.parse_date(s)


def currency_single(s):
    print "  currency_single(%r)" % s
    s = s.strip('$')
    s = s.replace(',', '')
    try:
        return Decimal(s)
    except InvalidOperation:
        raise ValueError("couldn't convert to currency")


def currency(els):
    print "  currency(%r)" % els
    s = string(els)
    for chunk in s.split():
        try:
            return currency_single(chunk)
        except ValueError:
            pass
    raise ValueError("couldn't find a currency here")


def href(els):
    print "  href(%r)" % els
    assert len(els) == 1
    el = els[0]
    return el.attrib['href']


def hrefs(els):
    return [el.attrib['href'] for el in els]
