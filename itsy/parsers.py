from decimal import Decimal, InvalidOperation
from datetime import datetime

import iso8601


def string(els):
    if isinstance(els, basestring):
        return els.strip()
    return ''.join(el.text_content().strip() for el in els)


def integer_single(s):
    s = s.strip().replace(',', '')
    return int(s)


def integer(els):
    s = string(els)
    for chunk in s.split():
        try:
            return integer_single(chunk)
        except ValueError:
            pass
    raise ValueError("couldn't find an integer in: %r" % s)


def calendar_date(els):
    s = string(els)
    return datetime.strptime(s, '%b %d, %Y')


def isodate(els):
    s = string(els)
    return iso8601.parse_date(s)


def currency_single(s):
    s = s.strip()
    if s.startswith('$'):
        units = 'usd'
        s = s[1:]
    elif s.startswith(u'\xa3'):
        units = 'gbp'
        s = s[1:]
    else:
        raise ValueError("doesn't start with a currency symbol")
    s = s.replace(',', '')
    try:
        return units, Decimal(s)
    except InvalidOperation:
        raise ValueError("couldn't convert to currency")


def currency(els):
    s = string(els)
    for chunk in s.split():
        try:
            return currency_single(chunk)
        except ValueError:
            pass
    raise ValueError("couldn't find a currency in: %r" % s)


def href(els):
    assert len(els) == 1
    el = els[0]
    return el.attrib['href']


def hrefs(els):
    return [el.attrib['href'] for el in els]
