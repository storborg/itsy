from . import parsers


class ExtractorMixin(object):

    def x(self, selector, parser='string'):
        if selector:
            elements = self.lxml.cssselect(selector)
        else:
            elements = [self.lxml]

        if parser == 'elements':
            return [Fragment(el) for el in elements]
        elif parser == 'element':
            assert len(elements) == 1
            return Fragment(elements[0])
        elif isinstance(parser, basestring):
            return getattr(parsers, parser)(elements)
        else:
            return parser(elements)


class Fragment(ExtractorMixin):

    def __init__(self, tree):
        self.lxml = tree


class Document(ExtractorMixin):

    def __init__(self, task, resp):
        self.task = task
        self.resp = resp
        self.raw = resp.text

    @property
    def json(self):
        import simplejson
        return simplejson.loads(self.raw)

    @property
    def lxml(self):
        from lxml.html import fromstring
        doc = fromstring(self.raw)
        doc.make_links_absolute(self.task.url)
        return doc

    def extract_links(self, cls):
        from lxml.cssselect import CSSSelector
        sel = CSSSelector(cls)
        return [el.attrib['href'] for el in sel(self.lxml)]
