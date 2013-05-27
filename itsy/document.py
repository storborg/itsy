class Document(object):

    def __init__(self, task, raw):
        self.task = task
        self.raw = raw

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
