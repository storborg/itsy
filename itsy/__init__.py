import requests
import gevent
from gevent import Greenlet, monkey
monkey.patch_all()

from Queue import Queue, Empty

from . import client
from .document import Document


sentinel = object()


class Task(object):

    def __init__(self, url, document_type, referer=sentinel, interval=None):
        self.url = url
        self.document_type = document_type
        self.referer = referer
        self.interval = interval


class Worker(Greenlet):

    def __init__(self, id, itsy):
        self.id = id
        self.itsy = itsy
        Greenlet.__init__(self)

    def one(self):
        task = self.itsy.pop()
        print "%d: Handling task: [%s] %s" % (self.id,
                                              task.document_type,
                                              task.url)
        raw = client.get(url=task.url, referer=task.referer)

        handler = self.itsy.handlers[task.document_type]
        doc = Document(task, raw)
        result = handler(task, doc)
        if result:
            for new_task in result:
                print "  -> [%s] %s" % (new_task.document_type, new_task.url)
                if new_task.referer == sentinel:
                    new_task.referer = task.url
                self.itsy.push(new_task)

    def _run(self):
        while True:
            self.one()


class Itsy(object):

    def __init__(self):
        self.handlers = {}
        self.queue = Queue()

    def add_handler(self, document_type, func):
        assert document_type not in self.handlers
        self.handlers[document_type] = func

    def add_seed(self, url, document_type, referer=None, interval=None):
        self.push(Task(url=url, document_type=document_type,
                       referer=referer, interval=interval))

    def fetch(self, url, referer):
        r = requests.get(url)
        return r.text

    def pop(self):
        while True:
            try:
                return self.queue.get(True, 5)
            except Empty:
                print "TIMEOUT"

    def push(self, task):
        self.queue.put(task)

    def crawl(self, num_workers=20):
        workers = []
        for ii in range(num_workers):
            worker = Worker(ii, self)
            worker.start()
            workers.append(worker)
        gevent.joinall(workers)
