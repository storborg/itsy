from gevent import monkey
monkey.patch_all(thread=False)

import logging
import logging.config
import sys

import gevent
from gevent import Greenlet

import requests

from .client import Client
from .document import Document
from .queue import Task, Queue, Empty

log = logging.getLogger(__name__)


class Worker(Greenlet):
    """
    An itsy worker. You should never have to use this directly.
    """
    def __init__(self, id, itsy):
        Greenlet.__init__(self)
        self.id = id
        self.itsy = itsy
        self.client = Client(proxies=itsy.proxies)

    def one(self):
        task = self.itsy.pop()
        log.info("%d: Handling task: [%s] %s",
                 self.id, task.document_type, task.url)
        raw = self.client.get(url=task.url, referer=task.referer)

        handler = self.itsy.handlers[task.document_type]
        doc = Document(task, raw)
        result = handler(task, doc)
        if result:
            for new_task in result:
                log.info("%d    -> [%s] %s%s",
                         self.id, new_task.document_type, new_task.url,
                         " HP" if new_task.high_priority else "")
                new_task.set_originating_task(task)
                self.itsy.push(new_task)

    def _run(self):
        while True:
            self.one()
            gevent.sleep(2)


class Itsy(object):
    """
    The ringleader of the Itsy web scraping framework.
    """
    def __init__(self, name, proxies=None):
        self.handlers = {}
        self.queue = Queue(name)
        self.proxies = proxies

    def add_handler(self, document_type, func):
        assert document_type not in self.handlers
        self.handlers[document_type] = func

    def add_seed(self, url, document_type, referer=None, repeat_after=None):
        self.push(Task(url=url, document_type=document_type,
                       referer=referer, repeat_after=repeat_after))

    def fetch(self, url, referer):
        r = requests.get(url)
        return r.text

    def pop(self):
        while True:
            try:
                return self.queue.pop()
            except Empty:
                log.warn("TIMEOUT")

    def push(self, task):
        self.queue.push(task)

    def crawl(self, num_workers=5):
        workers = []
        for ii in range(num_workers):
            worker = Worker(ii, self)
            worker.start()
            workers.append(worker)
        gevent.joinall(workers)


def configure_logging():
    """
    Helper to configure logging for a command-line script which uses Itsy.
    """
    logging.config.dictConfig({
        'formatters': {
            'generic': {
                'datefmt': '%H:%M:%S',
                'format':
                '%(asctime)s,%(msecs)03d %(levelname)s [%(name)s] %(message)s'
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'generic',
                'level': 'NOTSET',
                'stream': sys.stderr,
            },
            'null': {
                'class': 'logging.NullHandler',
            },
        },
        'loggers': {
            'itsy': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': False,
            },
        },
        'root': {
            'handlers': ['null'],
            'level': 'INFO',
        },
        'version': 1,
    })
