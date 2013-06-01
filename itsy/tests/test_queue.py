from unittest import TestCase

from itsy.queue import Queue
from itsy import Task


class TestQueue(TestCase):

    def test_roundtrip(self):
        task = Task('http://www.example.com', 'plain')

        q = Queue('test')
        q.push(task)

        task2 = q.pop()
        self.assertEqual(task.url, task2.url)
