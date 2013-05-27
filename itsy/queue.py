"""
A work queue implementation to serve itsy. Uses redis.

Basically the goal is to support these mechanisms:

    - Enqueue a crawl task (which is a Task instance)
    - Enqueue a crawl task to be executed no earlier than time X
    - Enqueue a crawl task at a high priority (high priority tasks have a
      separate queue which is always checked first)
    - Dequeue a crawl task

The rough idea is to use a sorted set per queue.

Enqueue like this:

ZADD todo <scheduled epoch> <task ID>

Then dequeue like this:

WATCH todo
element = ZRANGEBYSCORE todo 0 <current epoch> LIMIT 0 1
MULTI
ZREM todo element
EXEC

Which should be a nonblocking pop of the next task ID which has a scheduled
time prior to now.

TODO
- Figure out how to support a global rate limit or a per-domain rate limit.
"""
import pickle
import hashlib
import time

from redis import StrictRedis, WatchError


class Empty(Exception):
    """
    Raised when there is nothing to do.
    """
    pass


class Queue(object):

    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = StrictRedis(host=host, port=port, db=db)
        # XXX Temporary
        self.redis.flushall()

    def serialize(self, task):
        return pickle.dumps(task)

    def deserialize(self, s):
        return pickle.loads(s)

    def push(self, task):
        """
        Enqueue a new task.

        - Create a new task ID.
        - Serialize the task.
        - Store the task with SET.
        - Enqueue the task ID to the appropriate sorted set.
        """
        s = self.serialize(task)
        key = hashlib.md5(s).hexdigest()

        self.redis.set(key, s)

        # FIXME it would be nice if this would use a key that's unique to the
        # project, so that multiple itsy instances could use the same redis
        # server.
        queue_name = 'todo-hp' if task.high_priority else 'todo'
        self.redis.zadd(queue_name, float(task.scheduled_timestamp), key)

    def pop_queue(self, queue_name, cutoff):
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(queue_name)
                    elements = pipe.zrangebyscore(queue_name, min=0, max=cutoff,
                                                  start=0, num=1)
                    if elements:
                        key = elements[0]
                        pipe.multi()
                        pipe.zrem(queue_name, key)
                        pipe.execute()
                        return key
                    else:
                        return
                except WatchError:
                    continue

    def pop(self):
        """
        Pop the next task.

        - Pop the next task ID.
        - Fetch the task with GET.
        - Delete the task with DEL.
        - Deserialize the task.

        If there is no next task ID on any queue raise Empty().
        """
        now = time.time()
        for queue_name in ('todo-hp', 'todo'):
            key = self.pop_queue(queue_name, now)
            if key:
                break

        if not key:
            raise Empty()
        assert key

        s = self.redis.get(key)
        self.redis.delete(key)
        return self.deserialize(s)
