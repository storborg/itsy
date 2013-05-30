import logging

from datetime import timedelta
import pickle
import hashlib
import time

from redis import StrictRedis, WatchError

log = logging.getLogger(__name__)


class Empty(Exception):
    """
    Raised when there is nothing to do.
    """
    pass


class Task(object):
    DEFAULT_REFERER = object()

    def __init__(self, url, document_type, referer=DEFAULT_REFERER,
                 interval=None, scheduled_timestamp=0, high_priority=False,
                 min_age=3600):
        self.url = url
        self.document_type = document_type
        self.referer = referer
        self.high_priority = high_priority
        self.scheduled_timestamp = scheduled_timestamp

        if isinstance(interval, timedelta):
            self.interval = interval.total_seconds()
        else:
            self.interval = interval

        if isinstance(min_age, timedelta):
            self.min_age = min_age.total_seconds()
        else:
            self.min_age = min_age

    def __repr__(self):
        return '<Task %s [%s]>' % (self.url, self.document_type)


class Queue(object):
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

    def __init__(self, name, host='localhost', port=6379, db=0):
        self.name = name
        self.redis = StrictRedis(host=host, port=port, db=db)
        # XXX Temporary
        self.redis.flushall()

    def serialize(self, task):
        return pickle.dumps(task)

    def deserialize(self, s):
        return pickle.loads(s)

    def prefix_key(self, prefix, key):
        return ':'.join([self.name, prefix, key])

    def queue_key(self, high_priority):
        return self.prefix_key('todo', 'hp' if high_priority else 'nn')

    def set_url_timestamp(self, url):
        key = hashlib.md5(url).hexdigest()
        self.redis.set(self.prefix_key('urlts', key), '%d' % time.time())

    def get_url_timestamp(self, url):
        key = hashlib.md5(url).hexdigest()
        s = self.redis.get(self.prefix_key('urlts', key))
        if s:
            return int(s)

    def push(self, task):
        """
        Enqueue a new crawl task.
        """
        # Check the last time the URL was crawled.
        last_crawl = self.get_url_timestamp(task.url)

        # If it was crawled recently enough that the next scheduled time will
        # result in a recrawl sooner than min_age, bump out the scheduled time
        # accordingly.
        if last_crawl:
            earliest_crawl = last_crawl + task.min_age
            log.error("DELAYING due to min age by %d seconds",
                      earliest_crawl - task.scheduled_timestamp)
            task.scheduled_timestamp = max(task.scheduled_timestamp,
                                           earliest_crawl)

        # Serialize the task.
        s = self.serialize(task)

        # Create a task ID using the md5 of the serialized task.
        task_id = hashlib.md5(s).hexdigest()

        # Store the task by ID.
        self.redis.set(task_id, s)

        # Enqueue teh task ID to the appropriate sorted set, which acts as a
        # scheduled task queue.
        key = self.queue_key(task.high_priority)
        self.redis.zadd(key, float(task.scheduled_timestamp), task_id)

    def pop_queue(self, high_priority, cutoff):
        key = self.queue_key(high_priority)
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    elements = pipe.zrangebyscore(key, min=0,
                                                  max=cutoff,
                                                  start=0, num=1)
                    if elements:
                        task_id = elements[0]
                        pipe.multi()
                        pipe.zrem(key, task_id)
                        pipe.execute()
                        return task_id
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
        # Pop a task ID, if there is one that's scheduled.
        now = time.time()
        for high_priority in (True, False):
            key = self.pop_queue(high_priority, now)
            if key:
                break

        # If we didn't get a task ID, raise Empty()
        if not key:
            raise Empty()

        # If we did, fetch the task and deserialize it.
        s = self.redis.get(key)
        self.redis.delete(key)
        task = self.deserialize(s)

        # Record a timestamp for the URL.
        self.set_url_timestamp(task.url)

        # If the task has an interval, reschedule it.
        if task.interval:
            repeat_task = Task(url=task.url,
                               document_type=task.document_type,
                               interval=task.interval,
                               scheduled_timestamp=now + task.interval)
            self.push(repeat_task)

        return task
