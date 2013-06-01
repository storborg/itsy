import logging

from datetime import timedelta
import time
import hashlib
import pickle

from redis import StrictRedis, WatchError

log = logging.getLogger(__name__)


class Task(object):
    """
    Represents a single crawl task, corresponding to a future HTTP request to a
    particular URL, with some additional properties governing request
    parameters and the scheduling of that crawl.
    """
    DEFAULT_REFERER = object()
    DEFAULT_DOCUMENT_TYPE = object()

    @staticmethod
    def to_integer_seconds(val):
        if isinstance(val, timedelta):
            return val.total_seconds()
        else:
            return val

    def __init__(self, url, method='GET', data=None,
                 document_type=DEFAULT_DOCUMENT_TYPE, referer=DEFAULT_REFERER,
                 repeat_after=None, scheduled_timestamp=0, high_priority=False,
                 min_age=3600):

        if high_priority and scheduled_timestamp:
            raise ValueError('task cannot be high priority and also deferred')

        self.url = url
        self.document_type = document_type
        self.referer = referer
        self.high_priority = high_priority
        self.scheduled_timestamp = scheduled_timestamp
        self.repeat_after = Task.to_integer_seconds(repeat_after)
        self.min_age = Task.to_integer_seconds(min_age)

    def set_originating_task(self, task):
        if self.referer == self.DEFAULT_REFERER:
            self.referer = task.referer
        if self.document_type == self.DEFAULT_DOCUMENT_TYPE:
            self.document_type = task.document_type

    def __repr__(self):
        return '<Task %s [%s]%s>' % (self.url, self.document_type,
                                     ' HP' if self.high_priority else '')


class Empty(Exception):
    """
    Raised when there are no tasks scheduled for now left in the queue.
    """
    pass


class Queue(object):
    """
    Implements a sort-of-queue of crawl tasks.
    """
    def __init__(self, name, url_canonicalizer=None):
        self.name = name
        self.canonicalize_url = url_canonicalizer or (lambda url: url)
        self.redis = StrictRedis(host='localhost', port=6379, db=0)
        # XXX Temporary
        self.redis.flushall()

    def serialize(self, task):
        return pickle.dumps(task)

    def deserialize(self, s):
        return pickle.loads(s)

    def prefix_redis_key(self, prefix, key):
        return ':'.join([self.name, prefix, key])

    def pick_queue_key(self, high_priority):
        return self.prefix_redis_key('todo', 'hp' if high_priority else 'nn')

    def hash_url(self, url):
        return hashlib.md5(url).hexdigest()

    def record_crawl_timestamp(self, url, now):
        self.redis.set(self.prefix_redis_key('urlts', self.hash_url(url)),
                       '%d' % now)

    def get_crawl_timestamp(self, url):
        s = self.redis.get(self.prefix_redis_key('urlts', self.hash_url(url)))
        if s:
            return int(s)

    def get_existing_task_id(self, url):
        s = self.redis.get(self.prefix_redis_key('taskbyurl',
                                                 self.hash_url(url)))
        if s:
            task_id, high_priority_bool = s.split(',')
            return task_id, high_priority_bool == '1'
        else:
            return None, None

    def get_task(self, task_id):
        s = self.redis.get(task_id)
        return self.deserialize(s)

    def drop_task(self, task_id, high_priority):
        self.redis.zrem(self.pick_queue_key(high_priority), task_id)
        self.redis.delete(task_id)

    def push(self, task):
        """
        Schedule a new crawl task, which is an instance of ``Task``.

        If ``high_priority`` is set, drop all other tasks for the same
        canonicalized URL and enqueue to a separate high priority queue.

        If ``min_age`` is set and this task URL has been crawled before, check
        the last time it was crawled, and reschedule it to a timestamp which is
        ``min_age`` after the last crawl time.

        If a different crawl task is already scheduled for this URL, check the
        scheduled time. If it has an intentionally delayed crawl time and that
        scheduled time is *after* this task, drop it. Otherwise, drop this
        task. Either way, the earlier of the two tasks should be kept.
        """
        # XXX FIXME
        # We really should take a lock on this URL, to prevent race conditions
        # in the conditional logic below.

        existing_task_id, existing_is_hp = self.get_existing_task_id(task.url)

        if task.high_priority:
            # If this task is HP, drop any other task for this URL..
            if existing_task_id:
                self.drop_task(existing_task_id, existing_is_hp)

        elif existing_is_hp:
            # If there's an existing high priority task, drop this one.
            return

        else:
            # If min_age is set, check for previous crawls and further delay
            # this task in order to ensure that min_age is not violated.
            if task.min_age:
                last_timestamp = self.get_crawl_timestamp(task.url)
                if last_timestamp:
                    task.scheduled_timestamp = max(last_timestamp +
                                                   task.min_age,
                                                   task.scheduled_timestamp)

            # If there is an existing task, check if it's scheduled earlier.
            if existing_task_id:
                scheduled_ts = self.redis.zscore(
                    self.pick_queue_key(existing_is_hp),
                    existing_task_id)

                if scheduled_ts > task.scheduled_timestamp:
                    # If the existing task is later, drop it.
                    self.drop_task(existing_task_id, existing_is_hp)
                else:
                    # If this task is later, drop it and keep the existing
                    # task.
                    return

        # At this point we're ready to enqueue.
        s = self.serialize(task)
        task_id = hashlib.md5(s).hexdigest()

        # Store the task by ID.
        self.redis.set(task_id, s)

        # Finally, enqueue the task ID to the appropriate sorted set.
        self.redis.zadd(self.pick_queue_key(task.high_priority),
                        float(task.scheduled_timestamp), task_id)

    def pop_queue(self, high_priority, cutoff):
        key = self.pick_queue_key(high_priority)
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    elements = pipe.zrangebyscore(key, min=0, max=cutoff,
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
        Get the next scheduled crawl task, or raise Empty() if there is nothing
        to do.
        """
        now = time.time()
        for high_priority in (True, False):
            next_task_id = self.pop_queue(high_priority, now)
            if next_task_id:
                break

        # Nothing to do?
        if not next_task_id:
            raise Empty()

        # Otherwise, fetch the task and deserialize it.
        s = self.redis.get(next_task_id)
        self.redis.delete(next_task_id)
        task = self.deserialize(s)

        # Record a crawl timestamp for this URL.
        self.record_crawl_timestamp(task.url, now)

        # If the task has a repeat interval, reschedule it.
        if task.repeat_after:
            repeat_task = Task(url=task.url,
                               document_type=task.document_type,
                               repeat_after=task.repeat_after,
                               scheduled_timestamp=now + task.repeat_after)
            self.push(repeat_task)

        return task
