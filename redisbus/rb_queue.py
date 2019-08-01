"""
Queue based storage and tools on redis for mailboxes, logging, etc
"""
import json
import queue

from queue import Queue as ThreadQueue
import datetime
import logging
import threading
import time

import redis

from redisbus.utility import ISO_STRFTIME_FORMAT


class LogHandler(logging.Handler):
    """
    Logging system handler that pushes to a shared logging queue
    """
    LOG_TTL = 60*60*24
    LOG_MAX_ELEMENTS = 200

    def __init__(self, key, worker_id, connection_pool):
        logging.Handler.__init__(self)
        connection = redis.StrictRedis(connection_pool=connection_pool)
        self.key = key
        self.queue = Queue(key, connection)
        self.worker_id = worker_id

    def refresh_ttl(self):
        self.queue.connection.expire(self.key, LogHandler.LOG_TTL)

    def emit(self, record):
        # Push the log entry, reset the ttl to one hour and keep only the last 200 elements
        self.queue.push_constrained({
            'time': datetime.datetime.utcnow().strftime(ISO_STRFTIME_FORMAT),
            'worker_id': self.worker_id,
            'message': record.getMessage(),
            'filename': record.filename,
            'line': record.lineno,
            'level': record.levelname},
            ttl=LogHandler.LOG_TTL,
            trim_start=-LogHandler.LOG_MAX_ELEMENTS,
            trim_end=-1)


class Queue(object):
    """Wrapper for a blocking queue modeled on the redis list operations"""
    def __init__(self, name, connection):
        self.connection = connection
        self.name = name
        self.active = True

    def push(self, pyobj):
        self.connection.rpush(self.name, json.dumps(pyobj))

    def push_constrained(self, pyobj, ttl, trim_start, trim_end):
        with self.connection.pipeline(transaction=False) as pipe:
            pipe.rpush(self.name, json.dumps(pyobj))
            pipe.expire(self.name, ttl)
            pipe.ltrim(self.name, trim_start, trim_end)
            pipe.execute()

    def pop(self, wait=10):
        obj = None
        try:
            if wait > 0:
                obj = self.connection.blpop(self.name, wait)
                if obj:
                    obj = obj[1]
            else:
                obj = self.connection.lpop(self.name)
        except Exception as exc:
            self.active = False
            print('Failed pop() with keys: {} ({})'.format(self.name, str(exc)))

        try:
            if obj:
                decoded = json.loads(obj)
                return decoded
        except Exception as exc:
            print('Failed pop() while decoding message: {} ({})'.format(str(exc), str(obj)))
        return None

    def len(self):
        return self.connection.llen(self.name)

    def expire(self, time_seconds):
        return self.connection.expire(self.name, time_seconds)

    def clear(self):
        return self.connection.ltrim(self.name, 0, 0)


class Monitor(object):
    """Monitor multiple queue keys for values, blocking is done in a background thread"""
    def __init__(self, connection):
        self.queue_names_lock = threading.Lock()
        self.queue_names = []
        self.connection = connection
        self.active = False
        self.output_queue = ThreadQueue()

    def add_queue(self, queue_name):
        with self.queue_names_lock:
            self.queue_names.append(queue_name)

    def start(self):
        self.active = True
        thread = threading.Thread(target=Monitor.thread, args=(self,))
        thread.daemon = True

        thread.start()

    def stop(self):
        self.active = False

    def thread(self):
        """
        Use blocking pop to monitor a set of queues
        """
        while self.active:
            with self.queue_names_lock:
                queue_names = tuple(self.queue_names)
            if len(queue_names) > 0:
                obj = self.connection.brpop(queue_names, timeout=3)
                if obj:
                    json_message = obj[1]
                    json_doc = json.loads(json_message)
                    self.output_queue.put(json_doc)
            else:
                time.sleep(.2)

    def pop(self):
        try:
            obj = self.output_queue.get_nowait()
            self.output_queue.task_done()
            return obj
        except queue.Empty:
            return None
