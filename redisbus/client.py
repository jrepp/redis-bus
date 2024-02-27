"""
Client connection helper functions and classes
"""
import json
import logging
import time
import redis
import traceback
import types

from uuid import uuid4

from redisbus.rb_queue import Queue
from redisbus.worker import Message, Worker


class Arguments(object):
    """
    Set of arguments required to perform a RPC
    """

    def __init__(self, config, connection_pool, worker_id, call, data=None):
        self.connection_pool = connection_pool
        self.site = config['site']
        self.worker_id = worker_id
        self.call = call
        self.data = data
        self.jsondata = None
        self.worker = None
        self.multicast = None
        self.wait = 2
        self.command_ttl = 10


class Client(object):
    """
    Client that connects to the broker and requests commands
    """

    def __init__(self, connection, site, log=None, command_ttl=10):
        self.connection = connection
        self.site = site
        self.max_commands = 100
        self.command_ttl = command_ttl

        if not log:
            self.log = self.make_log()
        else:
            self.log = log

    @staticmethod
    def make_log():
        log = logging.getLogger('client')
        return log

    def call_direct(self, src_id, dst_id, command, data=None, correlation=None):
        key = 'direct:{}'.format(dst_id)
        return self.call(src_id, key, command, data, correlation)

    def call_group(self, src_id, site, worker_type, command, data=None, correlation=None):
        key = 'group:{}:{}'.format(site, worker_type)
        return self.call(src_id, key, command, data, correlation)

    def call(self, src_id, key, command, data=None, correlation=None):
        """Single call, single return. The reply queue is provided to the caller"""
        cid = correlation or "c:{}".format(str(uuid4()).split('-')[-1])
        queue = Queue(key, self.connection)
        self.log.info("call({}), key: '{}', data: '{}', correlation: '{}'".format(command, key, data, cid))
        queue.push({Message.COMMAND: command, Message.DATA: data, Message.CORRELATION: cid, Message.ORIGIN_ID: src_id})
        queue.connection.expire(key, self.command_ttl)
        return Queue('reply:' + cid, self.connection), 1

    def broadcast(self, src_id, command, data=None):
        cid = "b:{}".format(str(uuid4()).split('-')[-1])
        self.log.info("broadcast({}), data: '{}', correlation '{}'".format(command, data, cid))

        broadcast_key = "{}:{}".format(Worker.BROADCAST_RPC_KEY, self.site)
        self.connection.publish(broadcast_key, json.dumps({'x': command, 'd': data, 'c': cid, 'i': src_id}))
        return Queue('reply:' + cid, self.connection), None

    def multicast(self, src_id, multicast, command, data):
        pattern = 'worker:{}:{}'.format(self.site, multicast)
        cid = "m:{}".format(str(uuid4()).split('-')[-1])
        self.log.info("multicast({}), pattern: '{}".format(command, pattern))
        count = 0
        for key in self.connection.scan_iter(pattern):
            direct_id = 'direct:{}'.format(':'.join(key.split(':')[3:]))
            self.log.debug('multicasting to {}'.format(direct_id))
            self.call(src_id, direct_id, command, data, cid)
            count += 1
        return Queue('reply:' + cid, self.connection), count

    def reply(self, src_id, correlation, data=None):
        """Automatically reply using the provided correlation ID as the reply queue"""
        reply_q = Queue('reply:' + correlation, self.connection)
        if isinstance(data, types.GeneratorType):
            count = 0
            try:
                for element in data:
                    reply_q.push({Message.CORRELATION: correlation,
                                  Message.DATA: element,
                                  Message.ORIGIN_ID: src_id,
                                  Message.STREAM_COUNT: count})
                    count += 1
                reply_q.push({Message.CORRELATION: correlation,
                              Message.DATA: None,
                              Message.ORIGIN_ID: src_id,
                              Message.STREAM_COUNT: -1})
                reply_q.expire(max(self.command_ttl * count, 300))
            except Exception as exc:
                status_msg = "An exception occurred while replying to correlation {} - {} - {}".format(
                    correlation, str(exc), traceback.format_exc())
                reply_q.push({Message.CORRELATION: correlation,
                              Message.DATA: {'success': False, 'msg': status_msg},
                              Message.ORIGIN_ID: src_id})
                reply_q.expire(self.command_ttl)
        else:
            reply_q.push({Message.CORRELATION: correlation, Message.DATA: data, Message.ORIGIN_ID: src_id})
            reply_q.expire(self.command_ttl)


def perform_rpc(args):
    """
    Perform an RPC with the provided arguments yielding a reply from each worker
    that provides a payload.
    """
    conn = redis.StrictRedis(connection_pool=args.connection_pool)
    client = Client(connection=conn,
                    site=args.site,
                    log=args.log,
                    command_ttl=args.command_ttl)

    # not originating from a worker
    src_id = ""

    if args.jsondata:
        client.log.debug('decoding {}'.format(args.jsondata))
        args.data = json.loads(args.jsondata)

    if args.worker_id is not None:
        reply_queue, wait_count = client.call_direct(src_id, args.worker_id, args.call, args.data)
    elif args.multicast is not None:
        reply_queue, wait_count = client.multicast(src_id, args.multicast, args.call, args.data)
    elif args.worker_type is not None:
        reply_queue, wait_count = client.call_group(src_id, args.site, args.worker_type, args.call, args.data)
    else:
        reply_queue, wait_count = client.broadcast(src_id, args.call, args.data)

    # Pop all responses up to args.wait time
    start_time = time.time()
    deadline = start_time + args.wait
    reply_count = 0
    while True:
        reply = reply_queue.pop(wait=1)
        if reply:
            # Provide the message reply
            yield reply
            reply_count += 1

            # explicitly continue
            stream_count = reply.get(Message.STREAM_COUNT)
            if stream_count is not None and stream_count >= 0:
                continue
        else:
            # Check timeout
            now = time.time()
            if now >= deadline:
                break

        if wait_count is not None and reply_count == wait_count:
            break

    if reply_count == 0:
        client.log.error("failed to receive reply in {} seconds".format(deadline - start_time))
    else:
        client.log.info("received {} replies for {}".format(reply_count, reply_queue.name))


def process_queue(queue, deadline_seconds=3, wait_count=None):
    """Yields message objects from the queue"""
    deadline = time.time() + deadline_seconds
    message_count = 0
    while True:
        message = queue.pop(wait=1)
        if message:
            # Provide the message object to the caller
            yield message
            message_count += 1

            # print('popped {} {}'.format(message.get(Message.CORRELATION), message.get(Message.STREAM_COUNT)))
            # Explicitly continue based on message 'z' value
            stream_count = message.get(Message.STREAM_COUNT)
            if stream_count is not None and stream_count >= 0:
                continue
        else:
            # Check timeout
            now = time.time()
            if now >= deadline:
                break

        if wait_count is not None and message_count >= wait_count:
            break
