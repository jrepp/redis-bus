"""Definition of base worker and helper methods"""
import atexit
import json
import os
from uuid import uuid4
import socket
import time
import logging
import datetime
import redis
import traceback

from redisbus import client
from redisbus.rb_queue import LogHandler, Monitor
from redisbus import utility

g_workers_key = "workers"


class Message(object):
    """Constants for messages on the bus"""
    COMMAND = 'x'           # command (execute)
    ORIGIN_ID = 'i'         # i: id (sender), destination is implied by queue being read
    CORRELATION = 'c'       # correlationId
    DATA = 'd'              # data payload
    STREAM_COUNT = 'z'      # message count or termination, stream terminates when z == -1


def generate_worker_id():
    """
    Generates a unique string ID for a worker that contains the host and process
    """
    hostname = socket.gethostname()
    pid = os.getpid()
    uid = uuid4()
    host_addr = socket.gethostbyname(hostname)
    return ':'.join([host_addr, str(pid), str(uid).split('-')[-1]])


def info_from_worker_id(worker_id_str):
    """
    Convert a worker uuid into a tuple of host, pid and uid
    """
    host, pid, uid = worker_id_str.split(':')
    return host, pid, uid


class CommandContext(object):
    """Context provided to command workers, holds the correlation and client needed to reply
    back to the originating caller"""
    def __init__(self, worker, data, correlation):
        self.worker = worker
        self.client = client.Client(redis.StrictRedis(connection_pool=worker.connection_pool), worker.site, worker.log)
        self.data = data
        self.correlation = correlation
        self.did_reply = False

    def reply(self, data=None):
        assert self.correlation is not None
        self.client.reply(self.worker.id, self.correlation, data)
        self.did_reply = True

    def reply_failure(self, msg):
        data = {'success': False, 'msg': msg}
        self.client.reply(self.worker.id, self.correlation, data)
        self.did_reply = True
        self.worker.log.error('command "{}" failed {}'.format(str(data), msg))

    def reply_success(self, msg='OK', **kwargs):
        data = {'success': True, 'msg': msg}  # Msg just for symmetry
        data.update(kwargs)
        self.client.reply(self.worker.id, self.correlation, data)
        self.did_reply = True


class Subscription(object):
    """Represents a pubsub binding to a key in redis"""
    def __init__(self, channel_names, connection_pool):
        self.connection_pool = connection_pool
        self.channel_names = channel_names
        self.establish_conn()

    def establish_conn(self):
        connection = redis.StrictRedis(connection_pool=self.connection_pool)
        self.pubsub = connection.pubsub()
        self.subscribe()

    def subscribe(self):
        return self.pubsub.subscribe(self.channel_names)

    def unsubscribe(self):
        return self.pubsub.unsubscribe(self.channel_names)

    def get_message(self):
        max_retry = 3
        retry = 0

        while retry < max_retry:
            try:
                return self.pubsub.get_message()
            except redis.ConnectionError:
                self.establish_conn()
            retry += 1

    def __enter__(self):
        self.subscribe()

    def __exit__(self, *args):
        self.unsubscribe()


class Arguments(object):
    """
    Pass in a config to override worker arguments
    """

    def __init__(self, config=None):

        if config is None:
            config = {}

        # Site namespace to use for this worker
        self.site = config.get('site')

        # Worker type name, often the name of the script that contains the worker code
        self.worker_type = config.get('worker')

        # Worker path for file operations, can be different from CWD
        self.worker_path = config.get('worker_path')

        # Interval to call worker_tick()
        self.worker_interval = config.get('worker_interval', 0.4)

        # Optional source ID of the spawner that started this worker
        self.spawner = config.get('spawner')

        # Should be created before accessing redis resources
        self.connection_pool = None

        # Set to override the internal worker log, else one will be created with file logging etc
        self.log = None


class ClassFactory(object):
    """Registers and constructs worker classes by name"""
    def __init__(self):
        self.workers = {}

    def register(self, name, cls_type):
        self.workers[name] = cls_type

    def construct(self, name):
        self.workers[name]()


class Worker(object):
    """
    Base worker type that loops and calls inner_eval(self, o) for each
    item in the queue.
    """
    BROADCAST_DISCOVERY_KEY = 'discovery:worker'
    BROADCAST_RPC_KEY = 'rpc:worker'

    def __init__(self, args, worker_id=None):
        self.id = worker_id or generate_worker_id()
        self.type_name = args.worker_type
        self.site = args.site
        self.active = True
        self.args = args
        self.info = {}
        self.connection_pool = args.connection_pool
        self.interval = args.worker_interval
        self.broadcast_rpc_key = "{}:{}".format(Worker.BROADCAST_RPC_KEY, self.site)
        self.startup_time = time.time()
        self.maintenance_interval_seconds = 10
        self.last_maintenance = self.startup_time - self.maintenance_interval_seconds
        self.queue_monitor = None
        self.allow_downloads = False
        self.log_files = []
        self.files = []
        self.last_tick = time.time()
        self.tick_count = 0
        self.log_queue_handler = None
        self.rpc_subscription = None

        self.log = args.log or self.make_log()
        self.connect()

        def cleanup():
            self.remove_worker_info_key()

        atexit.register(cleanup)

    def make_log(self):
        """
        Build a custom logger that writes to the queue
        """
        name = '{}_{}'.format(self.type_name, self.id.split(':')[-1])

        # Get a root logger for the worker
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)

        if not os.path.exists('logs'):
            os.mkdir('logs')

        # Create file handler which logs debug messages
        iso_format_str = datetime.date.today().isoformat()
        log_file_name = 'logs/{}-{}.log'.format(name, iso_format_str)
        file_handler = logging.FileHandler(log_file_name)
        file_handler.setLevel(logging.DEBUG)

        # Track the file used for logging
        self.log_files.append(log_file_name)

        # Create the queue handler to report logs to a central repo
        log_key = 'logs:{}:{}'.format(self.type_name, self.id)
        self.log_queue_handler = LogHandler(log_key, self.id, self.connection_pool)
        self.log_queue_handler.setLevel(logging.INFO)

        # Create formatter and add it to the handlers
        formatter = utility.log_formatter()
        file_handler.setFormatter(formatter)
        self.log_queue_handler.setFormatter(formatter)

        # Add the core handlers to the logger
        log.addHandler(file_handler)
        log.addHandler(self.log_queue_handler)

        log.info('worker {} logging configured'.format(self.id))

        return log

    def connect(self):
        # kill the old queue monitor if any
        if self.queue_monitor:
            self.queue_monitor.stop()

        # spin up a new queue monitor
        self.queue_monitor = Monitor(redis.StrictRedis(connection_pool=self.connection_pool))
        self.queue_monitor.start()

        self.queue_monitor.add_queue("direct:{}".format(self.id))
        self.queue_monitor.add_queue("group:{}:{}".format(self.site, self.type_name))
        self.rpc_subscription = Subscription(self.broadcast_rpc_key, self.connection_pool)

    def loop(self):
        try:
            # Call worker startup
            self.worker_startup()
            self.last_tick = time.time()
            self.log.info("worker_startup complete")
            while self.active and self.queue_monitor.active:
                self.loop_inner()
        except KeyboardInterrupt:
            print("Interrupted via keyboard")
        except Exception as exc:
            self.log.error("Worker execution failed: {}: {}".format(exc, traceback.format_exc()))
            raise
        finally:
            self.log.info("worker_shutdown")
            self.remove_worker_info_key()
            self.worker_shutdown()

    def loop_inner(self):
        # Get elapsed since last tick
        now = time.time()
        elapsed = now - self.last_tick
        self.last_tick = now

        self.read_direct_messages()
        self.read_broadcast_messages()
        self.worker_tick(elapsed)

        self.tick_count += 1

        sleep_remainder = self.interval - elapsed
        if sleep_remainder > 0:
            time.sleep(sleep_remainder)

        # Do regular maintenance
        if now - self.last_maintenance > self.maintenance_interval_seconds:
            # self.broadcast_discovery()
            self.log_queue_handler.refresh_ttl()
            self.update_worker_info_key()
            tick_rate = self.tick_count / (now - self.last_maintenance)
            self.tick_count = 0
            self.log.debug("tick rate %.3f", tick_rate)
            self.last_maintenance = time.time()

    def read_direct_messages(self):
        while True:
            obj = self.queue_monitor.pop()
            if obj is None:
                break
            self.process_message(obj)

    def read_broadcast_messages(self):
        # Handle any broadcast messages
        while True:
            message = self.rpc_subscription.get_message()
            if message is None:
                break
            # Skip subscribe notification
            if message['type'] == 'subscribe':
                continue
            elif message['type'] == 'message' and message['channel'] == self.broadcast_rpc_key:
                try:
                    message_data = json.loads(message['data'])
                    self.process_message(message_data)
                except Exception as exc:
                    self.log.error("Decode failed for {} ({})".format(message_data, str(exc)))
                    raise

    def subscribe(self, key):
        self.pubsub_connection.subscribe(key)

    def broadcast_discovery(self):
        publish_key = Worker.BROADCAST_DISCOVERY_KEY
        self.queue_monitor.connection.publish(publish_key, self.worker_info_dict())

    def get_worker_ids(self, host, pid_list):
        worker_id_list = list()
        worker_dict = self.queue_monitor.connection.hgetall(g_workers_key)
        for _, value in worker_dict.items():
            worker_host, worker_pid, worker_uid = info_from_worker_id(value)
            if host == worker_host and worker_pid in pid_list:
                worker_id_list.append(value)
        return worker_id_list

    def remove_worker_info_key(self):
        try:
            key = "worker:{}:{}:{}".format(self.site, self.type_name, self.id)
            self.queue_monitor.connection.hdel(g_workers_key, key)
            self.queue_monitor.connection.delete(key)
        except Exception as exc:
            self.log.error('Failed to remove worker info key {}', str(exc))

    def update_worker_info_key(self):
        key = "worker:{}:{}:{}".format(self.site, self.type_name, self.id)
        self.queue_monitor.connection.hset(g_workers_key, key, self.id)
        self.queue_monitor.connection.set(key, json.dumps(self.worker_info_dict()),
                                          ex=self.maintenance_interval_seconds + 3)

    def worker_info_dict(self):
        info = {
            'site': self.site,
            'id': self.id,
            'type': type(self).__name__,
            'worker': self.type_name,
            'uptime': self.uptime(),
            'path': self.args.worker_path,
            'spawner': self.args.spawner,
            'cwd': os.getcwd(),
            'username': os.getenv('username'),
            'interval': self.args.worker_interval,
            'logs': self.log_files,
            'files': self.worker_files()
        }
        info.update(self.info)
        return info

    def uptime(self):
        return self.last_maintenance - self.startup_time

    #
    # Worker extension methods
    #
    def worker_tick(self, elapsed):
        pass

    def worker_startup(self):
        pass

    def worker_shutdown(self):
        pass

    @staticmethod
    def worker_files():
        return []

    @staticmethod
    def worker_log_entries():
        return []

    #
    # Main message processor
    #
    def process_message(self, message):
        command_name = message.get(Message.COMMAND)
        context = CommandContext(self, message.get(Message.DATA), message.get(Message.CORRELATION))
        func_name = 'cmd_' + command_name
        self.log.debug("calling command {} on {}".format(func_name, self.id))
        status_msg = None
        try:
            func = getattr(self, func_name)
            func(context)
            success = True
        except AttributeError as attribute_err:
            status_msg = "Unknown command function '{}' for worker '{}' ({})".format(
                func_name, type(self).__name__, str(attribute_err))
            success = False
        except Exception as exc:
            status_msg = "An exception occurred while executing command function '{}' for worker '{}'  - {} - {}"\
                .format(func_name, type(self).__name__, str(exc), traceback.format_exc())
            success = False

        if not success:
            self.log.error(status_msg)
            self.log.error("Message failed: {}".format(message))
            if not context.did_reply:
                context.reply_failure(status_msg)
        else:
            if not context.did_reply:
                context.reply_success()
        return success

    def cmd_info(self, context):
        worker_info = self.worker_info_dict()
        worker_info['commands'] = [attr.replace('cmd_', '')
                                   for attr in dir(self) if callable(getattr(self, attr)) and attr.startswith("cmd")]
        worker_info['success'] = True
        context.reply(worker_info)

    def cmd_stop(self, context):
        context.reply_success()
        self.active = False

    def cmd_ping(self, context):
        self.log.info('Recieved ping with data {}'.format(context.data))
        context.reply(context.data)

    def cmd_download(self, context):
        if not self.allow_downloads:
            return context.reply_failure("Downloads disabled for this worker")
        full_path = os.path.join(self.args.worker_path, context.data)
        self.log.info('Beginning download for {}'.format(full_path))
        context.reply(utility.chunk_file(full_path))

    def cmd_download_dir(self, context):
        if not self.allow_downloads:
            return context.reply_failure("Downloads disabled for this worker")
        full_path = os.path.join(self.args.worker_path, context.data)
        self.log.info('Beginning directory compressed download for {}'.format(full_path))
        context.reply(utility.compress_and_chunk_file(full_path, log=self.log))

    def cmd_update_spawner(self, context):
        self.args.spawner = context.data
        self.update_worker_info_key()
        context.reply_success()
