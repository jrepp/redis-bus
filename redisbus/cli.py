"""
This module defines the command line entry point for the redisbus library
"""

import os
import argparse
import logging
import pprint

import redis

from redisbus import client
from redisbus.config import load_file
from redisbus.worker import Worker


def start_worker_file(args, config):
    """
    Start a worker with a set of arguments and specified configurations
    """
    # Setup the worker startup script
    worker_type = args.worker_type
    worker = None
    worker_script = os.path.join('workers', worker_type, worker_type + '.py')
    context = {'__file__': worker_script}
    context.update(config.globals)
    context.update(config.workers)

    with open(worker_script) as f:
        code = compile(f.read(), worker_script, 'exec')
        exec(code, context, context)

    root_log = logging.getLogger()

    # Fixup args
    args.log = None  # Allow worker to create it's own log object

    for k, cls in context.items():
        if type(cls) is not type:
            continue

        if issubclass(cls, Worker) and cls is not Worker:
            worker = cls(args)
            root_log.info("{} started, id: {}".format(cls.__name__, worker.id))
            break

    if worker is None:
        root_log.error("No worker script found for '{}' in workers/".format(worker_type))
    else:
        worker.loop()


def main():
    """ Entry point for command line usage of the bus"""
    # Create console handler
    log = logging.getLogger()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    log.addHandler(console_handler)
    log.setLevel(logging.DEBUG)

    # Use a format compatible with pycharm messages
    formatter = logging.Formatter('%(pathname)s:%(lineno)s: [%(levelname)s %(name)s] %(message)s ')
    console_handler.setFormatter(formatter)

    script_path = os.getcwd()
    config_file = os.path.join(script_path, '.worker_config')
    config = load_file(config_file)
    log.info('Loading config from {}'.format(config_file))

    parser = argparse.ArgumentParser(description='Generic command line interface to redisbus.')
    parser.add_argument('--call', type=str, default=None, help='call (RPC) to execute')
    parser.add_argument('--wait', type=float, default=1, help='wait time for RPC response')
    parser.add_argument('--data', type=str, default='', help='data for command')
    parser.add_argument('--jsondata', type=str, default='',
                        help='data for command (json formatted)')
    parser.add_argument('--hostname', dest='hostname', type=str,
                        default=config.globals.get('redis_hostname') or 'localhost',
                        help='redis hostname')
    parser.add_argument('--port', dest='port', type=int, action='store',
                        default=config.globals.get('redis_port') or 6379,
                        help='redis port')
    parser.add_argument('--db', dest='db', type=int, action='store',
                        default=config.globals.get('redis_db') or 0,
                        help='redis database')
    parser.add_argument('--worker', dest='worker_type', type=str, action='store',
                        default=config.globals.get('worker') or None,
                        help='name of worker type to run, or to address for --call messages')
    parser.add_argument('--worker_id', dest='worker_id', type=str, action='store',
                        default=None,
                        help='worker ID used to address direct calls')
    parser.add_argument('--worker_interval', dest='worker_interval', type=float, action='store',
                        default=.4,
                        help='interval to tick workers')
    parser.add_argument('--worker_path', dest='worker_path', type=str, action='store',
                        default=config.globals.get('worker_path') or script_path,
                        help='path for worker operations')
    parser.add_argument('--multicast', dest='multicast', type=str, action='store',
                        default=None,
                        help='pattern for multicasting to workers e.g.: 10.130.*/10.130.10.13:*')
    parser.add_argument('--local', dest='local', type=bool, action='store',
                        default=False,
                        help='only send to workers local workers (worker_id will override)')
    parser.add_argument('--site', dest='site', type=str, action='store',
                        default=config.globals.get('site') or 'local',
                        help='site name to use for workers')
    parser.add_argument('--spawner', dest='spawner', type=str, action='store',
                        default=None,
                        help='spawning worker ID when launched from spawn')
    parser.add_argument('--verbose', dest='verbose', action='store_true',
                        help='Enable debug logging')
    args = parser.parse_args()

    log.info('Connecting to redis {}:{}, db: {}'.format(args.hostname, args.port, args.db))
    args.connection_pool = redis.ConnectionPool(host=args.hostname, port=args.port, db=args.db, encoding="utf-8",
                                                decode_responses=True)
    args.config = config
    args.command_ttl = 10

    if args.verbose:
        console_handler.setLevel(logging.DEBUG)
    else:
        console_handler.setLevel(logging.INFO)

    args.log = log

    if args.call is not None:
        for reply in client.perform_rpc(args):
            printer = pprint.PrettyPrinter(indent=4)
            printer.pprint(reply)
    elif args.worker_type is not None:
        start_worker_file(args, config)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
