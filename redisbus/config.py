"""
Simple python based config system
"""
import os


class Config(object):
    """
    Loads two dictionaries named 'global_config' and 'worker_config' into
    members of this class instance.
    """

    GLOBAL_CONFIG_KEY = 'global_config'
    WORKER_CONFIG_KEY = 'worker_config'

    def __init__(self):
        self.globals = dict()
        self.workers = dict()
        self.game = dict()

    def load_file(self, config_file):
        if not os.path.exists(config_file):
            self.set_defaults()
            return

        context = {}
        with open(config_file) as f:
            code = compile(f.read(), config_file, 'exec')
            exec(code, context, context)

        if Config.GLOBAL_CONFIG_KEY not in context:
            raise Exception("The global_config key was not defined in the config {}".format(config_file))
        if Config.WORKER_CONFIG_KEY not in context:
            raise Exception("The worker_config key was not defined in the config {}".format(config_file))

        _globals = context['global_config']
        workers = context['worker_config']
        game = context['game_config']

        assert(type(_globals) == dict)
        assert (type(workers) == dict)
        assert (type(game) == dict)

        self.globals = _globals
        self.workers = workers
        self.game = game

        self.set_defaults()

    def set_defaults(self):
        #  Set defaults using environment override
        self.globals.setdefault('redis_hostname', os.environ.get('REDIS_HOSTNAME', 'localhost'))
        self.globals.setdefault('redis_port', os.environ.get('REDIS_PORT', 6379))
        self.globals.setdefault('redis_db', os.environ.get('REDIS_DB', 0))
        self.globals.setdefault('site', os.environ.get('ODYSSEY_SITE', 'local'))


def load_file(config_file):
    """
    Given a python config file returns the mapping of global and worker configuration dictionaries
    """
    config = Config()
    config.load_file(config_file)
    return config
