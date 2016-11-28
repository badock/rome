from __future__ import print_function
import os
import ConfigParser


class Configuration(object):

    def __init__(self, config_path="/etc/rome/rome.conf"):
        self.configuration_path = config_path
        self.configuration = None
        self.load()

    def load(self):
        self.configuration = ConfigParser.ConfigParser()
        self.configuration.read(self.configuration_path)

    def host(self):
        return self.configuration.get('Rome', 'host')

    def database_caching(self):
        try:
            return self.configuration.getboolean('Rome', 'database_caching')
        except ConfigParser.NoOptionError:
            return False

    def port(self):
        return self.configuration.getint('Rome', 'port')

    def backend(self):
        return self.configuration.get('Rome', 'backend')

    def redis_cluster_enabled(self):
        return self.configuration.getboolean('Cluster', 'redis_cluster_enabled')

    def cluster_nodes(self):
        return self.configuration.get('Cluster', 'nodes').split(",")


CONFIGURATION = None


def build_config():
    search_path = [os.path.join(os.getcwd(), 'rome.conf'),
                   os.path.join(os.path.expanduser('~'), '.rome.conf'),
                   '/etc/rome/rome.conf']
    config_path = None
    for path in search_path:
        if os.path.exists(path):
            config_path = path
            break

    return Configuration(config_path)


def get_config():
    global CONFIGURATION
    if CONFIGURATION is None:
        CONFIGURATION = build_config()
    return CONFIGURATION


if __name__ == '__main__':
    CONFIGURATION = Configuration()
    print(CONFIGURATION.host())
    print(CONFIGURATION.port())
    print(CONFIGURATION.backend())
    print(CONFIGURATION.redis_cluster_enabled())
    print(CONFIGURATION.cluster_nodes())
