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
        """
        This function parses configuration and provides the address that should be used to contact
        the key/value store.
        :return: an address (IP, hostname) of an host
        """
        return self.configuration.get('Rome', 'host')

    def port(self):
        """
        This function parses configuration and provides the port that should be used to contact the
        key/value store.
        :return: a port as an integer
        """
        return self.configuration.getint('Rome', 'port')

    def backend(self):
        """
        This function parses configuration and provides the name of the key/value backend that
        should be used.
        :return: a backend name (such as redis)
        """
        return self.configuration.get('Rome', 'backend')

    def redis_cluster_enabled(self):
        """
        This function parses configuration and indicates if the cluster mode should be enabled.
        :return: a boolean which is true if the cluster mode is enabled
        """
        return self.configuration.getboolean('Cluster', 'redis_cluster_enabled')

    def cluster_nodes(self):
        """
        This function parses configuration and indicates if the cluster mode should be enabled.
        (Applicable only if the cluster mode is enabled)
        :return: a list of address of nodes
        """
        return self.configuration.get('Cluster', 'nodes').split(",")

    def database_caching(self):
        """
        This function parses configuration and tells if database caching is enabled
        :return: a boolean that is True when database caching is active
        """
        try:
            return self.configuration.getboolean('Rome', 'database_caching')
        except ConfigParser.NoOptionError:
            return False


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
