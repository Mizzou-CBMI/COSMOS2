from starcluster.clustersetup import ClusterSetup
from .utils.misc import catchall
from sce import log

class PluginSetup(ClusterSetup):
    """
    Creates an SCE plugin.  Important methods to override are:

    :method:`run2`
    :method:`on_add_node2`
    :method:`on_remove_node2
    :method:`on_shutdown2`
    :method:`on_restart2`

    Which have much simpler signatures that the base StarCluster plugins.

    Instances will be instantiated with attributes such as nodes, master, ec2, etc. (see :method:`__configure__`).
    """

    def __configure__(self, nodes, master, user, user_shell, volumes):
        """
        """
        self.nodes = nodes
        self.master = master
        self.ec2 = self.master.ec2
        self.user = user
        self.user_shell = user_shell
        self.volumes = volumes
        self.__postconfig__()

    def __postconfig__(self):
        pass

    def run(self, nodes, master, user, user_shell, volumes):
        self.__configure__(nodes, master, user, user_shell, volumes)
        self.master = master

        log.info("Executing plugin {0}".format(self.__module__))
        catchall(self.run2)

    def run2(self):
        pass

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        self.__configure__(nodes, master, user, user_shell, volumes)
        return catchall(self.on_add_node2, node)

    def on_add_node2(self, node):
        pass

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        self.__configure__(nodes, master, user, user_shell, volumes)
        return catchall(self.on_remove_node2, node)

    def on_remove_node2(self, node):
        pass

    def on_restart(self, nodes, master, user, user_shell, volumes):
        self.__configure__(nodes, master, user, user_shell, volumes)
        self.on_restart2()

    def on_restart2(self):
        pass

    def on_shutdown(self, nodes, master, user, user_shell, volumes):
        self.__configure__(nodes, master, user, user_shell, volumes)
        self.on_shutdown2()

    def on_shutdown2(self):
        pass