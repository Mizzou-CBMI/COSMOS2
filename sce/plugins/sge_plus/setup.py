"""
Configures SGE to work with h_vmem and other complex values.

This means you can submit jobs, specify how much memory you require, and Grid Engine will
be able to schedule properly.

To schedule jobs, use -l like this: qsub -l mem=10G,num_proc=3

..note:: You currently must run qconf -mc and set h_vmem and num_proc to consumable for this to work.

Example config:
[plugin sge_plus]
setup_class = sge_plus.Setup

Useful commands:
qconf -sc
qconf

"""
import os

from starcluster.logger import log

from ...plugin_setup import PluginSetup


opj = os.path.join


class Setup(PluginSetup):
    def __init__(self, master_slots=1):
        self.data_path = opj(os.path.dirname(__file__), 'data')
        self.master_slots = master_slots

    def run2(self):
        log.info('Running SGE Plus')
        master = self.master

        # update qconf complex list to make h_vmem and num_proc consumable
        log.info('Update complex configuration')
        master.ssh.put(opj(self.data_path, 'qconf_c'), '/tmp/qconf_c')
        master.ssh.execute('qconf -Mc /tmp/qconf_c')
        log.info('Update ms configuration')
        master.ssh.put(opj(self.data_path, 'msconf'), '/tmp/msconf')
        master.ssh.execute('qconf -Msconf /tmp/msconf')

        for node in self.nodes:
            self.on_add_node2(node)

    def update_complex_list(self, node):
        """
        Sets a node's h_vmem and num_proc complex values

        :param node: The node to update
        """
        log.info('Updating complex values for {0}'.format(node))
        memtot = int(node.ssh.execute('free -g|grep Mem:|grep -oE "[0-9]+"|head -1')[0])
        num_proc = self.master_slots if node.is_master() else node.ssh.execute('nproc')[0]
        node.ssh.execute(
            "qconf -rattr exechost complex_values slots={num_proc},num_proc={num_proc},mem={mem}g {node}".format(
                mem=memtot + 2, num_proc=num_proc, node=node.alias)
        )

    def on_add_node2(self, node):
        self.update_complex_list(node)
