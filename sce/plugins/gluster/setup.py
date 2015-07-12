__author__ = 'Erik Gafni'

"""
Sets all the ephemeral drives on the master node to one gluster volume which is mounted
across all worker nodes.

Example config entry:

[plugin glusterfs]
setup_class = gluster.Setup
stripe = 2

author: Erik Gafni
"""
import os

from starcluster import threadpool

from sce import log
from .gluster import Gluster
from sce import PluginSetup, utils


class Setup(PluginSetup):
    _pool = None

    def __init__(self, stripe=0, replicate=0):
        self.gluster = Gluster(self)
        self.stripe = stripe
        self.replicate = replicate

    @property
    def pool(self):
        if self._pool is None:
            self._pool = threadpool.get_thread_pool(4, disable_threads=False)
        return self._pool


    def run2(self):
        master = self.master
        utils.apt_update(master)
        log.info('Installing gluster packages.')
        master.ssh.execute('add-apt-repository ppa:semiosis/ubuntu-glusterfs-3.4 -y')
        utils.apt_update(master, checkfirst=False)
        master.apt_install('openssh-server glusterfs-server glusterfs-client xfsprogs attr')

        log.info('Partitioning and formatting ephemeral drives.')

        # get ephemeral drives
        ebs_devices = map(lambda d: d.replace('sd', 'xvd'), master.block_device_mapping.keys())
        all_devices = master.get_device_map().keys()
        ephemeral_devices = filter(lambda d: d not in ebs_devices, all_devices)

        log.info("Gathering devices for bricks: {0}".format(', '.join(ephemeral_devices)))
        for brick_number, device in enumerate(ephemeral_devices):
            export_path = os.path.join('/exports', 'brick{0}'.format(brick_number))
            self.pool.simple_job(self.gluster.add_brick, (master, device, export_path), jobid=device)
        self.pool.wait(len(self.gluster.device2export_path))

        log.info('Creating and starting gluster volume gv0.')
        # this is necessary if the server restarts
        #        setfattr -x trusted.glusterfs.volume-id $brick_path
        #        setfattr -x trusted.gfid $brick_path
        #        rm -rf $brick_path/.glusterfs

        self.gluster.add_volume(master, 'gv0', self.stripe, self.replicate)

        #mount gluster on all nodes
        for node in self.nodes:
            self.on_add_node2(node)

        master.ssh.execute('mkdir -p /gluster/gv0/master_scratch && ln -s /gluster/gv0/master_scratch /scratch')

    def on_add_node2(self, node):
        log.info('Installing glusterfs-client')
        node.ssh.execute('add-apt-repository ppa:semiosis/ubuntu-glusterfs-3.4 -y')
        utils.apt_update(node, checkfirst=False)
        node.apt_install('glusterfs-client -y')
        self.gluster.mount_volume(node, 'gv0', '/gluster/gv0')

