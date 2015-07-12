"""
Sets all the ephemeral drives on the master node to one gluster volume which is mounted
across all worker nodes.

Example config entry:
[plugin glusterfs]
setup_class = gluster.Setup
#stripe of 1 means no stripe
stripe = 1

author: Erik Gafni
"""
import re

from sce import log
from sce.utils.misc import trace


class Gluster(object):
    def __init__(self, setup_instance):
        """
        Collection of methods to help administrate GlusterFS
        :param setup: The gluster ClusterSetup instance
        """
        self.setup = setup_instance
        self.device2export_path = {}

    @trace
    def add_volume(self, node, name, stripe, replicate):
        """
        Creates and starts a volume
        :param node: (node) node to create the volume on
        :param name: (str) name of the volume
        :param stripe: (int) stripe count. <=1 means no stripes.
        :param replicate: (int) replicate count. <=1 means no replicas.
        """
        node.ssh.execute('gluster volume create {name}{replicate}{stripe} transport tcp {bricks}'.format(
            name=name,
            bricks=self.get_brick_uris(),
            stripe=' stripe {0}'.format(stripe) if int(stripe) > 1 else '',
            replicate=' replica {0}'.format(replicate) if int(replicate) > 1 else '')
        )
        node.ssh.execute('gluster volume start {0}'.format(name))

    @trace
    def add_brick(self, node, device, export_path, format=True):
        """
        Formats and mounts a brick
        :param node: (node) The node to format and mount on
        :param device: (str) The path to the device. ie /dev/xvdb1
        :param brick_number: (int) The brick number [0,1,2,3...]
        """
        self.device2export_path[device] = export_path
        if format:
            self.format_device(node, device)
        self.unmount(node, device, skipif=export_path)
        self.mount(node, device, export_path)

    def get_brick_uris(self):
        """
        :returns: (str) "master:brick1.export_path master:brick2.export_path ..."
        # todo include non-master nodes when applicable
        """
        return " ".join(map(lambda p: 'master:{0}'.format(p), self.device2export_path.values()))

    @trace
    def format_device(self, node, device):
        """
        Formats device
        :param node: the node.
        :param device: path to the device.
        """
        r = node.ssh.execute('file -s {0}'.format(device))
        if not re.search("XFS filesystem", r[0]):
            self.unmount(node, device)
            node.ssh.execute('mkfs.xfs {0} -f'.format(device))
        else:
            log.info('{0} already formatted, skipping'.format(device))

    @trace
    def unmount(self, node, device, skipif=None):
        """
        Unmounts a device if it is mounted.
        :param device: (str) path to the device.
        :param skipif: (str) if device is mounted to skipif, do not unmount
        """
        # todo implement skipif
        mm = node.get_mount_map()
        if device in mm.keys() and skipif != mm[device]:
            log.info('Unmounting {0}'.format(device))
            node.ssh.execute('umount {0}'.format(device))

    @trace
    def mount(self, node, device, path, unmount=False):
        """
        Mounts device to path on node
        :param unmount: (bool) if True, attempt unmount first.
        """
        if unmount:
            self.unmount(node, device, skipif=path)
        node.ssh.execute('mkdir -p {0}'.format(path))
        node.ssh.execute('mount {0} {1}'.format(device, path))

    @trace
    def mount_volume(self, node, volume, mountpoint):
        """
        Mounts gluster to a node
        :param node: the node to mount on
        :param volume: the name of the volume
        :param path: the root directory to mount the volume to.  Volume will be mounted to path/volume
        """

        if not node.ssh.path_exists(mountpoint):
            log.info("Creating mount point %s" % mountpoint)
            node.ssh.execute("mkdir -p %s" % mountpoint)

        node.ssh.execute('mount -t glusterfs master:%s %s' % (volume, mountpoint))