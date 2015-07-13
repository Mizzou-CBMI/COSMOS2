from sce.utils import sed_delete

__author__ = 'erik'


def add2file(file,line,sudo=False,node=None,del_if_exists=True):
    """
    Adds line to file if it does not exist
    if node is none, just return the command

    :param del_if_exists: if False, skip check for existing line and append no matter what.
    """
    if del_if_exists:
        sed_delete(file,regex=line,node=node,sudo=sudo)

    sudo = 'sudo ' if sudo else ''
    cmd = "{sudo}bash -c \"echo '{1}' >> {0}\"".format(file,line,sudo=sudo)
    log.info('exec({0}): {1}'.format(node if node else '<local>',cmd))
    if node:
        return node.ssh.execute(cmd)
    else:
        os.system(cmd)


def sed_delete(file,regex,node=None,sudo=False):
    sudo = 'sudo ' if sudo else ''
    cmd = "{sudo}sed '/{0}/d' {1}".format(re.escape(regex),file,sudo=sudo)
    log.info('exec({0}): {1}'.format(node if node else '<local>',cmd))
    if node:
        return node.ssh.execute(cmd)
    else:
        os.system(cmd)


def apt_update(node,checkfirst=True):
    """
    apt-update if it hasn't been done in the past few days
    :param checkfirst: if False, skip checking if already apt-updated in the past few days
    """
    s = node.ssh.execute('stat -c %y /var/lib/apt/periodic/update-success-stamp')[0]
    dt = datetime.now() - datetime.strptime(s[:10],"%Y-%m-%d")
    if not checkfirst or dt.days > 2:
        log.info("Running apt-get update -y on {0}".format(node))
        node.ssh.execute('apt-get update -y')