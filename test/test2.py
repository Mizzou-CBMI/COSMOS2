import os
from tools import ECHO, CAT
from kosmos import rel, Recipe, signal_execution_status_change, ExecutionStatus

opj = os.path.join


def run(ex, **kwargs):
    r = Recipe()
    echo = r.add_source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'}), ECHO(tags={'word': 'world2'})])
    cat = r.add_stage(CAT, parents=[echo], rel=rel.One2many([('n', [1, 2])]))

    ex.run(r)


@signal_execution_status_change.connect
def s(ex):
    msg = "%s %s" % (ex, ex.status)
    if ex.status in [ExecutionStatus.successful, ExecutionStatus.failed, ExecutionStatus.killed]:
        ex.log.info('Sending text message...')
        text_message(msg)
        ex.log.info('Text message sent')

def text_message(message):
    import smtplib
    from kosmos import app_store_path

    with open(opj(app_store_path, 'email.conf'), 'r') as fh:
        username, password, phonenumber = fh.read().strip().split("\n")

    vtext = "%s@vtext.com" % phonenumber

    msg = "From: %s\nTo: %s\nSubject: \n%s" % (username, vtext, message)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(username, password)
    server.sendmail(username, vtext, msg)
    server.quit()

if __name__ == '__main__':
    from kosmos import default_argparser
    import ipdb

    with ipdb.launch_ipdb_on_exception():
        ex, kwargs = default_argparser()
        run(ex, **kwargs)