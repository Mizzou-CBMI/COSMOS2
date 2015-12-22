import sys


def send(message, hostname=None, sticky=True):
    try:
        from gntp import notifier
        import os

        if hostname == None:
            hostname = os.environ.get('SSH_CLIENT', '').split(' ')[0]
        growl = notifier.GrowlNotifier(
            applicationName="Cosmos",
            notifications=["New Updates", "New Messages"],
            defaultNotifications=["New Messages"],
            hostname=hostname,
        )
        growl.register()

        # Send one message
        growl.notify(
            noteType="New Messages",
            title="Cosmos",
            description=message,
            sticky=sticky,
            priority=1,
        )
    except Exception as e:
        print >> sys.stderr, '*** ERROR sending growl notification to %s: %s' % (hostname, e)