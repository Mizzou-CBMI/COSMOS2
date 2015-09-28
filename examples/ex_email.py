from cosmos.api import Cosmos, signal_execution_status_change, ExecutionStatus
from ex1 import run_ex1
import os
from cosmos.util.helpers import mkdir

def run_ex3(execution):
    @signal_execution_status_change.connect
    def sig(ex):
        msg = "%s %s" % (ex, ex.status)
        if ex.status in [ExecutionStatus.successful, ExecutionStatus.failed, ExecutionStatus.killed]:
            text_message(msg)
            ex.log.info('Sent a text message')

    def text_message(message):
        from twilio.rest import TwilioRestClient

        account = "XYZ"
        token = "XYZ"
        client = TwilioRestClient(account, token)

        message = client.messages.create(to="+1231231234", from_="+1231231234", body=message)

    run_ex1(execution)


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()
    mkdir('out_dir')

    execution = cosmos.start('Example1', 'out_dir/ex1', max_attempts=2, restart=True, skip_confirm=True)
    run_ex1(execution)