from cosmos.api import Cosmos, signal_workflow_status_change, WorkflowStatus
from ex1 import run_ex1
import os

def run_ex3(workflow):
    @signal_workflow_status_change.connect
    def sig(ex):
        msg = "%s %s" % (ex, ex.status)
        if ex.status in [WorkflowStatus.successful, WorkflowStatus.failed, WorkflowStatus.killed]:
            text_message(msg)
            ex.log.info('Sent a text message')

    def text_message(message):
        from twilio.rest import TwilioRestClient

        account = "XYZ"
        token = "XYZ"
        client = TwilioRestClient(account, token)

        message = client.messages.create(to="+1231231234", from_="+1231231234", body=message)

    run_ex1(workflow)


if __name__ == '__main__':
    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)))
    cosmos.initdb()

    workflow = cosmos.start('Example_Email', 'analysis_output/ex3', max_attempts=2, restart=True, skip_confirm=True)
    run_ex1(workflow)