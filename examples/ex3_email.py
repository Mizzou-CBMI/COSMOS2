from cosmos import Execution, Cosmos, Recipe, Input, ExecutionStatus, signal_execution_status_change
import tools


def ex3_main(execution):
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

    r = Recipe()
    inp = r.add_source([Input('/tmp', 'tmp', 'dir', {'a': 'tag'})])
    fail = r.add_stage(tools.Fail, inp)

    execution.run(r)


if __name__ == '__main__':

    cosmos_app = Cosmos('sqlite:///sqlite.db', default_drm='local')
    cosmos_app.initdb()
    ex = Execution.start(cosmos_app=cosmos_app, output_dir='out/email_on_failure', name='email_on_failure', restart=True, max_attempts=2)
    ex3_main(ex)