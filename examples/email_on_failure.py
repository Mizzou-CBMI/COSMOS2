from kosmos import Execution, Kosmos, Recipe, Input, ExecutionStatus, signal_execution_status_change
import tools

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


if __name__ == '__main__':
    r = Recipe()
    inp = r.add_source([Input('blah', '/tmp', {'test': 'tag'})])
    fail = r.add_stage(tools.Fail, inp)

    kosmos_app = Kosmos('sqlite:///email_on_failure.db', default_drm='local')
    kosmos_app.initdb()
    ex = Execution.start(kosmos_app=kosmos_app, output_dir='out/email_on_failure', name='email_on_failure', restart=True, max_attempts=2)
    ex.run(r, lambda x: x.execution.output_dir)