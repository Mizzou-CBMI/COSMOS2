class DRM(object):
    "DRM base class"
    name = None

    def __init__(self, jobmanager):
        self.jobmanager = jobmanager

    def submit_job(self, task):
        raise NotImplementedError

    def filter_is_done(self, tasks):
        raise NotImplementedError

    def drm_statuses(self, tasks):
        raise NotImplementedError

    def get_task_return_data(self, task):
        raise NotImplementedError

    def kill(self, task):
        raise NotImplementedError

    def kill_tasks(self, tasks):
        for t in tasks:
            self.kill(t)