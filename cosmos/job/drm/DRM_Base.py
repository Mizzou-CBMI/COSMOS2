from abc import abstractmethod, ABCMeta


class DRM(object, metaclass=ABCMeta):
    "DRM base class"

    name = None
    poll_interval = 1
    required_drm_options = set()
    log = None

    def __init__(self, log):
        self.log = log

    @classmethod
    def validate_drm_options(cls, drm_name, drm_options):
        """Validate that the necessary DRM options have been supplied for the DRM to function.
        :params str drm_name: The name of the DRM to validate
        :params dict drm_options: The DRM options to validate
        """
        drm_cls = cls.get_drm(drm_name)

        if not drm_cls.required_drm_options and not drm_options:
            return

        assert (
            set(drm_options.keys()) >= drm_cls.required_drm_options
        ), "You must specify values for drm_options for the following keys: {args_list}".format(
            args_list=", ".join(drm_cls.required_drm_options),
        )

    @classmethod
    def get_drm(cls, drm_name):
        """Gets a DRM by name.
        :params str drm_name: The name of the DRM to retrieve
        :return DRM: The DRM with a matching name
        """
        return next(drm_cls for drm_cls in cls.__subclasses__() if drm_cls.name == drm_name)

    @classmethod
    def get_drm_names(cls):
        """Get the names of all DRMs.

        :return set: All DRM names
        """
        return {drm.name for drm in cls.__subclasses__()}

    @abstractmethod
    def submit_job(self, task):
        pass

    def submit_jobs(self, tasks):
        for task in tasks:
            self.submit_job(task)

    @abstractmethod
    def filter_is_done(self, tasks):
        pass

    @abstractmethod
    def drm_statuses(self, tasks):
        pass

    @abstractmethod
    def kill(self, task):
        pass

    def kill_tasks(self, tasks):
        for t in tasks:
            self.kill(t)

    def populate_logs(self, task):
        pass

    def release_resources_after_completion(self, task):
        pass
