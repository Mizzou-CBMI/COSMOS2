from abc import abstractproperty, ABCMeta


class Containerizer(object):

    """Represents a specific containerize which has a notion of instantiating an image and executing
        commands from within the resulting created container."""

    __metaclass__ = ABCMeta

    def __init__(self, **arguments):
        assert 'cmd' not in arguments, '"cmd" cannot be a containerizer argument since it is specified at command'
        assert self.required_arguments == set(arguments.keys()), 'You must specify values for {args_list}'.format(
            args_list=', '.join(self.required_arguments),
        )
        self.arguments = arguments

    @classmethod
    def get_containerizer(cls, name):
        """Get a specific containizer implementation class by name.

        Returns:
            (object): The containerizer class
        """
        containerizers = (containerizer for containerizer in cls.__subclasses__() if containerizer.name == name)
        selected_containerizer = next(containerizers, None)
        assert selected_containerizer, 'The containerizer "{containerizer}" is not currently supported. '\
            'We currently support {supported_containerizers}.'.format(  # noqa
            containerizer=name,
            supported_containerizers=', '.join(containerizer.name for containerizer in cls.__subclasses__()),
        )
        return selected_containerizer

    @abstractproperty
    def name(self):
        """The name of the containerizer.

        Returns:
            (str): The containerizer name
        """
        raise NotImplementedError()

    @abstractproperty
    def required_arguments(self):
        """The required arguments to instantiate the container.

        Returns:
            (set): The names of the required arguments
        """
        raise NotImplementedError()

    @abstractproperty
    def containerizer_template(self):
        """The string-template a CLI uses to instantiate and run a command from a container.

        Returns:
            (str): The new-style interpolation-ready string to initialize a container and run a command from it
        """
        raise NotImplementedError()

    def get_containerizer_command(self, cmd):
        """The string a CLI uses to instantiate and run a command from a container.

        Returns:
            (str): The string to initialize a container and run a command from it
        """
        return self.containerizer_template.format(**dict(cmd=cmd, **self.arguments))
