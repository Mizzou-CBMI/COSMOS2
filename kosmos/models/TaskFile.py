import re
import shutil

class TaskFileValidationError(Exception): pass


class TaskFileError(Exception): pass



class TaskFile():
    """
    Task File
    """

    def __init__(self, name=None, basename=None, path=None, task=None):
        """
        :param name: This is the name of the file, and is used as the key for obtaining it.  No Tool an
            have multiple TaskFiles with the same name.  Defaults to ``fmt``.
        :param fmt: The format of the file.  Defaults to the extension of ``path``.
        :param path: The path to the file.  Required.
        :param basename: (str) The name to use for the file for auto-generated paths.  You must explicitly
            specify the extension of the filename, if you want one i.e. 'myfile.txt' not 'myfile'
        """
        self.task = task

        if path:
            if name is None:
                groups = re.search('\.([^\.]+)$', self.path).groups()
                name = groups[0]

        if basename is None:
            basename = 'out.'+name

        self.name = name
        self.basename = basename
        self._path = path

        if not re.search("^[\w\.]+$", self.name):
            raise TaskFileValidationError, 'The taskfile.name can only contain letters, numbers, and periods. Failed name is "{0}"'.format(
                self.name)

    @property
    def path(self):
        return self._path

    def __repr__(self):
        return self._path

    def delete(self):
        """
        Deletes this task and all files associated with it
        """
        shutil.rmtree(self.path)
