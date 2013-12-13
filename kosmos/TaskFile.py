import re
import shutil

class TaskFileValidationError(Exception): pass


class TaskFileError(Exception): pass



class TaskFile():
    """
    Task File
    """

    class Meta:
        app_label = 'cosmos'
        db_table = 'cosmos_taskfile'

    def __init__(self, name=None, basename=None, path=None):
        """
        :param name: This is the name of the file, and is used as the key for obtaining it.  No Tool an
            have multiple TaskFiles with the same name.  Defaults to ``fmt``.
        :param fmt: The format of the file.  Defaults to the extension of ``path``.
        :param path: The path to the file.  Required.
        :param basename: (str) The name to use for the file for auto-generated paths.  You must explicitly
            specify the extension of the filename, if you want one i.e. 'myfile.txt' not 'myfile'
        """

        if path:
            if name is None:
                groups = re.search('\.([^\.]+)$', self.path).groups()
                name = groups[0]

        if basename is None:
            basename = 'out'

        self.name = name
        self.basename = basename
        self.path = path

        if not re.search("^[\w\.]+$", self.name):
            raise TaskFileValidationError, 'The taskfile.name can only contain letters, numbers, and periods. Failed name is "{0}"'.format(
                self.name)

    def delete(self):
        """
        Deletes this task and all files associated with it
        """
        shutil.rmtree(self.path)
