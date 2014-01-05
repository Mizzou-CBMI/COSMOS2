import re
import shutil

from ..db import Base
from sqlalchemy import Column, String, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import relationship

class TaskFileValidationError(Exception): pass
class TaskFileError(Exception): pass

class TaskFile(Base):
    """
    Task File
    """
    __tablename__ = 'taskfile'
    __table_args__ = (UniqueConstraint('output_for_task_id', 'name', name='_uc1'),)

    id = Column(Integer, primary_key=True)
    output_for_task_id = Column(ForeignKey('task.id'))
    task = relationship("Task", backref='taskfiles')
    path = Column(String)
    name = Column(String, nullable=False)
    basename = Column(String)

    def __init__(self, *args, **kwargs):
        super(TaskFile,self).__init__(*args, **kwargs)
        if self.basename is None:
            self.basename = 'out.%s' % self.name

    # def __init__(self, name=None, basename=None, path=None, task=None):
    #     """
    #     :param name: This is the name of the file, and is used as the key for obtaining it.  No Tool an
    #         have multiple TaskFiles with the same name.  Defaults to ``fmt``.
    #     :param fmt: The format of the file.  Defaults to the extension of ``path``.
    #     :param path: The path to the file.  Required.
    #     :param basename: (str) The name to use for the file for auto-generated paths.  You must explicitly
    #         specify the extension of the filename, if you want one i.e. 'myfile.txt' not 'myfile'
    #     """
    #     self.task = task
    #
    #     if path:
    #         if name is None:
    #             groups = re.search('\.([^\.]+)$', self.path).groups()
    #             name = groups[0]
    #
    #     if basename is None:
    #         basename = 'out.'+name
    #
    #     self.name = name
    #     self.basename = basename
    #     self.path = path
    #
    #     if not re.search("^[\w\.]+$", self.name):
    #         raise TaskFileValidationError, 'The taskfile.name can only contain letters, numbers, and periods. Failed name is "{0}"'.format(
    #             self.name)


    def __repr__(self):
        return self.path or 'no_path_yet'
        #return '<TaskFile[%s] %s%s>' % (self.id or '', self.name, ' '+self.path or '')

    def delete(self):
        """
        Deletes this task and all files associated with it
        """
        shutil.rmtree(self.path)
