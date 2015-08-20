# class Command(object):
#     def __init__(self, command, output_files, cpu_req, mem_req=None, persist=None, **kwargs):
#         for k, v in locals():
#             setattr(self, k, v)
#
#     def generate_task(self, stage, parents, default_drm):
#         d = {attr: getattr(self, attr) for attr in ['mem_req', 'time_req', 'cpu_req', 'must_succeed', 'NOOP']}
#         d['drm'] = 'local' if self.drm is not None else default_drm
#
#         inputs = list(self._map_inputs(parents))
#         task = Task(stage=stage, tags=self.tags,
#                     _input_file_assocs=[InputFileAssociation(taskfile=tf, forward=is_forward) for tf, is_forward in
#                                         inputs], parents=parents,
#                     **d)
#         task.skip_profile = self.skip_profile
#
#         input_taskfiles, _ = zip(*inputs) if inputs else ([], None)
#         input_dict = TaskFileDict(input_taskfiles, type='input')  # used to format basenames
#         inputs = list(iter(input_dict))
#
#         # Create output TaskFiles
#         for name, format, path in self.load_sources:
#             TaskFile(name=name, format=format, path=path, task_output_for=task, persist=True)
#
#         for output in self.outputs:
#             name = str_format(output.name, dict(i=inputs, **self.tags))
#             if output.basename is None:
#                 basename = None
#             else:
#                 d = self.tags.copy()
#                 d.update(dict(name=name, format=output.format, i=inputs))
#                 basename = str_format(output.basename, dict(**d))
#             TaskFile(task_output_for=task, persist=self.persist, name=name, format=output.format, basename=basename)
#
#         task.tool = self
#         return task