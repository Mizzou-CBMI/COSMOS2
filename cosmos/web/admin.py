# from .. import Workflow, Stage, Task, TaskFile
#
# from flask.ext import admin
# from flask.ext.admin.contrib import sqla
#
#
# def add_cosmos_admin(flask_app, session):
#     adm = admin.Admin(flask_app, 'Flask Admin', base_template="admin_layout.html")
#     for m in [Workflow, Stage, Task, TaskFile]:
#         adm.add_view(sqla.ModelView(m, session))
