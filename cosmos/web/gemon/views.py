import getpass
from flask import Blueprint
from flask import render_template


bprint = Blueprint('gemon', __name__, template_folder='templates')


@bprint.route('/')
def home():
    import numpy as np
    from .ge import qstat
    import pandas as pd
    # df_user = qstat()
    df_all = qstat('*')
    if len(df_all) != 0:
        df_user = df_all[df_all['JB_owner'] == getpass.getuser()]


        def summarize(df):
            def f():
                for state, df_ in df.groupby(['state']):
                    yield '%s_jobs' % state, [len(df_)]
                    yield '%s_slots' % state, [df_.slots.astype(int).sum()]

                yield 'sum(io_usage)', ["{:,}".format(int(np.nan_to_num(df.io_usage.astype(float).sum())))]

            return pd.DataFrame(dict(f()))

        df_user_summary = summarize(df_user)
        df_all_summary = summarize(df_all)
    else:
        df_user_summary, df_all_summary = None, None

    return render_template('gemon/home.html', df_user=df_user, df_user_summary=df_user_summary,
                           df_all_summary=df_all_summary)