# import subprocess as sp
# import os
#
# def main(new_version):
#     def run(cmd):
#         os.system(cmd)
#
#
#     run('git flow release start %s' % new_version)
#     run('git flow release finish %s' % new_version)
#
#     with open('cosmos/VERSION','w') as fh:
#         fh.write(new_version)
#
#     run('python setup.py sdist upload')
#     run('')
#
# if __name__ == '__main__':
#     import argparse
#     p = argparse.ArgumentParser()
#     p.add_argument('new_version')
#     args = p.parse_args()
#
#     main(**vars(args))