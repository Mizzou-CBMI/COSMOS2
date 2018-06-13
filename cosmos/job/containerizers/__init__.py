__author__ = 'arithehun'

import importlib
import os
import pkgutil

pkg_dir = os.path.dirname(os.path.realpath(__file__))
for (module_loader, name, ispkg) in pkgutil.iter_modules([pkg_dir]):
    importlib.import_module('.' + name, __package__)
