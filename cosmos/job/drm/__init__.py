__author__ = 'egafni'

import importlib
import os
import pkgutil

# Imports all classes within DRM so that the __subclasses__ attribute is populated
pkg_dir = os.path.dirname(os.path.realpath(__file__))
for (module_loader, name, ispkg) in pkgutil.iter_modules([pkg_dir]):
    importlib.import_module('.' + name, __package__)
