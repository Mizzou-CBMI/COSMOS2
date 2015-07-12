import argparse
import os
from importlib import import_module
import re

def get_plugin_instance(starcluster_config, plugin_name):
    m = re.search('(.+)\.(.+)', starcluster_config.plugins[plugin_name]['setup_class'])
    setup_mod = m.group(1)
    setup_class = m.group(2)
    plugin_setup = import_module(setup_mod)
    kwargs = starcluster_config.plugins[plugin_name].copy()
    del kwargs['__name__']
    del kwargs['setup_class']
    plugin_instance = getattr(plugin_setup, setup_class)(**kwargs)
    return plugin_instance