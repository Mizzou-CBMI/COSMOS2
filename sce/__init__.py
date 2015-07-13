import os

from starcluster import logger

import utils

logger.configure_sc_logging()
log = logger.log

settings = {
    'library_path': os.path.dirname(os.path.realpath(__file__))
}


from .plugin_setup import PluginSetup