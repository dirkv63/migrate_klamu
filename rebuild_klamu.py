"""
This procedure will rebuild the sqlite klamu database
"""

import logging
import os
from lib import my_env
from lib import klamu_store

cfg = my_env.init_env("klamu_migrate", __file__)
logging.info("Start application")
db = os.getenv('KLAMU_DB')
klamu = klamu_store.DirectConn(db)
klamu.rebuild()
logging.info("sqlite klamu rebuild")
logging.info("End application")
