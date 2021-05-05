import logging

logging.basicConfig(level=logging.NOTSET)
logging.basicConfig(
    level=logging.NOTSET, format='%(name)-12s %(levelname)-8s %(message)s'
)

logger = logging.getLogger('CPG_SM')
