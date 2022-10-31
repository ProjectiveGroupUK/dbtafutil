from dbtafutil.base import BaseTask
from dbtafutil.utils.logger import Logger
import logging

logger = logging.getLogger(Logger.getRootLoggerName())

class GenDagTask(BaseTask):
    def run(self):
        """Entry point for gendag task."""
        print('hello')
        logger.debug("Generate DAG process initiated..")

    def interpret_results(self, results):
        return True
