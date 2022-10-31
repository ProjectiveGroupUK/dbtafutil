from dbtafutil.base import BaseTask
from dbtafutil.utils.logger import Logger
import logging

logger = logging.getLogger(Logger.getRootLoggerName())

class GenDagTask(BaseTask):
    def run(self):
        """Entry point for gendag task."""
        logger.debug("Generate DAG process initiated..")

        logger.debug(f'Argument models: {self.args.models}')
        logger.debug(f'Argument select: {self.args.select}')
        logger.debug(f'Argument tags: {self.args.tags}')

    def interpret_results(self, results):
        return True
