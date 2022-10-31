from dbtafutil.base import BaseTask
from dbtafutil.utils.logger import Logger
import logging
from pathlib import Path
from dbtafutil.utils.utils import Globals


logger = logging.getLogger(Logger.getRootLoggerName())

class GenDagTask(BaseTask):

    globals = Globals()

    def validateArgs(self) -> int:
        logger.debug('Inside validateArgs')
        """
        Validation # 1
        If none of the argument from models/select/tags has been passed, then 
        raise error
        """
        if (self.args.models == None and self.args.select == None and self.args.tags is None):
            logger.error('Either models or tags should be mentioned in arguments for DAG to be generated!')
            return 1


        """
        Validation # 2
        If dags output folder has been specified, check that it is valid path, i.e. 
        either folder exists at that path, or parent folder exists
        """
        if (self.args.dags_output_folder is not None):
            path = Path(self.args.dags_output_folder)
            logger.debug(f'path = {path}')
            if not (path.exists() and path.is_dir()):
                logger.info('Dag Output Folder not found, checking for existence of parent folder')
                parentPath = path.parent.absolute()
                logger.debug(f'parent path = {parentPath}')
                if not parentPath.exists():
                    logger.error(f'Dags output folder specified ({self.args.dags_output_folder}), does not exists!')
                    return 1
                else:
                    self.globals.setDagsOutputDir(dagsOutputDir=parentPath)    
            else:
                self.globals.setDagsOutputDir(dagsOutputDir=path)
        else:
            self.globals.setDagsOutputDir(dagsOutputDir=Path.cwd().joinpath('airflow_dags'))
        
        """
        Validation # 3
        If dbt project folder has been specified, check it exists
        """
        if (self.args.dbt_project_folder is not None):
            path = Path(self.args.dbt_project_folder)
            logger.debug(f'path = {path}')
            if not (path.exists() and path.is_dir()):
                logger.error(f'DBT project folder specified ({self.args.dbt_project_folder}), does not exists!')
                return 1
            else:
                self.globals.setDBTProjectDir(dbtProjectDir=path)    
        else:
            self.globals.setDBTProjectDir(dbtProjectDir=Path.cwd())

        """
        Validation # 4
        Check that dbt_project.yml file exists in dbt project folder
        """
        dbt_project_file = self.globals.getDBTProjectDir().joinpath('dbt_project.yml')
        if not dbt_project_file.exists():
            logger.error(f'Not in a dbt project folder. Missing dbt_project.yml file!')
            return 1

        return 0

    def run(self):
        """Entry point for gendag task."""
        logger.debug("Generate DAG process initiated..")

        logger.debug(f'Argument models: {self.args.models}')
        logger.debug(f'Argument select: {self.args.select}')
        logger.debug(f'Argument tags: {self.args.tags}')
        logger.debug(f'Argument skip_tests: {self.args.skip_tests}')
        logger.debug(f'Argument dags_output_folder: {self.args.dags_output_folder}')
        logger.debug(f'Argument dbt_project_folder: {self.args.dbt_project_folder}')

        if self.validateArgs() == 0:
            logger.debug(f'Validations succeeded')
        else:
            raise Exception('Validations failed, aborting process!')

        logger.info(f'Dags output folder: {self.globals.getDagsOutputDir()}')
        logger.info(f'DBT Project folder: {self.globals.getDBTProjectDir()}')

    def interpret_results(self, results):
        return True
    
