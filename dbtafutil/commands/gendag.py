import logging
import shutil
from pathlib import Path

from dbtafutil.base import BaseTask
from dbtafutil.utils.logger import Logger
from dbtafutil.utils.utils import Globals
from dbtafutil.helper.dbt_manifest_util import DbtManifestUtil
from dbtafutil.helper.generate_dags import genrateModelsDags, generateTagsDags

logger = logging.getLogger(Logger.getRootLoggerName())
globals = Globals()
dbtManifestUtil = DbtManifestUtil()

class GenDagTask(BaseTask):

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
                    globals.setDagsOutputDir(dagsOutputDir=parentPath)    
            else:
                globals.setDagsOutputDir(dagsOutputDir=path)
        else:
            globals.setDagsOutputDir(dagsOutputDir=Path.cwd().joinpath('airflow_dags'))
        
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
                globals.setDBTProjectDir(dbtProjectDir=path)    
        else:
            globals.setDBTProjectDir(dbtProjectDir=Path.cwd())

        """
        Validation # 4
        Check that dbt_project.yml file exists in dbt project folder
        """
        dbt_project_file = globals.getDBTProjectDir().joinpath('dbt_project.yml')
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

        logger.info(f'Dags output folder: {globals.getDagsOutputDir()}')
        logger.info(f'DBT Project folder: {globals.getDBTProjectDir()}')

        # if both models as well as select have been passed as arguments, then club them together
        modelsList = []
        if self.args.models is not None:
            modelsList = self.args.models

        if self.args.select is not None:
            for modelName in self.args.select: modelsList.append(modelName)

        # Now also do a dedup of the list, in case same model names have been passed multiple times
        modelsList = list(set(modelsList))
        logger.debug(f'modelsList: {modelsList}')

        # Also dedup tag list if any passed
        tagsList = []
        if self.args.tags is not None:
            tagsList = list(set(self.args.tags))

        logger.debug(f'tagsList: {tagsList}')

        """
        Now that we have passed the validations etc., run the task to generate project wide manifest file
        But before running any task, first check that dbt executable works
        """
        logger.info('Checking dbt version:')
        ret = dbtManifestUtil.checkDbtVersion()
        if ret != 0:
            raise Exception('Problem with running dbt executable')
        
        # Create dag folder if it doesn't already exists
        fldr = globals.getDagsOutputDir()
        if not fldr.exists():
            Path.mkdir(fldr)
            logger.info(f'Created Dags output folder: {fldr} ')
        
        # also create dbt_resources folder inside dag folder, where manifest file needs to be copied over
        fldr = globals.getUtilResourcesDir().joinpath('airflow_dags')
        if not fldr.exists():
            Path.mkdir(fldr)

        logger.info('Now generating dbt project Manifest file:')
        ret = dbtManifestUtil.generateManifestFile()
        if ret != 0:
            raise Exception('Problem with generating dbt project Manifest')        

        try:
            shutil.copyfile(globals.getDBTManifestFile(),globals.getUtilManifestFile())
            logger.info('Copied dbt manifest file to utility resources folder')
        except Exception as e:
            raise Exception(e)

        # if models have been passed as argument, call it's function to generate dags for those models
        if len(modelsList) > 0:
            logger.info('Starting to generate DAGs based on models')
            genrateModelsDags(modelsList=modelsList)
            logger.info('Completed generating DAGs for models')

        # if tags have been passed as argument, call it's function to generate dags for those tags
        if len(tagsList) > 0:
            logger.info('Starting to generate DAGs based on tags')
            generateTagsDags(tagsList=tagsList, manifestPath=globals.getUtilManifestFile(), dagsFolder=fldr)
            logger.info('Completed generating DAGs for tags')

    def interpret_results(self, results):
        return True
    
