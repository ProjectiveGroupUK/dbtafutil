import subprocess
from dbtafutil.utils.logger import Logger
from dbtafutil.utils.utils import Globals
import logging
logger = logging.getLogger(Logger.getRootLoggerName())
globals = Globals()

class DbtManifestUtil():
    def runOsCommand(self, commandList: list):
        cmd = commandList
        # try:
        pro = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for line in pro.stdout:
            logger.info(line) 

    def checkDbtVersion(self) -> int:
        cmd = ["dbt", "--version"]
        try:
            self.runOsCommand(commandList=cmd)
            return 0
        except FileNotFoundError:
            logger.error("There seems to be some issue with dbt installation, couldn't run dbt version check command")
            return 1
        except KeyboardInterrupt:
            logger.error("Process terminated by user")
            return 1
        except Exception as e:
            logger.error(f'Error Encountered {e}')   

    def generateManifestFile(self):
        dbt_project_folder = globals.getDBTProjectDir()
        cmd = ["dbt", "ls", "--project-dir", dbt_project_folder]
        try:
            self.runOsCommand(commandList=cmd)
            return 0
        except FileNotFoundError:
            logger.error("There seems to be some issue with dbt installation, couldn't run dbt version check command")
            return 1
        except KeyboardInterrupt:
            logger.error("Process terminated by user")
            return 1
        except Exception as e:
            logger.error(f'Error Encountered {e}')   

if __name__ == "__main__":
    logger = Logger().initialize('dev')
    dbtManifestUtil = DbtManifestUtil()
    ret = dbtManifestUtil.checkDbtVersion()
