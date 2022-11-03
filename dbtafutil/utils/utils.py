from pathlib import Path
from enum import Enum

class ExitCodes(int, Enum):
    Success = 0
    ModelError = 1
    UnhandledError = 2

class Globals():
    _instance = None
    _dbtProjectDir: Path
    _dagsOutputDir: Path
    
    """
    This is singleton class, hence on instantiation, it returns the same instance
    if already instantiated, otherwise creates a new instance.
    This is to enable reusing setter and getter methods across the project
    """
    def __new__(self):
        if self._instance is None:
            self._instance = super(Globals, self).__new__(self)
            # Put any initialization here.
            self._dbtProjectDir = None
            self._dagsOutputDir = None
        return self._instance    

    def setDBTProjectDir(self, dbtProjectDir:Path) -> None:
        self._dbtProjectDir = dbtProjectDir

    def getDBTProjectDir(self) -> Path:
        return self._dbtProjectDir

    def getDBTManifestFile(self) -> Path:
        return self.getDBTProjectDir().joinpath('target','manifest.json')

    def setDagsOutputDir(self, dagsOutputDir:Path) -> None:
        self._dagsOutputDir = dagsOutputDir

    def getDagsOutputDir(self) -> Path:
        return self._dagsOutputDir

    def getUtilResourcesDir(self) -> Path:
        return self.getDagsOutputDir().joinpath('dbt_resources')

    def getUtilManifestFile(self) -> Path:
        return self.getUtilResourcesDir().joinpath('manifest.json')
