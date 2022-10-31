from pathlib import Path
from enum import Enum

class ExitCodes(int, Enum):
    Success = 0
    ModelError = 1
    UnhandledError = 2

class Globals():
    _dbtProjectDir: Path
    _dagsOutputDir: Path

    def __init__(self):
        self._dbtProjectDir = None
        self._dagsOutputDir = None

    def setDBTProjectDir(self, dbtProjectDir:Path) -> None:
        self._dbtProjectDir = dbtProjectDir

    def getDBTProjectDir(self) -> Path:
        return self._dbtProjectDir

    def setDagsOutputDir(self, dagsOutputDir:Path) -> None:
        self._dagsOutputDir = dagsOutputDir

    def getDagsOutputDir(self) -> Path:
        return self._dagsOutputDir

