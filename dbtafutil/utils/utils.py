from pathlib import Path
from enum import Enum

class ExitCodes(int, Enum):
    Success = 0
    ModelError = 1
    UnhandledError = 2

class Globals():
    _projectDir: Path
    _configDir: Path
    _configFilePath: Path

    def __init__(self):
        self._projectDir = None
        self._configDir = None

    def setProjectDir(self, projectDir:Path) -> None:
        self._projectDir = projectDir

    def getProjectDir(self) -> Path:
        return self._projectDir

    def setConfigDir(self, configDir:Path) -> None:
        self._configDir = configDir

    def getConfigDir(self) -> Path:
        return self._configDir

    def setConfigFilePath(self, configFilePath:Path) -> None:
        self._configFilePath = configFilePath

    def getConfigFilePath(self) -> Path:
        return self._configFilePath

