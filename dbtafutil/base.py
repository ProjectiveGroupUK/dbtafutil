from abc import ABCMeta, abstractmethod
from typing import Type, Union, Dict, Any, Optional
from dataclasses import dataclass


class NoneConfig:
    @classmethod
    def from_args(cls, args):
        return None


@dataclass
class Project:
    project_name: str


class BaseTask(metaclass=ABCMeta):
    # ConfigType: Union[Type[NoneConfig], Type[Project]] = NoneConfig
    ConfigType: Union[Type[NoneConfig], Type[Project]] = NoneConfig

    def __init__(self, args, config):
        self.args = args
        self.args.single_threaded = False
        self.config = config


    @classmethod
    def from_args(cls, args):
        try:
            # This is usually RuntimeConfig but will be UnsetProfileConfig
            # for the clean or deps tasks
            config = cls.ConfigType.from_args(args)
        except Exception as exc:
            raise Exception("Could not run dbtafutil") from exc
        return cls(args, config)

    @abstractmethod
    def run(self):
        raise Exception("Not Implemented")

    def interpret_results(self, results):
        return True


