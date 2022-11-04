from typing import Any
import logging
from pathlib import Path
from dbtafutil.helper.generate_dag import generateDag

from dbtafutil.utils.logger import Logger
from dbtafutil.utils.utils import Globals

logger = logging.getLogger(Logger.getRootLoggerName())
globals = Globals()

def genrateModelsDags (modelsList:list, **kwargs: Any) -> None:
    logger.debug('Inside genrateModelsDags')
    logger.debug(f'kwargs = {kwargs}')
    """
    loop through the models and generate dag for individual mode
    """
    for model in modelsList:
        logger.info(f"Generating dag for model: {model}")
        modelName = model.strip().lower()
        generateDag(inputType="model", identifierName=modelName, **kwargs)

    return
    
def generateTagsDags (tagsList:list, **kwargs: Any) -> None:
    logger.debug('Inside generateTagsDags')
    logger.debug(f'kwargs = {kwargs}')
    """
    loop through the models and generate dag for individual mode
    """
    for tag in tagsList:
        logger.info(f"Generating dag for tag: {tag}")
        tagName = tag.strip().lower()
        generateDag(inputType="tag", identifierName=tagName, **kwargs)
    return