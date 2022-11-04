import logging
from pathlib import Path
from dbtafutil.helper.generate_dag import generateDag

from dbtafutil.utils.logger import Logger
from dbtafutil.utils.utils import Globals

logger = logging.getLogger(Logger.getRootLoggerName())
globals = Globals()

def genrateModelsDags (modelsList:list) -> None:
    logger.debug('Inside genrateModelsDags')
    """
    loop through the models and generate dag for individual mode
    """
    for model in modelsList:
        modelName = model.strip().lower()
        generateDag(inputType="model", identifierName=modelName)

    return
    
def generateTagsDags (tagsList:list) -> None:
    logger.debug('Inside generateTagsDags')
    """
    loop through the models and generate dag for individual mode
    """
    for tag in tagsList:
        tagName = tag.strip().lower()
        generateDag(inputType="tag", identifierName=tagName)
    return