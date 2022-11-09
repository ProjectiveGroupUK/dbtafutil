from typing import Any
import logging
from pathlib import Path
from dbtafutil.helper.generate_dag import generateDag
import re

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
        #set variables here
        modelParents:bool=False
        modelChildren:bool=False
        modelParentsDegree=None
        modelChildrenDegree=None

        # Starts with +
        if re.search("^([0-9]+|\+).*", model):
            modelParents = True
            if re.findall('^\d+', model):
                #need to convert to num
                modelParentsDegree=re.findall('^[0-9]+', model)[0]
            else: modelParentsDegree = '999999999'

            model=re.findall("\+.*", model)[0][1:]       
        
        # Ends with +
        if re.search(".*(\+|\+[0-9]*)", model):
            modelChildren = True
            if re.findall('\+\d$', model):
                modelChildrenDegree=re.findall('\d$', model)[0]
            else:
                modelChildrenDegree='999999999'
            
            model=re.findall(".*\+", model)[0][:-1]
        
        modelName = model.strip() ##.lower()
        generateDag(inputType="model", identifierName=modelName, modelParents=modelParents,modelChildren=modelChildren,modelParentsDegree=modelParentsDegree,modelChildrenDegree=modelChildrenDegree   )
        

    return
    
def generateTagsDags (tagsList:list, **kwargs: Any) -> None:
    logger.debug('Inside generateTagsDags')
    logger.debug(f'kwargs = {kwargs}')
    """
    loop through the models and generate dag for individual mode
    """
    for tag in tagsList:
        logger.info(f"Generating dag for tag: {tag}")
        tagName = tag.strip()  ##.lower()
        generateDag(inputType="tag", identifierName=tagName, **kwargs)
    return