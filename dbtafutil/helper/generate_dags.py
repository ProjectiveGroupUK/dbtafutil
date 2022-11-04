import logging
from pathlib import Path
import json

from dbtafutil.utils.logger import Logger
from dbtafutil.utils.utils import Globals
from dbtafutil.helper.set_generator import generate_single_set
from dbtafutil.jinja_templates import DBT_SET_TEMPLATE


logger = logging.getLogger(Logger.getRootLoggerName())
globals = Globals()




### this works
def loadJsonFile (filePath:str) -> dict:
    #open Manifest file
    f = open(filePath)
    return json.load(f)

def genrateModelsDags (modelsList:list) -> None:
    logger.debug('Inside genrateModelsDags')
    return
def generateTagsDags (tagsList:list,manifestPath:str, dagsFolder:str) -> None:
    #TODO need to do one per tag inlist!!
    for tag in tagsList:
        logger.info('Fizlar you are here')
        data=loadJsonFile(manifestPath)
        identifiedTagModels=[]
        for i in data['nodes']:
            # Check if its a model
            if data['nodes'][i]['resource_type'] == 'model':
                model_tags=data['nodes'][i]['tags']
                # Are tags in our list
                if (any(x in tagsList for x in model_tags)):
                    identifiedTagModels.append(data['nodes'][i]['alias'])
                
        # If our config file is valid, generate the set for this single product set
        generate_single_set(
            manifest_json=data,
            output_dir=dagsFolder,
            jinja_template=DBT_SET_TEMPLATE,
            dbt_tag=tag
            )                  

    logger.debug('Inside generateTagsDags')
    return