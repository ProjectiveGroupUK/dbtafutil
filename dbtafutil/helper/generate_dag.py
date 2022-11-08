import os
from typing import Any, Dict, List, Tuple, Optional
import json
import logging
import re
from pathlib import Path
from dbtafutil.utils.jinja_templates import render_jinja_template, DBT_DAG_TEMPLATE

from dbtafutil.utils.logger import Logger
from dbtafutil.utils.utils import Globals

logger = logging.getLogger(Logger.getRootLoggerName())
globals = Globals()


class DbtChecklists:
    """
    A class to support tracking of the DBT tasks checked when parsing the manifest.json file. It enables easier tracking
    of multiple list objects which can be scrapped at the end of each Set iteration, but which all need to be passed
    between several functions during the generation of a single set
    The class maintains 5 lists which hold in turn
    dbt_tasks: the dictionaries defining each task to be rendered in our Airflow Dag
    dbt_tasks_execution: the strings which will define Airflow dependencies in a set (eg "task_1 >> task_2")
    model_checklist: a list of nodes in the manifest file which have been checked and added to dbt_tasks if necessary
    upstream_checklist: a list of upstream nodes in the manifest file which have been checked and added if necessary
    test_checklist: a list of test nodes in the manifest file which have been checked, and added if necessary
    """

    def __init__(self):
        self.dbt_tasks: List[Dict[str, str]] = []
        self.dbt_tasks_execution: List[str] = []
        self.model_checklist: List[str] = []
        self.upstream_checklist: List[str] = []
        self.test_checklist: List[str] = []

    def __contains__(self, item: str) -> bool:
        """
        Method to check if a node name has been checked across both base and upstream nodes. There may be cases to
        maintain both lists, but we need to check both to avoid duplication of tasks in the dbt_tasks list
        :param node_name: Node of the manifest file to be checked against our checklists
        :return: Boolean, true if the node is in either model_checklist or upstream_checklist, otherwise false
        """
        res = item in self.model_checklist + self.upstream_checklist
        return res


def checkNodeInManifest(
    nodeName: str,
    manifestJson: Dict[str, Any],
    tagName: Optional[str] = None,
    modelName: Optional[str] = None,
) -> bool:
    logger.info("Inside checkNodeInManifest")
    logger.info(f"nodeName: {nodeName}")
    logger.info(f"tagName: {tagName}")
    logger.info(f"modelName: {modelName}")
    """Check if a node in the manifest file should be included in the set."""
    # Only include model nodes
    rtn_val:bool = False
    if nodeName.split(".")[0] != "model":
        return rtn_val
    try:
        nodes = manifestJson["nodes"]
    except KeyError as e:
        raise KeyError("Nodes section of manifest file could not be found.") from e
    try:
        node = nodes[nodeName]
    except KeyError as e:
        raise KeyError(
            f"Given node name {nodeName} is not present in manifest json nodes block"
        ) from e
    if modelName:
        #enter model logic
        print ("in check model name part")
        print(nodeName.split(".")[-1])
        try:
            if modelName == nodeName: #modelName == nodeName.split(".")[-1]:
                print("returning true")
                #if modelName return true 
                rtn_val = True
                print(rtn_val)
                return rtn_val

        except KeyError as e:
            raise KeyError(
                f"Given node name {nodeName} is not present in manifest json nodes block"
            ) from e
    if tagName:
        print("in tag part")
        print(tagName)
        print("try")
        try:
            tags = node["tags"]
            print(tags)
            if tags:
                rtn_val= True
        except KeyError as e:
            raise KeyError(
                f"The tags section of the `{nodeName}` node could not be found in manifest file."
            ) from e
   
        # Only include if the tag is specified in the configuration
        return rtn_val


def loadManifest():
    """
    Helper function to load the dbt manifest file.
    Returns: A JSON object containing the dbt manifest content.
    """
    logger.info("Loading DBT manifest file...")

    with open(globals.getUtilManifestFile()) as f:
        fileContent = json.load(f)
        logger.info("Manifest file loaded!")
    return fileContent


def buildTaskDict(nodeName: str, taskType: str) -> Tuple[Dict[str, str], str]:
    if "." not in nodeName:
        raise ValueError(
            f"Given node name `{nodeName}` is not a valid dbt node name. No period found to split on."
        )

    if taskType.lower() == "run":
        node_task_id = nodeName.split(".")[-1]
        task_dict = {
            "task_type": taskType,
            "task_id": node_task_id,
            "dbt_model": node_task_id,
        }

    elif taskType.lower() == "test":
        node_task_id = nodeName.split(".")[-1]
        test_task_id = f"test_{node_task_id}"
        task_dict = {
            "task_type": "test",
            "task_id": test_task_id,
            "dbt_model": node_task_id,
        }

    else:
        raise ValueError(
            "Task type provided is invalid. Task type must be either 'run' or 'test'."
        )

    return task_dict, node_task_id


def getTagRunTasks(
    tagName: str,
    manifestJson: Dict[str, Any],
    checklists: DbtChecklists,
    nodeName: str,
):
    logger.info(f"Building airflow taks for {nodeName}")
    taskDict, nodeTaskId = buildTaskDict(nodeName=nodeName, taskType="run")
    checklists.dbt_tasks.append(taskDict)
    checklists.model_checklist.append(nodeName)

    # Repeat the previous step for nodes upstream of this one
    # We add relevant nodes to our task list, and build Airflow dependency strings
    upstreamNode: str
    # upstreamCount: int = 0
    

    for upstreamNode in set(manifestJson["nodes"][nodeName]["depends_on"]["nodes"]):
        if checkNodeInManifest(
            nodeName=upstreamNode,
            manifestJson=manifestJson,
            tagName=tagName,
        ):
            logger.info(f"Building airflow task for {upstreamNode}")
            upstream_task_id = upstreamNode.split(".")[-1]

            # if item is in list already (as just a base remove it) as it has dependencies.
            if upstream_task_id in checklists.dbt_tasks_execution:
                checklists.dbt_tasks_execution.remove(upstream_task_id)

            # upstreamCount += 1

            # Build the airflow dependency string and add it to the execution list
            checklists.dbt_tasks_execution.append(f"{upstream_task_id} >> {nodeTaskId}")
                                               

    # if upstreamCount == 0:
    #     checklists.dbt_tasks_execution.append(f"{nodeTaskId}")
    if not any(nodeTaskId in nodes for nodes in checklists.dbt_tasks_execution):
        checklists.dbt_tasks_execution.append(f"{nodeTaskId}")
    return checklists


def getTestTasks(checklists: DbtChecklists, nodeName: str, baseNode: str):
    taskDict, taskId = buildTaskDict(nodeName=nodeName, taskType="test")
    testTaskId = taskDict["task_id"]

    # Append the full dictionary to our task list
    checklists.dbt_tasks.append(taskDict)
    # Append the base node to the test checklist
    checklists.test_checklist.append(baseNode)
    # Append the airflow dependency string to the execution list
    checklists.dbt_tasks_execution.append(f"{taskId} >> {testTaskId}")

    return checklists

## fizlar new def required
def getModelRunTasks(
    modelName: str,
    modelParents:bool,
    modelChildren:bool,
    modelParentsDegree:str,
    modelChildrenDegree:str,
    manifestJson: Dict[str, Any],
    checklists: DbtChecklists,
    nodeName: str,
):
    logger.info(f"Building airflow tasks for {nodeName}") 
   
    #taskDict, nodeTaskId = buildTaskDict(nodeName=nodeName, taskType="run")
    #checklists.dbt_tasks.append(taskDict)
    #checklists.model_checklist.append(nodeName)

    # Repeat the previous step for nodes upstream of this one
    # We add relevant nodes to our task list, and build Airflow dependency strings
    # is there a plus ?

    #If both then go to the lowest model and run through that.
    
    # Do we need to get Parents of Model?
    baseModels=set([])
    if modelParents:
        print("We need to parents of Model")
        print("base nodes")
        print(baseModels)   
        print(modelName)
        print(nodeName)
        print(modelParents)
        print(modelChildren)
        print(modelParentsDegree)
        print(modelChildrenDegree)
       
        n=1 
        nodeToCheck:list=nodeName.split()
        while n > 0:
            print("checking parent node!:")
            if not nodeToCheck:
                n=0
            
            for node in nodeToCheck:
                print (nodeToCheck)
                print(node)
                print(node.split(".")[0])
                if node.split(".")[0] == "model":
                    print("model")
                    parentNode= manifestJson["parent_map"][node]
                    print(parentNode)
                    for parent in parentNode:
                        if parent.split(".")[0] == "model":
                            print("this is parent")
                            print(parent)
                            print(parent.split(".")[0])
                            baseModels.add(parent)
                    nodeToCheck=parentNode
                else:
                    print("else")
                    n=0
                    #baseModels.add(prevNode)
            

    print("base nodes")
    print(baseModels)   
    print(modelName)
    print(nodeName)
    print(modelParents)
    print(modelChildren)
    print(modelParentsDegree)
    print(modelChildrenDegree)
    print(type(list(baseModels)))
    

    if not len(baseModels):
        baseModels=nodeName
        model=nodeName
    else:
        baseModels=list(baseModels)
        #add to beginning of list
        baseModels.insert(0,nodeName)
        model=nodeName

    
    for all in baseModels:
        print("modeltocheck")
        print (all)        
        upstreamNode: str
        # upstreamCount: int = 0
        taskDict, nodeTaskId = buildTaskDict(nodeName=model, taskType="run")
        checklists.dbt_tasks.append(taskDict)
        checklists.model_checklist.append(model)
        for upstreamNode in set(manifestJson["nodes"][model]["depends_on"]["nodes"]):
            #Fizlar you are here!
            #
            # 
            if not  checkNodeInManifest(
                nodeName=upstreamNode,
                manifestJson=manifestJson,
            ):
                print("checked node failed")
                pass
            else:
                print ("check passed")


            print("upstream models")
            print(upstreamNode)
            if 1==1:
                logger.info(f"Building airflow task for {upstreamNode}")
                upstream_task_id = upstreamNode.split(".")[-1]

                # if item is in list already (as just a base remove it) as it has dependencies.
                if upstream_task_id in checklists.dbt_tasks_execution:
                    checklists.dbt_tasks_execution.remove(upstream_task_id)

                # upstreamCount += 1

                # Build the airflow dependency string and add it to the execution list
                checklists.dbt_tasks_execution.append(f"{upstream_task_id} >> {nodeTaskId}")

            # if upstreamCount == 0:
             #     checklists.dbt_tasks_execution.append(f"{nodeTaskId}")
            if not any(nodeTaskId in nodes for nodes in checklists.dbt_tasks_execution):
             checklists.dbt_tasks_execution.append(f"{nodeTaskId}")

            if upstreamNode in baseModels:
                print("in if")
                model=upstreamNode
            else:
                print("pass")
                pass

    
    return checklists












    
    exit(0)
    upstreamNode: str
    # upstreamCount: int = 0
    for upstreamNode in set(manifestJson["nodes"][nodeName]["depends_on"]["nodes"]):
        #Fizlar you are here!
        
        exit(0)

        if checkNodeInManifest(
            nodeName=upstreamNode,
            manifestJson=manifestJson,
            tagName=tagName,
        ):
            logger.info(f"Building airflow task for {upstreamNode}")
            upstream_task_id = upstreamNode.split(".")[-1]

            # if item is in list already (as just a base remove it) as it has dependencies.
            if upstream_task_id in checklists.dbt_tasks_execution:
                checklists.dbt_tasks_execution.remove(upstream_task_id)

            # upstreamCount += 1

            # Build the airflow dependency string and add it to the execution list
            checklists.dbt_tasks_execution.append(f"{upstream_task_id} >> {nodeTaskId}")

    # if upstreamCount == 0:
    #     checklists.dbt_tasks_execution.append(f"{nodeTaskId}")
    if not any(nodeTaskId in nodes for nodes in checklists.dbt_tasks_execution):
        checklists.dbt_tasks_execution.append(f"{nodeTaskId}")
    return checklists

def generateDag(inputType: str, identifierName: str, **kwargs: Any):
    logger.info("Inside genrateModelsDags")
    logger.info(f"inputType = {inputType}")
    logger.info(f"identifierName = {identifierName}")
    logger.info(f"kwargs = {kwargs}")
    

    # Type checking
    if inputType not in ("model", "tag"):
        raise TypeError("Incorrect value for input type")

    checklists = DbtChecklists()

    outputDir = globals.getDagsOutputDir()
    logger.debug(f"outputDir = {outputDir}")

    # dbt_tasks, dbt_tasks_execution = getTasks(modelName=identifierName)

    logger.info(f"Checking manifest file nodes for models..")
    manifestJson = loadManifest()
    
    for nodeName in manifestJson["nodes"].keys():
        nodeType = nodeName.split(".")[0]

        if inputType == "tag":
            if checkNodeInManifest(
                manifestJson=manifestJson,
                tagName=identifierName,
                nodeName=nodeName,):
                    checklists = getTagRunTasks(
                    tagName=identifierName,
                    manifestJson=manifestJson,
                    checklists=checklists,
                    nodeName=nodeName,
                    )

            elif (nodeType == "test") and (not kwargs["skip_tests"]):
                for upstreamNode in set(
                    manifestJson["nodes"][nodeName]["depends_on"]["nodes"]
                ):
                    if checkNodeInManifest(
                        nodeName=upstreamNode,
                        manifestJson=manifestJson,
                        tagName=identifierName,
                    ):
                        checklists = getTestTasks(
                            checklists=checklists,
                            nodeName=upstreamNode,
                            baseNode=nodeName,
                        )
            else: 
                pass
        else:
            # input type is model
            #print("in model part")
            #print(nodeType)
            if (nodeType == "model") and (nodeName.split(".")[-1] == identifierName):
                print("node type is model")
                print(identifierName)
                print(nodeName)
                print("CheckNodeInManifest Passed")
                checklists = getModelRunTasks(
                modelName=identifierName,
                modelParents=kwargs["modelParents"],
                modelChildren=kwargs["modelChildren"],
                modelParentsDegree=kwargs["modelParentsDegree"],
                modelChildrenDegree=kwargs["modelChildrenDegree"],
                manifestJson=manifestJson,
                checklists=checklists,
                nodeName=nodeName,
                )
                    
                #if nodeName == 'model.kiwi_dbt.promotion_rep':
                    #exit(0)
            else:
                pass



    print("fizlar dbt tasks")
    dbt_tasks = []
    [dbt_tasks.append(x) for x in checklists.dbt_tasks if x not in dbt_tasks]

    dbt_tasks_execution = []
    [
        dbt_tasks_execution.append(x)
        for x in checklists.dbt_tasks_execution
        if x not in dbt_tasks_execution
    ]

    if len(dbt_tasks) > 0:
        outputFilePath = Path(
            os.path.join(outputDir, f"{inputType}_{identifierName}.py")
        )

        tasksConfig = {
            **{
                "dag_name": identifierName,
                "schedule_interval": "None",
                "catchup": False,
                "default_args": {"start_date": "datetime(2022, 1, 1)"},
            },
            **{"tasks": dbt_tasks, "tasks_execution": dbt_tasks_execution},
        }

        rendered_jinja_template = render_jinja_template(DBT_DAG_TEMPLATE, **tasksConfig)
        with open(outputFilePath, "w") as f:
            f.write(rendered_jinja_template)
    else:
        logger.error(
            f"No dbt models/tests identified for {inputType}::{identifierName}"
        )

    return
