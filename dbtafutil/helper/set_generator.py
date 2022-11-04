import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple
#TODO need to add Jinja to requirements install
from jinja2 import Template

from dbtafutil.jinja_templates import render_jinja_template
from dbtafutil.helper import manifest_utils
from dbtafutil.helper.checklist_class import DbtChecklists

#from grid_actions.set_generator.utils.database_connection import DatabaseConnection

logger = logging.getLogger(__name__)


def build_dir(dir_name: str):
    """
    Function to build an empty directory, removing it if it already exists
    :param dir_name: Path to the directory we wish to build
    :return:
    """
    # if directory already exists then remove it and its content
    logger.info(f"Building empty directory {dir_name}")
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)

    # now create an empty directory
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def get_model_tasks(
    dbt_tag: str,
    manifest_json: Dict[str, Any],
    checklists: DbtChecklists,
    node_name: str,
):
    logger.info(f"Building airflow details for {node_name}")
    task_dict, node_task_id = manifest_utils.build_task_dict(
        node_name=node_name, task_type="run"
    )
    checklists.dbt_tasks.append(task_dict)
    checklists.model_checklist.append(node_name)

 
    # Repeat the previous step for nodes upstream of this one
    # We add relevant nodes to our task list, and build Airflow dependency strings
    upstream_node: str
    for upstream_node in set(manifest_json["nodes"][node_name]["depends_on"]["nodes"]):
        
        if manifest_utils.check_node_in_set_tasks(
            node_name=upstream_node,
            manifest_json=manifest_json,
            dbt_tag=dbt_tag,
        ):
            logger.info(f"Building airflow string for {upstream_node}")
            upstream_task_id = upstream_node.split(".")[-1]
            
            #if item is in list already (as just a base remove it) as it has dependencies.
            if upstream_task_id in checklists.dbt_tasks_execution:
                checklists.dbt_tasks_execution.remove(upstream_task_id)

            # Build the airflow dependency string and add it to the execution list
            checklists.dbt_tasks_execution.append(
                f"{upstream_task_id} >> {node_task_id}"
            )
    
    #Add node to task execution if doesn't exists 
    if not any(node_task_id in nodes for nodes in checklists.dbt_tasks_execution):
        checklists.dbt_tasks_execution.append(
            f"{node_task_id}"
        )

    return checklists


def get_model_tests(checklists: DbtChecklists, node_name: str, base_node: str):
    task_dict, task_id = manifest_utils.build_task_dict(
        node_name=node_name, task_type="test"
    )
    test_task_id = task_dict["task_id"]

    # Append the full dictionary to our task list
    checklists.dbt_tasks.append(task_dict)
    # Append the base node to the test checklist
    checklists.test_checklist.append(base_node)
    # Append the airflow dependency string to the execution list
    checklists.dbt_tasks_execution.append(f"{task_id} >> {test_task_id}")

    return checklists


def get_set_tasks(
    manifest_json: Dict[str, Any], dbt_tag: str
) -> Tuple[List[Dict[str, str]], List[str]]:
    # Initiate the lists we need - a list of task dictionaries, a list of airflow dependency strings
    # And 3 tracking lists to cover what nodes we have checked already
    # We will maintain them in a dictionary to avoid repeatedly passing 5 different lists to functions
    checklists = DbtChecklists()

    logger.info(f"Check through manifest file nodes for tag {dbt_tag}...")
    for node_name in manifest_json["nodes"].keys():
        node_type = node_name.split(".")[0]
        if manifest_utils.check_node_in_set_tasks(
            node_name=node_name,
            manifest_json=manifest_json,
            dbt_tag=dbt_tag,
        ):
            checklists = get_model_tasks(
                dbt_tag=dbt_tag,
                manifest_json=manifest_json,
                checklists=checklists,
                node_name=node_name,
            )

        elif node_type == "test":
            for upstream_node in set(
                manifest_json["nodes"][node_name]["depends_on"]["nodes"]
            ):
                if manifest_utils.check_node_in_set_tasks(
                    node_name=upstream_node,
                    manifest_json=manifest_json,
                    dbt_tag=dbt_tag,
                ):
                    checklists = get_model_tests(
                        checklists=checklists,
                        node_name=upstream_node,
                        base_node=node_name,
                    )

        else:
            pass

    dbt_tasks = checklists.dbt_tasks
    dbt_tasks_execution = checklists.dbt_tasks_execution

    return dbt_tasks, dbt_tasks_execution


def generate_single_set(
    manifest_json: Dict[str, Any],
    output_dir: str,
    jinja_template: Template,
    dbt_tag:str
):
    """Function to generate the full DAG for a single given Set config file."""
    print(output_dir)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    #dbt_tag = config.dbt_tag
    set_id = 'Fizlar_dag_test'

    dbt_tasks, dbt_tasks_execution = get_set_tasks(
        manifest_json=manifest_json, dbt_tag=dbt_tag
    )
    output_file_path = Path(os.path.join(output_dir, f"{set_id}.py"))

    #tasks_config = {
    #    **config.dict(),
    #    **{"tasks": dbt_tasks, "tasks_execution": dbt_tasks_execution},
    #}

    #rendered_jinja_template = render_jinja_template(jinja_template, **tasks_config)
    default_args = { 'start_date': 'datetime(2022, 1, 1)'}
    tasks_config = {
        **{"tasks": dbt_tasks, "tasks_execution": dbt_tasks_execution, "set_name": "dag1", "default_args":default_args},
    }
    rendered_jinja_template = render_jinja_template(jinja_template,**tasks_config )
    with open(output_file_path, "w") as f:
        f.write(rendered_jinja_template)

    #if config.wrapper_dag:
    #    wrapper_id = f"wrapper_{config.set_name}"
    #
    #    # Now write the template wrapper file
    #    wrapper_file_path = Path(os.path.join(output_dir, f"{wrapper_id}.py"))
    #    rendered_wrapper = render_jinja_template(WRAPPER_DAG_TEMPLATE, **config.dict())

    #    with open(wrapper_file_path, "w") as f:
    #        f.write(rendered_wrapper)

    #    responses = metadata_utils.write_wrapper_dependencies(
    #        config=config, dag_id=config.set_name, wrapper_id=wrapper_id, db=db
    #    )

    #else:

    #    # Returns Snowflake results messages in for use in logging and debugging
    #    responses = metadata_utils.write_dependencies(
    #        config, config.set_name, db, False
    #    )

    # This is a temporary return for testing purposes while DB calls are mocked.
    return (
        dbt_tasks,
        dbt_tasks_execution
    )
