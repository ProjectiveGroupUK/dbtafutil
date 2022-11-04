import os
import subprocess
from typing import Any, Dict, Optional, Tuple

#from dotenv import find_dotenv, load_dotenv

#from dbtafutil.constants import PACKAGE_DIRECTORY


def check_node_in_set_tasks(
    node_name: str,
    manifest_json: Dict[str, Any],
    dbt_tag: Optional[str] = None,
) -> bool:
    """Check if a node in the manifest file should be included in the set."""
    # Only include model nodes
    if node_name.split(".")[0] != "model":
        return False
    try:
        nodes = manifest_json["nodes"]
    except KeyError as e:
        raise KeyError("Nodes section of manifest file could not be found.") from e
    try:
        node = nodes[node_name]
    except KeyError as e:
        raise KeyError(
            f"Given node name {node_name} is not present in manifest json nodes block"
        ) from e
    try:
        tags = node["tags"]
    except KeyError as e:
        raise KeyError(
            f"The tags section of the `{node_name}` node could not be found in manifest file."
        ) from e
        # Only include if the tag is specified in the configuration
    return dbt_tag is None or dbt_tag in tags


def build_task_dict(node_name: str, task_type: str) -> Tuple[Dict[str, str], str]:
    if "." not in node_name:
        raise ValueError(
            f"Given node name `{node_name}` is not a valid dbt node name. No period found to split on."
        )

    if task_type.lower() == "run":
        node_task_id = node_name.split(".")[-1]
        task_dict = {
            "task_type": task_type,
            "task_id": node_task_id,
            "dbt_model": node_task_id,
        }

    elif task_type.lower() == "test":
        node_task_id = node_name.split(".")[-1]
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


#def generate_manifest_file(dbt_project_dir: str, manifest_path: str) -> None:
#    """
#    Function to generate a new manifest file, in the same location where one was expected.
#    """
#
#    # We will try and access our GHA secrets, to check which authentication profile we can use
#    if os.getenv("SF_ACCOUNT") == "sainsburys.eu-west-1.privatelink":
#        dbt_profile = "ci-admin"
#    else:
#        # If we are running locally, load the env file and
#        load_dotenv(find_dotenv())
#        dbt_profile = "local-admin"
#
#    # Get the parent directory of the expected manifest file location, and pass it to the dbt compile command
#    # to set where dbt writes the manifest file. We also specify to use an admin dbt profile, stored in our repo
#    # which means we can generate this manifest regardless of the settings in dbt_project.yml
#    target_dir = os.path.dirname(manifest_path)
#    dbt_profile_dir = str(PACKAGE_DIRECTORY / "set_generator" / "utils")
#
#    generate_command = [
#        "dbt",
#        "compile",
#        "--project-dir",
#        dbt_project_dir,
#        "--target-path",
#        target_dir,
#        "--profile",
#        dbt_profile,
#        "--profiles-dir",
#        dbt_profile_dir,
#    ]
#
#    # Execute the dbt docs generate command
#    subprocess.run(args=generate_command, capture_output=True, encoding="UTF-8")
