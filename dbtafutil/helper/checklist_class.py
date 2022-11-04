from typing import Dict, List


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
