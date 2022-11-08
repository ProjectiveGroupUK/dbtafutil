# DAG dynamically generated using the generator python script
# written by Nitin Garg from DTSQUARED
#
# Generator is used to generate starter DAG, feel free to make
# any further modification as necessary
#
# Don't forget to override Version Number on top of this script
# if you are making modifications manually and don't want
# it to be overwritten by generator accidentally
import os
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator
from airflow.utils.trigger_rule import TriggerRule



default_args = {
    "start_date": datetime(2022, 1, 1),
}

@dag(
schedule_interval=None,
default_args=default_args,
catchup=False,
)
def raw_osts_item_price_modifier():

    start = DummyOperator(
    task_id="start",
    )
    with TaskGroup("task_group_raw_osts_item_price_modifier") as task_group_raw_osts_item_price_modifier:
        
        raw_osts_item_price_modifier = DbtRunOperator(
            task_id="raw_osts_item_price_modifier",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_item_price_modifier"
            )
        
        raw_osts_item_price_modifier_v >> raw_osts_item_price_modifier
        raw_osts_trans >> raw_osts_item_price_modifier
        raw_osts_trans_v >> raw_osts_item_price_modifier
        OSTS_RAW_SALES_TRAN_STAGE_NEW >> raw_osts_item_price_modifier
    
    end = DummyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE
    )

    start >> task_group_raw_osts_item_price_modifier >> end
    

raw_osts_item_price_modifier_dag = raw_osts_item_price_modifier()