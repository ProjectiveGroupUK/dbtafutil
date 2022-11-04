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
from plugins.event_bridge_plugin import send_failure_message, send_success_message
from airflow.utils.trigger_rule import TriggerRule



default_args = {
    "start_date": datetime(2022, 1, 1),
}

@dag(
schedule_interval=None,
default_args=default_args,
catchup=False,
)
def test():

    start = DummyOperator(
    task_id="start",
    )
    with TaskGroup("task_group_test") as task_group_test:
        
        my_first_dbt_model = DbtRunOperator(
            task_id="my_first_dbt_model",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="my_first_dbt_model"
            )
        
        test_my_first_dbt_model = DbtTestOperator(
            task_id="test_my_first_dbt_model",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="my_first_dbt_model"
            )
        
        my_second_dbt_model = DbtRunOperator(
            task_id="my_second_dbt_model",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="my_second_dbt_model"
            )
        
        test_my_second_dbt_model = DbtTestOperator(
            task_id="test_my_second_dbt_model",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="my_second_dbt_model"
            )
        
        stand_alone_mode = DbtRunOperator(
            task_id="stand_alone_mode",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="stand_alone_mode"
            )
        
        my_first_dbt_model
        my_first_dbt_model >> test_my_first_dbt_model
        my_first_dbt_model >> my_second_dbt_model
        my_second_dbt_model >> test_my_second_dbt_model
        stand_alone_mode
    
    notify_success = DummyOperator(task_id='notify_success', trigger_rule=TriggerRule.ALL_SUCCESS,
                   on_success_callback=send_success_message)

    notify_failure = DummyOperator(task_id='notify_failure', trigger_rule=TriggerRule.ONE_FAILED,
                   on_success_callback=send_failure_message)

    end = DummyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE
    )

    start >> task_group_test >> [notify_success, notify_failure] >> end
    

test_dag = test()