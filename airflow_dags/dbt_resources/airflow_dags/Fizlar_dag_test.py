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
schedule_interval='',
default_args=default_args,
catchup=False,
)
def dag1():

    start = DummyOperator(
    task_id="start",
    )
    with TaskGroup("task_group_dag1") as task_group_dag1:
        
        promotion_rep = DbtRunOperator(
            task_id="promotion_rep",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="promotion_rep"
            )
        
        sale_transaction_item_base_br = DbtRunOperator(
            task_id="sale_transaction_item_base_br",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_base_br"
            )
        
        sale_transaction_item_discount_br = DbtRunOperator(
            task_id="sale_transaction_item_discount_br",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_discount_br"
            )
        
        sale_transaction_payment_br = DbtRunOperator(
            task_id="sale_transaction_payment_br",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_payment_br"
            )
        
        sale_transaction_tender_reward_br = DbtRunOperator(
            task_id="sale_transaction_tender_reward_br",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_tender_reward_br"
            )
        
        sale_transaction_item_discount_br_v = DbtRunOperator(
            task_id="sale_transaction_item_discount_br_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_discount_br_v"
            )
        
        sts_bdv_item_promotion_tender_rewards_tab_v = DbtRunOperator(
            task_id="sts_bdv_item_promotion_tender_rewards_tab_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_item_promotion_tender_rewards_tab_v"
            )
        
        sts_bdv_sale_transaction_item_br_base_v = DbtRunOperator(
            task_id="sts_bdv_sale_transaction_item_br_base_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_sale_transaction_item_br_base_v"
            )
        
        sts_bdv_sale_transaction_item_br_rpm_v = DbtRunOperator(
            task_id="sts_bdv_sale_transaction_item_br_rpm_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_sale_transaction_item_br_rpm_v"
            )
        
        sts_bdv_sale_transaction_item_br_v = DbtRunOperator(
            task_id="sts_bdv_sale_transaction_item_br_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_sale_transaction_item_br_v"
            )
        
        promotion_rep
        sale_transaction_item_discount_br_v >> sale_transaction_item_discount_br
        sale_transaction_payment_br
        sale_transaction_tender_reward_br >> sts_bdv_item_promotion_tender_rewards_tab_v
        sale_transaction_item_base_br >> sts_bdv_sale_transaction_item_br_base_v
        sts_bdv_sale_transaction_item_br_rpm_v
        sts_bdv_sale_transaction_item_br_v
    
    notify_success = DummyOperator(task_id='notify_success', trigger_rule=TriggerRule.ALL_SUCCESS,
                   on_success_callback=send_success_message)

    notify_failure = DummyOperator(task_id='notify_failure', trigger_rule=TriggerRule.ONE_FAILED,
                   on_success_callback=send_failure_message)

    end = DummyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE
    )

    start >> task_group_dag1 >> [notify_success, notify_failure] >> end
    

dag1_dag = dag1()