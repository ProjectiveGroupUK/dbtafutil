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
def kiwi_bdv():

    start = DummyOperator(
    task_id="start",
    )
    with TaskGroup("task_group_kiwi_bdv") as task_group_kiwi_bdv:
        
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
        
        dim_promotion = DbtRunOperator(
            task_id="dim_promotion",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="dim_promotion"
            )
        
        dim_tender_reward_type = DbtRunOperator(
            task_id="dim_tender_reward_type",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="dim_tender_reward_type"
            )
        
        dim_tender_type = DbtRunOperator(
            task_id="dim_tender_type",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="dim_tender_type"
            )
        
        fact_sale_transaction_item_tender_reward = DbtRunOperator(
            task_id="fact_sale_transaction_item_tender_reward",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="fact_sale_transaction_item_tender_reward"
            )
        
        fact_sale_transaction_tender = DbtRunOperator(
            task_id="fact_sale_transaction_tender",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="fact_sale_transaction_tender"
            )
        
        sale_transaction_coupon_stssls_link = DbtRunOperator(
            task_id="sale_transaction_coupon_stssls_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_coupon_stssls_link"
            )
        
        sale_transaction_header_stssls_link = DbtRunOperator(
            task_id="sale_transaction_header_stssls_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_header_stssls_link"
            )
        
        sale_transaction_item_price_modifier_stssls_link = DbtRunOperator(
            task_id="sale_transaction_item_price_modifier_stssls_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_price_modifier_stssls_link"
            )
        
        sale_transaction_item_stssls_link = DbtRunOperator(
            task_id="sale_transaction_item_stssls_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_stssls_link"
            )
        
        sale_transaction_link_stssls_link = DbtRunOperator(
            task_id="sale_transaction_link_stssls_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_link_stssls_link"
            )
        
        sale_transaction_payment_stssls_link = DbtRunOperator(
            task_id="sale_transaction_payment_stssls_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_payment_stssls_link"
            )
        
        sale_transaction_tender_reward_stssls_link = DbtRunOperator(
            task_id="sale_transaction_tender_reward_stssls_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_tender_reward_stssls_link"
            )
        
        sale_trans_promotion_hub = DbtRunOperator(
            task_id="sale_trans_promotion_hub",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_trans_promotion_hub"
            )
        
        sale_trans_promotion_stssls_sat = DbtRunOperator(
            task_id="sale_trans_promotion_stssls_sat",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_trans_promotion_stssls_sat"
            )
        
        osts_raw_sales_tran_transformed_stage = DbtRunOperator(
            task_id="osts_raw_sales_tran_transformed_stage",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="osts_raw_sales_tran_transformed_stage"
            )
        
        raw_osts_item_price_modifier = DbtRunOperator(
            task_id="raw_osts_item_price_modifier",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_item_price_modifier"
            )
        
        raw_osts_item_promo = DbtRunOperator(
            task_id="raw_osts_item_promo",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_item_promo"
            )
        
        raw_osts_trans = DbtRunOperator(
            task_id="raw_osts_trans",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans"
            )
        
        raw_osts_trans_coupon = DbtRunOperator(
            task_id="raw_osts_trans_coupon",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_coupon"
            )
        
        raw_osts_trans_item = DbtRunOperator(
            task_id="raw_osts_trans_item",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_item"
            )
        
        raw_osts_trans_link = DbtRunOperator(
            task_id="raw_osts_trans_link",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_link"
            )
        
        raw_osts_trans_payment = DbtRunOperator(
            task_id="raw_osts_trans_payment",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_payment"
            )
        
        sts_bdv_item_promotion_tender_rewards_tab = DbtRunOperator(
            task_id="sts_bdv_item_promotion_tender_rewards_tab",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_item_promotion_tender_rewards_tab"
            )
        
        sts_bdv_sale_transaction_item_br = DbtRunOperator(
            task_id="sts_bdv_sale_transaction_item_br",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_sale_transaction_item_br"
            )
        
        sts_bdv_sale_transaction_item_br_base = DbtRunOperator(
            task_id="sts_bdv_sale_transaction_item_br_base",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_sale_transaction_item_br_base"
            )
        
        sts_bdv_sale_transaction_item_br_rpm = DbtRunOperator(
            task_id="sts_bdv_sale_transaction_item_br_rpm",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_bdv_sale_transaction_item_br_rpm"
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
        
        dim_promotion_v = DbtRunOperator(
            task_id="dim_promotion_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="dim_promotion_v"
            )
        
        dim_tender_reward_type_v = DbtRunOperator(
            task_id="dim_tender_reward_type_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="dim_tender_reward_type_v"
            )
        
        dim_tender_type_v = DbtRunOperator(
            task_id="dim_tender_type_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="dim_tender_type_v"
            )
        
        fact_sale_transaction_item_tender_reward_v = DbtRunOperator(
            task_id="fact_sale_transaction_item_tender_reward_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="fact_sale_transaction_item_tender_reward_v"
            )
        
        fact_sale_transaction_tender_v = DbtRunOperator(
            task_id="fact_sale_transaction_tender_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="fact_sale_transaction_tender_v"
            )
        
        raw_osts_item_price_modifier_v = DbtRunOperator(
            task_id="raw_osts_item_price_modifier_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_item_price_modifier_v"
            )
        
        raw_osts_item_promotion_v = DbtRunOperator(
            task_id="raw_osts_item_promotion_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_item_promotion_v"
            )
        
        raw_osts_item_promo_v = DbtRunOperator(
            task_id="raw_osts_item_promo_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_item_promo_v"
            )
        
        raw_osts_promo_data_v = DbtRunOperator(
            task_id="raw_osts_promo_data_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_promo_data_v"
            )
        
        raw_osts_trans_coupon_link_v = DbtRunOperator(
            task_id="raw_osts_trans_coupon_link_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_coupon_link_v"
            )
        
        raw_osts_trans_coupon_v = DbtRunOperator(
            task_id="raw_osts_trans_coupon_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_coupon_v"
            )
        
        raw_osts_trans_header_v = DbtRunOperator(
            task_id="raw_osts_trans_header_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_header_v"
            )
        
        raw_osts_trans_item_v = DbtRunOperator(
            task_id="raw_osts_trans_item_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_item_v"
            )
        
        raw_osts_trans_link_v = DbtRunOperator(
            task_id="raw_osts_trans_link_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_link_v"
            )
        
        raw_osts_trans_payment_v = DbtRunOperator(
            task_id="raw_osts_trans_payment_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_payment_v"
            )
        
        raw_osts_trans_tender_reward_v = DbtRunOperator(
            task_id="raw_osts_trans_tender_reward_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_tender_reward_v"
            )
        
        raw_osts_trans_v = DbtRunOperator(
            task_id="raw_osts_trans_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="raw_osts_trans_v"
            )
        
        sale_transaction_item_base_br_v = DbtRunOperator(
            task_id="sale_transaction_item_base_br_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_base_br_v"
            )
        
        sale_transaction_item_price_modifier_stssls_link_v = DbtRunOperator(
            task_id="sale_transaction_item_price_modifier_stssls_link_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_price_modifier_stssls_link_v"
            )
        
        sale_transaction_item_stssls_link_v = DbtRunOperator(
            task_id="sale_transaction_item_stssls_link_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_item_stssls_link_v"
            )
        
        sale_transaction_link_stssls_link_v = DbtRunOperator(
            task_id="sale_transaction_link_stssls_link_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_link_stssls_link_v"
            )
        
        sale_transaction_payment_br_v = DbtRunOperator(
            task_id="sale_transaction_payment_br_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_payment_br_v"
            )
        
        sale_transaction_payment_stssls_link_v = DbtRunOperator(
            task_id="sale_transaction_payment_stssls_link_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_payment_stssls_link_v"
            )
        
        sale_transaction_tender_reward_br_v = DbtRunOperator(
            task_id="sale_transaction_tender_reward_br_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sale_transaction_tender_reward_br_v"
            )
        
        sts_rdv_sale_transaction_v = DbtRunOperator(
            task_id="sts_rdv_sale_transaction_v",
            dbt_bin=f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/.local/bin/dbt",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="sts_rdv_sale_transaction_v"
            )
        
        test_osts_raw_sales_tran_transformed_stage = DbtTestOperator(
            task_id="test_osts_raw_sales_tran_transformed_stage",
            profiles_dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            dir=f"{os.path.dirname(os.path.abspath(__file__))}/dbt/",
            target="dev",
            models="osts_raw_sales_tran_transformed_stage"
            )
        
        raw_osts_promo_data_v >> promotion_rep
        sale_transaction_item_base_br_v >> sale_transaction_item_base_br
        sale_transaction_item_discount_br_v >> sale_transaction_item_discount_br
        sale_transaction_payment_br_v >> sale_transaction_payment_br
        sale_transaction_tender_reward_br_v >> sale_transaction_tender_reward_br
        dim_promotion_v >> dim_promotion
        dim_tender_reward_type_v >> dim_tender_reward_type
        dim_tender_type_v >> dim_tender_type
        fact_sale_transaction_item_tender_reward_v >> fact_sale_transaction_item_tender_reward
        fact_sale_transaction_tender_v >> fact_sale_transaction_tender
        raw_osts_trans_coupon_link_v >> sale_transaction_coupon_stssls_link
        raw_osts_trans_header_v >> sale_transaction_header_stssls_link
        sale_transaction_item_price_modifier_stssls_link_v >> sale_transaction_item_price_modifier_stssls_link
        sale_transaction_item_stssls_link_v >> sale_transaction_item_stssls_link
        sale_transaction_link_stssls_link_v >> sale_transaction_link_stssls_link
        sale_transaction_payment_stssls_link_v >> sale_transaction_payment_stssls_link
        raw_osts_trans_tender_reward_v >> sale_transaction_tender_reward_stssls_link
        raw_osts_item_promotion_v >> sale_trans_promotion_hub
        raw_osts_item_promotion_v >> sale_trans_promotion_stssls_sat
        raw_osts_trans >> osts_raw_sales_tran_transformed_stage
        raw_osts_item_price_modifier_v >> raw_osts_item_price_modifier
        raw_osts_item_promo_v >> raw_osts_item_promo
        raw_osts_trans_v >> raw_osts_trans
        raw_osts_trans_coupon_v >> raw_osts_trans_coupon
        raw_osts_trans_item_v >> raw_osts_trans_item
        raw_osts_trans_link_v >> raw_osts_trans_link
        raw_osts_trans_payment_v >> raw_osts_trans_payment
        sts_bdv_item_promotion_tender_rewards_tab_v >> sts_bdv_item_promotion_tender_rewards_tab
        sts_bdv_sale_transaction_item_br_v >> sts_bdv_sale_transaction_item_br
        sts_bdv_sale_transaction_item_br_base_v >> sts_bdv_sale_transaction_item_br_base
        sts_bdv_sale_transaction_item_br_rpm_v >> sts_bdv_sale_transaction_item_br_rpm
        raw_osts_item_price_modifier >> sale_transaction_item_discount_br_v
        sale_transaction_tender_reward_br >> sts_bdv_item_promotion_tender_rewards_tab_v
        sale_transaction_item_base_br >> sts_bdv_sale_transaction_item_br_base_v
        sale_transaction_item_price_modifier_stssls_link >> sts_bdv_sale_transaction_item_br_rpm_v
        sts_bdv_sale_transaction_item_br_base >> sts_bdv_sale_transaction_item_br_v
        sale_transaction_header_stssls_link >> sts_bdv_sale_transaction_item_br_v
        sts_bdv_item_promotion_tender_rewards_tab >> sts_bdv_sale_transaction_item_br_v
        sts_bdv_sale_transaction_item_br_rpm >> sts_bdv_sale_transaction_item_br_v
        promotion_rep >> dim_promotion_v
        sale_transaction_payment_br >> fact_sale_transaction_tender_v
        raw_osts_trans >> raw_osts_item_price_modifier_v
        raw_osts_trans >> raw_osts_item_promotion_v
        raw_osts_trans >> raw_osts_item_promo_v
        raw_osts_trans_coupon >> raw_osts_trans_coupon_link_v
        raw_osts_trans >> raw_osts_trans_coupon_v
        raw_osts_trans >> raw_osts_trans_header_v
        raw_osts_trans >> raw_osts_trans_item_v
        raw_osts_trans >> raw_osts_trans_link_v
        raw_osts_trans >> raw_osts_trans_payment_v
        raw_osts_trans_coupon >> raw_osts_trans_payment_v
        raw_osts_item_promo >> raw_osts_trans_tender_reward_v
        raw_osts_trans_payment >> raw_osts_trans_tender_reward_v
        raw_osts_trans_item >> sale_transaction_item_base_br_v
        raw_osts_trans_link >> sale_transaction_item_base_br_v
        raw_osts_item_price_modifier >> sale_transaction_item_price_modifier_stssls_link_v
        raw_osts_trans_item >> sale_transaction_item_stssls_link_v
        raw_osts_trans_link >> sale_transaction_link_stssls_link_v
        raw_osts_trans_payment >> sale_transaction_payment_br_v
        raw_osts_trans_payment >> sale_transaction_payment_stssls_link_v
        sts_rdv_sale_transaction_v
        osts_raw_sales_tran_transformed_stage >> test_osts_raw_sales_tran_transformed_stage
    
    end = DummyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE
    )

    start >> task_group_kiwi_bdv >> end
    

kiwi_bdv_dag = kiwi_bdv()