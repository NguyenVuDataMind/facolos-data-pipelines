#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MISA CRM ETL Airflow DAG
Integrated with TikTok Shop Infrastructure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.models import Variable
import logging
import sys
import os

# Add project root to path
sys.path.append('/opt/airflow/dags')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractors.misa_crm_extractor import MISACRMExtractor
from src.transformers.misa_crm_transformer import MISACRMTransformer
from src.loaders.misa_crm_loader import MISACRMLoader
from config.settings import settings
import uuid

# DAG Configuration - consistent with TikTok Shop pattern
DAG_ID_INCREMENTAL = 'misa_crm_etl_incremental'
DAG_ID_FULL_REFRESH = 'misa_crm_etl_full_refresh'
SCHEDULE_INTERVAL_INCREMENTAL = '*/15 * * * *'  # Every 15 minutes
SCHEDULE_INTERVAL_FULL_REFRESH = '0 2 * * *'    # Daily at 2 AM
START_DATE = datetime(2025, 8, 25)

# Default arguments - consistent with Facolos infrastructure
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=45)
}

def run_misa_crm_incremental_etl(**context):
    """
    Run MISA CRM incremental ETL pipeline
    """
    logger = logging.getLogger(__name__)
    logger.info("🔄 Starting MISA CRM Incremental ETL")

    try:
        # Initialize ETL components
        batch_id = str(uuid.uuid4())
        extractor = MISACRMExtractor()
        transformer = MISACRMTransformer()
        loader = MISACRMLoader()

        start_time = datetime.now()

        # Extract incremental data
        endpoints = ['customers', 'sale_orders', 'contacts', 'stocks', 'products']
        raw_data = {}

        for endpoint in endpoints:
            raw_data[endpoint] = extractor.extract_incremental_data(endpoint)
            logger.info(f"📡 {endpoint}: {len(raw_data[endpoint])} records extracted")

        # Transform data
        transformed_data = transformer.transform_all_endpoints(raw_data, batch_id)

        # Load data (append for incremental)
        loaded_counts = loader.load_all_data_to_staging(transformed_data, truncate_first=False)

        # Calculate duration
        end_time = datetime.now()
        duration = end_time - start_time

        # Prepare result
        result = {
            'success': True,
            'batch_id': batch_id,
            'duration': str(duration),
            'extracted_records': {k: len(v) for k, v in raw_data.items()},
            'loaded_records': loaded_counts,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        }

        logger.info(f"✅ Incremental ETL completed successfully")
        logger.info(f"📊 Batch ID: {batch_id}")
        logger.info(f"⏱️ Duration: {duration}")

        # Log statistics
        for endpoint, count in result['extracted_records'].items():
            logger.info(f"   📡 {endpoint}: {count} records extracted")

        for endpoint, count in result['loaded_records'].items():
            logger.info(f"   📥 {endpoint}: {count} records loaded")

        # Push results to XCom for downstream tasks
        context['task_instance'].xcom_push(key='etl_result', value=result)

        return result

    except Exception as e:
        logger.error(f"❌ ETL pipeline error: {str(e)}")
        raise

def run_misa_crm_full_etl(**context):
    """
    Run MISA CRM full ETL pipeline (backfill)
    """
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting MISA CRM Full ETL (Backfill)")

    try:
        # Initialize ETL components
        batch_id = str(uuid.uuid4())
        extractor = MISACRMExtractor()
        transformer = MISACRMTransformer()
        loader = MISACRMLoader()

        start_time = datetime.now()

        # Extract all data
        endpoints = ['customers', 'sale_orders', 'contacts', 'stocks', 'products']
        raw_data = {}

        for endpoint in endpoints:
            raw_data[endpoint] = extractor.extract_all_data_from_endpoint(endpoint)
            logger.info(f"📡 {endpoint}: {len(raw_data[endpoint])} records extracted")

        # Transform data
        transformed_data = transformer.transform_all_endpoints(raw_data, batch_id)

        # Load data (truncate first for full load)
        loaded_counts = loader.load_all_data_to_staging(transformed_data, truncate_first=True)

        # Calculate duration
        end_time = datetime.now()
        duration = end_time - start_time

        # Prepare result
        result = {
            'success': True,
            'batch_id': batch_id,
            'duration': str(duration),
            'extracted_records': {k: len(v) for k, v in raw_data.items()},
            'loaded_records': loaded_counts,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        }

        logger.info(f"✅ Full ETL completed successfully")
        logger.info(f"📊 Batch ID: {batch_id}")
        logger.info(f"⏱️ Duration: {duration}")

        # Log statistics
        for endpoint, count in result['extracted_records'].items():
            logger.info(f"   📡 {endpoint}: {count} records extracted")

        for endpoint, count in result['loaded_records'].items():
            logger.info(f"   📥 {endpoint}: {count} records loaded")

        # Push results to XCom
        context['task_instance'].xcom_push(key='etl_result', value=result)

        return result

    except Exception as e:
        logger.error(f"❌ ETL pipeline error: {str(e)}")
        raise

def validate_misa_crm_data_quality(**context):
    """
    Validate MISA CRM data quality after ETL
    """
    logger = logging.getLogger(__name__)
    logger.info("🔍 Validating MISA CRM data quality")
    
    try:
        # Get ETL result from previous task
        etl_result = context['task_instance'].xcom_pull(key='etl_result')
        
        if not etl_result:
            raise Exception("No ETL result found in XCom")
        
        # Basic validation checks
        total_records = sum(etl_result['stats']['loaded_records'].values())
        
        if total_records > 0:
            logger.info(f"✅ Data quality validation passed - {total_records} records loaded")
            
            # Log details by endpoint
            for endpoint, count in etl_result['stats']['loaded_records'].items():
                logger.info(f"   📊 {endpoint}: {count} records")
            
            return True
        else:
            logger.warning("⚠️ No records loaded - possible data quality issue")
            return False
            
    except Exception as e:
        logger.error(f"❌ Data quality validation error: {str(e)}")
        return False

def send_misa_crm_success_notification(**context):
    """
    Send success notification for MISA CRM ETL
    """
    logger = logging.getLogger(__name__)
    
    # Get ETL result
    etl_result = context['task_instance'].xcom_pull(key='etl_result')
    
    if etl_result:
        batch_id = etl_result['batch_id']
        duration = etl_result['stats']['duration']
        total_records = sum(etl_result['stats']['loaded_records'].values())
        
        logger.info(f"📧 MISA CRM ETL Success Notification")
        logger.info(f"   Company: {settings.company_name}")
        logger.info(f"   Batch ID: {batch_id}")
        logger.info(f"   Duration: {duration}")
        logger.info(f"   Total Records: {total_records}")
        
        # Here you would integrate with your notification system
        # (Slack, email, etc.) - consistent with TikTok Shop notifications
        
    return True

# =====================================================
# INCREMENTAL ETL DAG (Every 15 minutes)
# =====================================================

incremental_dag = DAG(
    DAG_ID_INCREMENTAL,
    default_args=default_args,
    description='MISA CRM ETL Pipeline - Incremental loads every 15 minutes',
    schedule_interval=SCHEDULE_INTERVAL_INCREMENTAL,
    max_active_runs=1,
    catchup=False,
    tags=['misa_crm', 'etl', 'incremental', 'facolos']
)

# Incremental ETL Tasks
incremental_health_check = BashOperator(
    task_id='misa_crm_incremental_health_check',
    bash_command=f'echo "🏥 MISA CRM Incremental ETL Health Check - $(date) - {settings.company_name}"',
    dag=incremental_dag
)

incremental_etl_task = PythonOperator(
    task_id='run_misa_crm_incremental_etl',
    python_callable=run_misa_crm_incremental_etl,
    dag=incremental_dag,
    pool='misa_crm_pool',  # Use resource pool to limit concurrent runs
    execution_timeout=timedelta(minutes=30)
)

incremental_data_quality_check = PythonOperator(
    task_id='validate_misa_crm_incremental_data_quality',
    python_callable=validate_misa_crm_data_quality,
    dag=incremental_dag,
    execution_timeout=timedelta(minutes=5)
)

incremental_success_notification = PythonOperator(
    task_id='send_misa_crm_incremental_success_notification',
    python_callable=send_misa_crm_success_notification,
    dag=incremental_dag,
    trigger_rule='all_success'
)

# Incremental task dependencies
incremental_health_check >> incremental_etl_task >> incremental_data_quality_check >> incremental_success_notification

# =====================================================
# FULL REFRESH DAG (Daily at 2 AM)
# =====================================================

full_refresh_dag = DAG(
    DAG_ID_FULL_REFRESH,
    default_args={
        **default_args,
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
        'execution_timeout': timedelta(hours=2)  # Longer timeout for full load
    },
    description='MISA CRM ETL Pipeline - Full refresh (daily at 2 AM)',
    schedule_interval=SCHEDULE_INTERVAL_FULL_REFRESH,
    max_active_runs=1,
    catchup=False,
    tags=['misa_crm', 'etl', 'full_refresh', 'facolos']
)

# Full refresh tasks
full_refresh_health_check = BashOperator(
    task_id='misa_crm_full_refresh_health_check',
    bash_command=f'echo "🏥 MISA CRM Full Refresh Health Check - $(date) - {settings.company_name}"',
    dag=full_refresh_dag
)

# Create staging tables (if not exists)
create_misa_crm_staging_tables = MsSqlOperator(
    task_id='create_misa_crm_staging_tables',
    mssql_conn_id='mssql_default',
    sql='staging/create_misa_crm_staging.sql',
    dag=full_refresh_dag
)

full_refresh_etl_task = PythonOperator(
    task_id='run_misa_crm_full_etl',
    python_callable=run_misa_crm_full_etl,
    dag=full_refresh_dag,
    pool='misa_crm_pool',
    execution_timeout=timedelta(hours=2)  # Longer timeout for full load
)

full_refresh_data_quality_check = PythonOperator(
    task_id='validate_misa_crm_full_refresh_data_quality',
    python_callable=validate_misa_crm_data_quality,
    dag=full_refresh_dag,
    execution_timeout=timedelta(minutes=10)
)

full_refresh_success_notification = PythonOperator(
    task_id='send_misa_crm_full_refresh_success_notification',
    python_callable=send_misa_crm_success_notification,
    dag=full_refresh_dag,
    trigger_rule='all_success'
)

# Full refresh task dependencies
full_refresh_health_check >> create_misa_crm_staging_tables >> full_refresh_etl_task >> full_refresh_data_quality_check >> full_refresh_success_notification

# =====================================================
# MANUAL BACKFILL DAG (On-demand)
# =====================================================

manual_backfill_dag = DAG(
    'misa_crm_etl_manual_backfill',
    default_args={
        **default_args,
        'retries': 1,
        'retry_delay': timedelta(minutes=15),
        'execution_timeout': timedelta(hours=3)
    },
    description='MISA CRM ETL Pipeline - Manual backfill (on-demand trigger)',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    catchup=False,
    tags=['misa_crm', 'etl', 'backfill', 'manual', 'facolos']
)

manual_backfill_health_check = BashOperator(
    task_id='misa_crm_manual_backfill_health_check',
    bash_command=f'echo "🏥 MISA CRM Manual Backfill Health Check - $(date) - {settings.company_name}"',
    dag=manual_backfill_dag
)

manual_create_staging_tables = MsSqlOperator(
    task_id='manual_create_misa_crm_staging_tables',
    mssql_conn_id='mssql_default',
    sql='staging/create_misa_crm_staging.sql',
    dag=manual_backfill_dag
)

manual_backfill_etl_task = PythonOperator(
    task_id='run_misa_crm_manual_backfill',
    python_callable=run_misa_crm_full_etl,
    dag=manual_backfill_dag,
    pool='misa_crm_pool',
    execution_timeout=timedelta(hours=3)
)

manual_backfill_data_quality_check = PythonOperator(
    task_id='validate_misa_crm_manual_backfill_data_quality',
    python_callable=validate_misa_crm_data_quality,
    dag=manual_backfill_dag,
    execution_timeout=timedelta(minutes=15)
)

manual_backfill_success_notification = PythonOperator(
    task_id='send_misa_crm_manual_backfill_success_notification',
    python_callable=send_misa_crm_success_notification,
    dag=manual_backfill_dag,
    trigger_rule='all_success'
)

# Manual backfill task dependencies
manual_backfill_health_check >> manual_create_staging_tables >> manual_backfill_etl_task >> manual_backfill_data_quality_check >> manual_backfill_success_notification
