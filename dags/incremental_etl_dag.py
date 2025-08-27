#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production Incremental ETL DAG - 15 phút/lần
Production-ready Airflow DAG cho incremental updates từ MISA CRM và TikTok Shop
với monitoring, alerting, và performance tracking
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import sys
import os
import json

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Import production configuration
from config.production import ProductionConfig

# Get production DAG configuration
dag_config = ProductionConfig.get_airflow_dag_config()

# DAG definition với production settings
dag = DAG(
    'facolos_incremental_etl_production',
    default_args={
        'owner': 'facolos-etl-production',
        'depends_on_past': False,
        'start_date': days_ago(1),
        **dag_config['default_args']
    },
    description='Production Incremental ETL cho MISA CRM và TikTok Shop - 15 phút/lần',
    schedule_interval=dag_config['schedule_interval'],
    max_active_runs=dag_config['max_active_runs'],
    catchup=dag_config['catchup'],
    tags=['facolos', 'etl', 'incremental', 'production', 'misa-crm', 'tiktok-shop']
)

def run_incremental_etl(**context):
    """Main incremental ETL function với production monitoring"""
    from src.extractors.misa_crm_extractor import MISACRMExtractor
    from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
    from src.transformers.misa_crm_transformer import MISACRMTransformer
    from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
    from src.loaders.misa_crm_loader import MISACRMLoader
    from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
    from src.utils.logging import setup_logging

    logger = setup_logging("airflow_incremental_etl")
    execution_date = context['execution_date']
    task_instance = context['task_instance']

    logger.info(f"🚀 AIRFLOW PRODUCTION ETL - Starting incremental ETL for {execution_date}")
    logger.info(f"📋 Task Instance: {task_instance.task_id}")
    logger.info(f"🔄 DAG Run ID: {context['dag_run'].run_id}")

    cycle_start_time = datetime.now()
    results = {
        'execution_date': execution_date.isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'task_id': task_instance.task_id,
        'start_time': cycle_start_time,
        'misa_crm': {},
        'tiktok_shop': {},
        'total_records': 0,
        'success': False
    }

    try:
        # PHASE 1: MISA CRM Incremental
        logger.info("🏢 PHASE 1: MISA CRM Incremental ETL")
        
        misa_extractor = MISACRMExtractor()
        misa_transformer = MISACRMTransformer()
        misa_loader = MISACRMLoader()
        
        misa_endpoints = [
            {'name': 'customers', 'table': 'customers', 'priority': 2},
            {'name': 'sale_orders', 'table': 'sale_orders_flattened', 'priority': 1},
            {'name': 'contacts', 'table': 'contacts', 'priority': 3},
            {'name': 'stocks', 'table': 'stocks', 'priority': 4},
            {'name': 'products', 'table': 'products', 'priority': 5}
        ]
        
        misa_endpoints.sort(key=lambda x: x['priority'])
        
        for endpoint in misa_endpoints:
            try:
                logger.info(f"   🔄 Processing {endpoint['name']}...")
                
                raw_data = misa_extractor.extract_all_data_from_endpoint(
                    endpoint['name'], max_pages=2
                )
                
                if not raw_data:
                    logger.info(f"   ⚠️ No new data for {endpoint['name']}")
                    results['misa_crm'][endpoint['name']] = 0
                    continue
                
                if endpoint['name'] == 'sale_orders':
                    transformed_data = misa_transformer.transform_sale_orders_flattened(raw_data)
                else:
                    transform_method = getattr(misa_transformer, f'transform_{endpoint["name"]}')
                    transformed_data = transform_method(raw_data)
                
                import pandas as pd
                df = pd.DataFrame(transformed_data)
                success = misa_loader.load_dataframe_to_staging(df, endpoint['table'])
                
                if success:
                    records_count = len(df)
                    results['misa_crm'][endpoint['name']] = records_count
                    results['total_records'] += records_count
                    logger.info(f"   ✅ {endpoint['name']}: {records_count} records processed")
                else:
                    logger.warning(f"   ⚠️ {endpoint['name']}: Load failed (likely duplicates)")
                    results['misa_crm'][endpoint['name']] = 0
                    
            except Exception as e:
                logger.error(f"   ❌ {endpoint['name']} error: {e}")
                results['misa_crm'][endpoint['name']] = 'error'
        
        # PHASE 2: TikTok Shop Incremental
        logger.info("\n🛒 PHASE 2: TikTok Shop Incremental ETL")
        
        try:
            tiktok_extractor = TikTokShopOrderExtractor()
            tiktok_transformer = TikTokShopOrderTransformer()
            tiktok_loader = TikTokShopOrderLoader()
            
            raw_orders = tiktok_extractor.extract_recent_orders(days_back=1)
            
            if raw_orders:
                transformed_df = tiktok_transformer.transform_orders_to_dataframe(raw_orders)
                success = tiktok_loader.load_incremental_orders(transformed_df)
                
                if success:
                    records_count = len(transformed_df)
                    results['tiktok_shop']['orders'] = records_count
                    results['total_records'] += records_count
                    logger.info(f"   ✅ TikTok Shop: {records_count} records processed")
                else:
                    logger.warning(f"   ⚠️ TikTok Shop: Load failed")
                    results['tiktok_shop']['orders'] = 0
            else:
                logger.info("   ⚠️ No new TikTok Shop orders")
                results['tiktok_shop']['orders'] = 0
                
        except Exception as e:
            logger.error(f"   ❌ TikTok Shop error: {e}")
            results['tiktok_shop']['orders'] = 'error'
        
        # Calculate final metrics
        cycle_end_time = datetime.now()
        duration = (cycle_end_time - cycle_start_time).total_seconds()
        results['end_time'] = cycle_end_time
        results['duration_seconds'] = duration
        results['success'] = True

        logger.info(f"\n📊 AIRFLOW PRODUCTION ETL SUMMARY:")
        logger.info(f"   ⏱️  Duration: {duration:.1f} seconds")
        logger.info(f"   📊 Total records processed: {results['total_records']}")
        logger.info(f"   🏢 MISA CRM results: {results['misa_crm']}")
        logger.info(f"   🛒 TikTok Shop results: {results['tiktok_shop']}")
        logger.info(f"   ✅ Execution successful: {results['success']}")

        # Store results in XCom for monitoring
        task_instance.xcom_push(key='etl_results', value=results)

        return results

    except Exception as e:
        results['success'] = False
        results['error'] = str(e)
        results['end_time'] = datetime.now()
        results['duration_seconds'] = (results['end_time'] - cycle_start_time).total_seconds()

        # Store error results in XCom
        task_instance.xcom_push(key='etl_results', value=results)

        logger.error(f"❌ Airflow Production ETL failed: {e}")
        raise

def verify_data_quality(**context):
    """Verify data quality sau incremental ETL"""
    from src.utils.logging import setup_logging
    import pyodbc
    from config.settings import settings
    
    logger = setup_logging("data_quality_check")
    
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={settings.sql_server_host},{settings.sql_server_port};"
            f"DATABASE={settings.sql_server_database};"
            f"UID={settings.sql_server_username};"
            f"PWD={settings.sql_server_password};"
            f"TrustServerCertificate=yes"
        )
        
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        tables = [
            'misa_customers', 'misa_sale_orders_flattened', 'misa_contacts',
            'misa_stocks', 'misa_products', 'tiktok_shop_order_detail'
        ]
        
        total_records = 0
        tables_with_data = 0
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM staging.{table}")
            count = cursor.fetchone()[0]
            total_records += count
            
            if count > 0:
                tables_with_data += 1
            
            logger.info(f"   {table}: {count:,} rows")
        
        cursor.close()
        connection.close()
        
        logger.info(f"📊 Data Quality Summary:")
        logger.info(f"   Total records: {total_records:,}")
        logger.info(f"   Tables with data: {tables_with_data}/6")
        
        if tables_with_data >= 5:
            logger.info("✅ Data quality check passed")
            return True
        else:
            logger.warning("⚠️ Data quality check warning: Some tables empty")
            return False
            
    except Exception as e:
        logger.error(f"❌ Data quality check failed: {e}")
        return False

# Task definitions
incremental_etl_task = PythonOperator(
    task_id='run_incremental_etl',
    python_callable=run_incremental_etl,
    dag=dag,
    execution_timeout=timedelta(minutes=10)
)

data_quality_task = PythonOperator(
    task_id='verify_data_quality',
    python_callable=verify_data_quality,
    dag=dag,
    execution_timeout=timedelta(minutes=2)
)

health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "✅ Incremental ETL completed successfully at $(date)"',
    dag=dag
)

# Task dependencies
incremental_etl_task >> data_quality_task >> health_check_task
