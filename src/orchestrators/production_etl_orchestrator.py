#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production ETL Orchestrator
Orchestrator chính cho production ETL pipeline với monitoring tích hợp
"""

import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.extractors.misa_crm_extractor import MISACRMExtractor
from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
from src.transformers.misa_crm_transformer import MISACRMTransformer
from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
from src.loaders.misa_crm_loader import MISACRMLoader
from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
from src.monitoring.production_monitor import production_monitor
from src.utils.logging import setup_logging
from config.production import ProductionConfig

logger = setup_logging("production_etl_orchestrator")

class ProductionETLOrchestrator:
    """
    Production ETL Orchestrator với monitoring và error handling
    """
    
    def __init__(self):
        self.config = ProductionConfig()
        self.monitor = production_monitor
        
        # Initialize components
        self.misa_extractor = None
        self.misa_transformer = None
        self.misa_loader = None
        
        self.tiktok_extractor = None
        self.tiktok_transformer = None
        self.tiktok_loader = None
        
        # Cycle tracking
        self.cycle_count = 0
        self.start_time = None
        
    def initialize_components(self) -> bool:
        """Initialize all ETL components với error handling"""
        try:
            logger.info("🔧 Initializing production ETL components...")
            
            # MISA CRM components
            self.misa_extractor = MISACRMExtractor()
            self.misa_transformer = MISACRMTransformer()
            self.misa_loader = MISACRMLoader()
            
            # TikTok Shop components
            self.tiktok_extractor = TikTokShopOrderExtractor()
            self.tiktok_transformer = TikTokShopOrderTransformer()
            self.tiktok_loader = TikTokShopOrderLoader()
            
            logger.info("✅ All production ETL components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize production components: {e}")
            return False
    
    def run_production_cycle(self, execution_context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Run một production ETL cycle với comprehensive monitoring
        """
        self.cycle_count += 1
        self.start_time = datetime.now()
        
        logger.info(f"🚀 PRODUCTION ETL CYCLE #{self.cycle_count}")
        logger.info(f"⏰ Started at: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
        
        cycle_results = {
            'cycle_number': self.cycle_count,
            'start_time': self.start_time,
            'execution_context': execution_context,
            'misa_crm': {},
            'tiktok_shop': {},
            'total_records': 0,
            'duration_seconds': 0,
            'success': False,
            'errors': []
        }
        
        try:
            # Initialize components if needed
            if not self.misa_extractor:
                if not self.initialize_components():
                    cycle_results['errors'].append("Component initialization failed")
                    return self._finalize_cycle_results(cycle_results)
            
            # PHASE 1: MISA CRM Processing
            logger.info("🏢 PHASE 1: MISA CRM Production Processing")
            misa_results = self._process_misa_crm_endpoints()
            cycle_results['misa_crm'] = misa_results
            
            # PHASE 2: TikTok Shop Processing
            logger.info("\n🛒 PHASE 2: TikTok Shop Production Processing")
            tiktok_results = self._process_tiktok_shop_data()
            cycle_results['tiktok_shop'] = tiktok_results
            
            # PHASE 3: Data Quality Verification
            logger.info("\n🔍 PHASE 3: Production Data Quality Verification")
            quality_results = self._verify_data_quality()
            cycle_results['data_quality'] = quality_results
            
            # Calculate totals
            cycle_results['total_records'] = self._calculate_total_records(cycle_results)
            cycle_results['success'] = self._determine_cycle_success(cycle_results)
            
            return self._finalize_cycle_results(cycle_results)
            
        except Exception as e:
            logger.error(f"❌ Production cycle #{self.cycle_count} failed: {e}")
            cycle_results['errors'].append(f"Cycle exception: {str(e)}")
            cycle_results['success'] = False
            return self._finalize_cycle_results(cycle_results)
    
    def _process_misa_crm_endpoints(self) -> Dict[str, Any]:
        """Process all MISA CRM endpoints với production settings"""
        
        misa_results = {}
        
        # MISA endpoints với priority order
        endpoints = [
            {'name': 'sale_orders', 'table': 'sale_orders_flattened', 'priority': 1},
            {'name': 'customers', 'table': 'customers', 'priority': 2},
            {'name': 'contacts', 'table': 'contacts', 'priority': 3},
            {'name': 'stocks', 'table': 'stocks', 'priority': 4},
            {'name': 'products', 'table': 'products', 'priority': 5}
        ]
        
        endpoints.sort(key=lambda x: x['priority'])
        
        for endpoint in endpoints:
            try:
                logger.info(f"   🔄 Processing {endpoint['name']} (Priority {endpoint['priority']})...")
                
                # Extract với production limits
                raw_data = self.misa_extractor.extract_all_data_from_endpoint(
                    endpoint['name'], 
                    max_pages=self.config.MISA_MAX_PAGES_PER_CYCLE
                )
                
                if not raw_data:
                    logger.info(f"   ⚠️ No new data for {endpoint['name']}")
                    misa_results[endpoint['name']] = {
                        'status': 'no_data',
                        'records': 0,
                        'extracted': 0,
                        'transformed': 0
                    }
                    continue
                
                # Transform
                if endpoint['name'] == 'sale_orders':
                    transformed_data = self.misa_transformer.transform_sale_orders_flattened(raw_data)
                else:
                    transform_method = getattr(self.misa_transformer, f'transform_{endpoint["name"]}')
                    transformed_data = transform_method(raw_data)
                
                # Load
                df = pd.DataFrame(transformed_data)
                success = self.misa_loader.load_dataframe_to_staging(df, endpoint['table'])
                
                if success:
                    records_count = len(df)
                    misa_results[endpoint['name']] = {
                        'status': 'success',
                        'records': records_count,
                        'extracted': len(raw_data),
                        'transformed': len(df)
                    }
                    logger.info(f"   ✅ {endpoint['name']}: {records_count} records processed successfully")
                else:
                    misa_results[endpoint['name']] = {
                        'status': 'duplicate_skipped',
                        'records': 0,
                        'extracted': len(raw_data),
                        'transformed': len(df)
                    }
                    logger.warning(f"   ⚠️ {endpoint['name']}: Load failed (likely duplicates)")
                    
            except Exception as e:
                logger.error(f"   ❌ {endpoint['name']} processing error: {e}")
                misa_results[endpoint['name']] = {
                    'status': 'error',
                    'error': str(e),
                    'records': 0
                }
        
        return misa_results
    
    def _process_tiktok_shop_data(self) -> Dict[str, Any]:
        """Process TikTok Shop data với production settings"""
        
        try:
            # Extract với production buffer
            raw_orders = self.tiktok_extractor.extract_recent_orders(
                days_back=self.config.TIKTOK_DAYS_BACK_BUFFER
            )
            
            if not raw_orders:
                logger.info("   ⚠️ No new TikTok Shop orders")
                return {
                    'orders': {
                        'status': 'no_data',
                        'records': 0,
                        'extracted': 0,
                        'transformed': 0
                    }
                }
            
            # Transform
            transformed_df = self.tiktok_transformer.transform_orders_to_dataframe(raw_orders)
            
            # Load với deduplication
            success = self.tiktok_loader.load_incremental_orders(transformed_df)
            
            if success:
                records_count = len(transformed_df)
                logger.info(f"   ✅ TikTok Shop: {records_count} records processed successfully")
                return {
                    'orders': {
                        'status': 'success',
                        'records': records_count,
                        'extracted': len(raw_orders),
                        'transformed': len(transformed_df)
                    }
                }
            else:
                logger.warning(f"   ⚠️ TikTok Shop: Load failed")
                return {
                    'orders': {
                        'status': 'load_failed',
                        'records': 0,
                        'extracted': len(raw_orders),
                        'transformed': len(transformed_df)
                    }
                }
                
        except Exception as e:
            logger.error(f"   ❌ TikTok Shop processing error: {e}")
            return {
                'orders': {
                    'status': 'error',
                    'error': str(e),
                    'records': 0
                }
            }
    
    def _verify_data_quality(self) -> Dict[str, Any]:
        """Comprehensive data quality verification"""
        
        try:
            import pyodbc
            from config.settings import settings
            
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
            
            # Check all staging tables
            tables = [
                'misa_customers', 'misa_sale_orders_flattened', 'misa_contacts',
                'misa_stocks', 'misa_products', 'tiktok_shop_order_detail'
            ]
            
            total_records = 0
            tables_with_data = 0
            table_counts = {}
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM staging.{table}")
                count = cursor.fetchone()[0]
                total_records += count
                table_counts[table] = count
                
                if count > 0:
                    tables_with_data += 1
                
                logger.info(f"   📊 {table}: {count:,} rows")
            
            cursor.close()
            connection.close()
            
            quality_score = (tables_with_data / len(tables)) * 100
            quality_passed = tables_with_data >= 5  # At least 5/6 tables should have data
            
            logger.info(f"   📈 Data Quality Score: {quality_score:.1f}%")
            logger.info(f"   📊 Total records: {total_records:,}")
            logger.info(f"   ✅ Quality check: {'PASSED' if quality_passed else 'FAILED'}")
            
            return {
                'total_records': total_records,
                'tables_with_data': tables_with_data,
                'table_counts': table_counts,
                'quality_score': quality_score,
                'quality_check_passed': quality_passed
            }
            
        except Exception as e:
            logger.error(f"   ❌ Data quality verification failed: {e}")
            return {
                'error': str(e),
                'quality_check_passed': False
            }
    
    def _calculate_total_records(self, cycle_results: Dict[str, Any]) -> int:
        """Calculate total records processed in cycle"""
        
        total = 0
        
        # MISA CRM records
        for endpoint_result in cycle_results.get('misa_crm', {}).values():
            if isinstance(endpoint_result, dict):
                total += endpoint_result.get('records', 0)
        
        # TikTok Shop records
        tiktok_result = cycle_results.get('tiktok_shop', {}).get('orders', {})
        if isinstance(tiktok_result, dict):
            total += tiktok_result.get('records', 0)
        
        return total
    
    def _determine_cycle_success(self, cycle_results: Dict[str, Any]) -> bool:
        """Determine if cycle was successful"""
        
        # Check for critical errors
        if cycle_results.get('errors'):
            return False
        
        # Check data quality
        if not cycle_results.get('data_quality', {}).get('quality_check_passed', False):
            return False
        
        # Check if at least one system processed data successfully
        misa_success = any(
            result.get('status') == 'success' 
            for result in cycle_results.get('misa_crm', {}).values()
            if isinstance(result, dict)
        )
        
        tiktok_success = (
            cycle_results.get('tiktok_shop', {}).get('orders', {}).get('status') == 'success'
        )
        
        return misa_success or tiktok_success
    
    def _finalize_cycle_results(self, cycle_results: Dict[str, Any]) -> Dict[str, Any]:
        """Finalize cycle results và record metrics"""
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        cycle_results['end_time'] = end_time
        cycle_results['duration_seconds'] = duration
        
        # Log summary
        logger.info(f"\n📊 PRODUCTION CYCLE #{self.cycle_count} SUMMARY:")
        logger.info(f"   ⏱️  Duration: {duration:.1f} seconds")
        logger.info(f"   📊 Records processed: {cycle_results['total_records']}")
        logger.info(f"   🏢 MISA CRM endpoints: {len([k for k, v in cycle_results.get('misa_crm', {}).items() if isinstance(v, dict) and v.get('status') == 'success'])}/5 successful")
        logger.info(f"   🛒 TikTok Shop status: {cycle_results.get('tiktok_shop', {}).get('orders', {}).get('status', 'unknown')}")
        logger.info(f"   ✅ Overall success: {cycle_results['success']}")
        
        # Record metrics với monitoring system
        metrics = self.monitor.record_cycle_metrics(cycle_results)
        cycle_results['metrics'] = metrics
        
        return cycle_results

# Global orchestrator instance
production_orchestrator = ProductionETLOrchestrator()
