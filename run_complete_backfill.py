#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete Backfill Script
Thực hiện backfill dữ liệu cho tất cả 6 staging tables trong một lần chạy
- MISA CRM: 5 tables (customers, sale_orders_flattened, contacts, stocks, products)
- TikTok Shop: 1 table (tiktok_shop_order_detail)
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add project root to Python path
sys.path.append('.')

from config.settings import settings
from src.extractors.misa_crm_extractor import MISACRMExtractor
from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
from src.transformers.misa_crm_transformer import MISACRMTransformer
from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
from src.loaders.misa_crm_loader import MISACRMLoader
from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
from src.utils.logging import setup_logging

logger = setup_logging("complete_backfill")

class CompleteBackfillOrchestrator:
    """
    Orchestrates complete backfill cho tất cả 6 staging tables
    """
    
    def __init__(self, days_back: int = 30):
        self.days_back = days_back
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=days_back)
        
        # Initialize components
        self.misa_extractor = None
        self.misa_transformer = None
        self.misa_loader = None
        
        self.tiktok_extractor = None
        self.tiktok_transformer = None
        self.tiktok_loader = None
        
        # Results tracking
        self.results = {
            'misa_crm': {},
            'tiktok_shop': {},
            'summary': {}
        }
    
    def initialize_components(self) -> bool:
        """Initialize all ETL components"""
        try:
            logger.info("🔧 Initializing ETL components...")
            
            # MISA CRM components
            logger.info("   Initializing MISA CRM components...")
            self.misa_extractor = MISACRMExtractor()
            self.misa_transformer = MISACRMTransformer()
            self.misa_loader = MISACRMLoader()
            
            # TikTok Shop components
            logger.info("   Initializing TikTok Shop components...")
            self.tiktok_extractor = TikTokShopOrderExtractor()
            self.tiktok_transformer = TikTokShopOrderTransformer()
            self.tiktok_loader = TikTokShopOrderLoader()
            
            logger.info("✅ All ETL components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize components: {e}")
            return False
    
    def test_api_connections(self) -> bool:
        """Test API connections before backfill"""
        try:
            logger.info("🔌 Testing API connections...")
            
            # Test MISA CRM connection
            logger.info("   Testing MISA CRM API...")
            misa_token = self.misa_extractor.get_access_token()
            if not misa_token:
                logger.error("❌ MISA CRM API connection failed")
                return False
            logger.info("✅ MISA CRM API connection successful")
            
            # Test TikTok Shop connection
            logger.info("   Testing TikTok Shop API...")
            tiktok_health = self.tiktok_extractor.test_api_connection()
            if not tiktok_health:
                logger.error("❌ TikTok Shop API connection failed")
                return False
            logger.info("✅ TikTok Shop API connection successful")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ API connection test failed: {e}")
            return False
    
    def backfill_misa_crm_data(self) -> Dict[str, Any]:
        """Complete MISA CRM backfill for all 5 endpoints"""
        logger.info("🏢 Starting MISA CRM complete backfill...")
        logger.info(f"   Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        
        misa_results = {}
        
        # MISA CRM endpoints with priority order
        endpoints = [
            {'name': 'customers', 'table': 'customers', 'priority': 2, 'description': 'Customers'},
            {'name': 'sale_orders', 'table': 'sale_orders_flattened', 'priority': 1, 'description': 'Sale Orders (Flattened) - PRIORITY'},
            {'name': 'contacts', 'table': 'contacts', 'priority': 3, 'description': 'Contacts'},
            {'name': 'stocks', 'table': 'stocks', 'priority': 4, 'description': 'Stocks'},
            {'name': 'products', 'table': 'products', 'priority': 5, 'description': 'Products'}
        ]
        
        # Sort by priority
        endpoints.sort(key=lambda x: x['priority'])
        
        for endpoint in endpoints:
            endpoint_name = endpoint['name']
            table_name = endpoint['table']
            
            try:
                logger.info(f"\n📋 Priority {endpoint['priority']}: {endpoint['description']}")
                logger.info(f"📊 Processing MISA CRM {endpoint_name} → staging.misa_{table_name}")
                logger.info("-" * 60)
                
                # STEP 1: EXTRACT
                logger.info(f"   🔄 Extracting {endpoint_name}...")
                raw_data = self.misa_extractor.extract_all_data_from_endpoint(endpoint_name, max_pages=3)
                
                if not raw_data:
                    logger.warning(f"⚠️ No data extracted for {endpoint_name}")
                    misa_results[endpoint_name] = {'status': 'no_data', 'records': 0}
                    continue
                
                logger.info(f"   ✅ Extracted {len(raw_data):,} {endpoint_name} records")
                
                # STEP 2: TRANSFORM
                logger.info(f"   🔄 Transforming {endpoint_name}...")
                
                if endpoint_name == 'sale_orders':
                    # Use flattened transformer for sale orders
                    transformed_data = self.misa_transformer.transform_sale_orders_flattened(raw_data)
                else:
                    # Use regular transformer
                    transform_method = getattr(self.misa_transformer, f'transform_{endpoint_name}')
                    transformed_data = transform_method(raw_data)
                
                # Convert to DataFrame
                df = pd.DataFrame(transformed_data)
                logger.info(f"   ✅ Transformed to {len(df):,} records")
                
                # STEP 3: LOAD
                logger.info(f"   🔄 Loading to staging.misa_{table_name}...")
                success = self.misa_loader.load_dataframe_to_staging(df, table_name)
                
                if success:
                    records_loaded = len(df)
                    logger.info(f"   ✅ {endpoint_name}: {records_loaded:,} records loaded successfully")
                    misa_results[endpoint_name] = {
                        'status': 'success',
                        'extracted': len(raw_data),
                        'transformed': len(df),
                        'loaded': records_loaded
                    }
                else:
                    logger.error(f"   ❌ {endpoint_name}: Load failed")
                    misa_results[endpoint_name] = {
                        'status': 'load_failed',
                        'extracted': len(raw_data),
                        'transformed': len(df)
                    }
                
            except Exception as e:
                logger.error(f"❌ Error processing {endpoint_name}: {e}")
                misa_results[endpoint_name] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return misa_results
    
    def backfill_tiktok_shop_data(self) -> Dict[str, Any]:
        """Complete TikTok Shop backfill"""
        logger.info("\n🛒 Starting TikTok Shop complete backfill...")
        logger.info(f"   Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        logger.info("-" * 60)
        
        try:
            # STEP 1: EXTRACT
            logger.info("   🔄 Extracting TikTok Shop orders...")
            raw_orders = self.tiktok_extractor.extract_recent_orders(days_back=self.days_back)
            
            if not raw_orders:
                logger.warning("⚠️ No TikTok Shop orders extracted")
                return {'status': 'no_data', 'records': 0}
            
            logger.info(f"   ✅ Extracted {len(raw_orders):,} TikTok Shop orders")
            
            # STEP 2: TRANSFORM
            logger.info("   🔄 Transforming TikTok Shop orders...")
            transformed_df = self.tiktok_transformer.transform_orders_to_dataframe(raw_orders)
            logger.info(f"   ✅ Transformed to {len(transformed_df):,} records")
            
            # STEP 3: LOAD
            logger.info("   🔄 Loading to staging.tiktok_shop_order_detail...")
            success = self.tiktok_loader.load_incremental_orders(transformed_df)
            
            if success:
                records_loaded = len(transformed_df)
                logger.info(f"   ✅ TikTok Shop: {records_loaded:,} records loaded successfully")
                return {
                    'status': 'success',
                    'extracted': len(raw_orders),
                    'transformed': len(transformed_df),
                    'loaded': records_loaded
                }
            else:
                logger.error(f"   ❌ TikTok Shop: Load failed")
                return {
                    'status': 'load_failed',
                    'extracted': len(raw_orders),
                    'transformed': len(transformed_df)
                }
                
        except Exception as e:
            logger.error(f"❌ Error processing TikTok Shop data: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def verify_all_data(self) -> Dict[str, Any]:
        """Verify all data loaded to staging tables"""
        logger.info("\n🔍 Verifying all staging data...")
        logger.info("-" * 60)
        
        try:
            import pyodbc
            
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
            
            # All 6 staging tables
            tables = [
                ('misa_customers', 'MISA CRM Customers'),
                ('misa_sale_orders_flattened', 'MISA CRM Sale Orders (Flattened)'),
                ('misa_contacts', 'MISA CRM Contacts'),
                ('misa_stocks', 'MISA CRM Stocks'),
                ('misa_products', 'MISA CRM Products'),
                ('tiktok_shop_order_detail', 'TikTok Shop Order Details')
            ]
            
            total_records = 0
            tables_with_data = 0
            table_counts = {}
            
            logger.info("📊 Final Staging Tables Status:")
            
            for table, description in tables:
                cursor.execute(f"SELECT COUNT(*) FROM staging.{table}")
                count = cursor.fetchone()[0]
                total_records += count
                table_counts[table] = count
                
                if count > 0:
                    tables_with_data += 1
                    status = "✅"
                else:
                    status = "⚠️"
                
                logger.info(f"   {status} {description}: {count:,} rows")
            
            cursor.close()
            connection.close()
            
            return {
                'total_records': total_records,
                'tables_with_data': tables_with_data,
                'table_counts': table_counts,
                'success': tables_with_data >= 5  # At least 5/6 tables should have data
            }
            
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def run_complete_backfill(self) -> bool:
        """Run complete backfill for all 6 staging tables"""
        logger.info("🚀 Starting COMPLETE BACKFILL for ALL STAGING TABLES")
        logger.info("=" * 70)
        logger.info(f"📅 Backfill period: {self.days_back} days")
        logger.info(f"📅 Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        logger.info(f"🎯 Target: 6 staging tables (5 MISA CRM + 1 TikTok Shop)")
        logger.info("")
        
        try:
            # Step 1: Initialize components
            if not self.initialize_components():
                return False
            
            # Step 2: Test API connections
            if not self.test_api_connections():
                return False
            
            # Step 3: Backfill MISA CRM data (5 tables)
            logger.info("\n" + "="*70)
            logger.info("🏢 PHASE 1: MISA CRM BACKFILL (5 TABLES)")
            logger.info("="*70)
            self.results['misa_crm'] = self.backfill_misa_crm_data()
            
            # Step 4: Backfill TikTok Shop data (1 table)
            logger.info("\n" + "="*70)
            logger.info("🛒 PHASE 2: TIKTOK SHOP BACKFILL (1 TABLE)")
            logger.info("="*70)
            self.results['tiktok_shop'] = self.backfill_tiktok_shop_data()
            
            # Step 5: Verify all data
            logger.info("\n" + "="*70)
            logger.info("🔍 PHASE 3: VERIFICATION")
            logger.info("="*70)
            verification_results = self.verify_all_data()
            self.results['verification'] = verification_results
            
            # Step 6: Generate final summary
            self.generate_final_summary()
            
            return verification_results.get('success', False)
            
        except Exception as e:
            logger.error(f"❌ Complete backfill failed: {e}")
            return False

    def generate_final_summary(self):
        """Generate comprehensive final summary"""
        logger.info("\n" + "="*70)
        logger.info("📊 COMPLETE BACKFILL SUMMARY REPORT")
        logger.info("="*70)

        # MISA CRM Summary
        logger.info("🏢 MISA CRM Results (5 Tables):")
        total_misa_loaded = 0
        successful_misa = 0

        misa_priority_order = ['sale_orders', 'customers', 'contacts', 'stocks', 'products']

        for endpoint in misa_priority_order:
            if endpoint in self.results['misa_crm']:
                result = self.results['misa_crm'][endpoint]
                status = result.get('status', 'unknown')
                loaded = result.get('loaded', 0)

                if status == 'success':
                    successful_misa += 1
                    total_misa_loaded += loaded
                    priority_mark = "⭐" if endpoint == 'sale_orders' else " "
                    logger.info(f"   ✅{priority_mark} {endpoint}: {loaded:,} records loaded")
                else:
                    priority_mark = "⭐" if endpoint == 'sale_orders' else " "
                    logger.info(f"   ❌{priority_mark} {endpoint}: {status}")

        # TikTok Shop Summary
        logger.info("\n🛒 TikTok Shop Results (1 Table):")
        tiktok_result = self.results['tiktok_shop']
        tiktok_status = tiktok_result.get('status', 'unknown')
        tiktok_loaded = tiktok_result.get('loaded', 0)

        if tiktok_status == 'success':
            logger.info(f"   ✅ tiktok_shop_order_detail: {tiktok_loaded:,} records loaded")
        else:
            logger.info(f"   ❌ tiktok_shop_order_detail: {tiktok_status}")

        # Verification Summary
        verification = self.results.get('verification', {})
        total_in_db = verification.get('total_records', 0)
        tables_with_data = verification.get('tables_with_data', 0)

        logger.info(f"\n📈 OVERALL SUMMARY:")
        logger.info(f"   MISA CRM loaded: {total_misa_loaded:,} records")
        logger.info(f"   TikTok Shop loaded: {tiktok_loaded:,} records")
        logger.info(f"   Total records in database: {total_in_db:,}")
        logger.info(f"   Tables with data: {tables_with_data}/6")
        logger.info(f"   Success rate: {(tables_with_data/6)*100:.1f}%")
        logger.info(f"   MISA CRM success: {successful_misa}/5 endpoints")

        # Priority check
        sale_orders_success = self.results['misa_crm'].get('sale_orders', {}).get('status') == 'success'
        if sale_orders_success:
            logger.info("🎯 PRIORITY 1 SUCCESS: Sale Orders loaded!")

        # Final determination
        overall_success = (tables_with_data >= 5) and (total_in_db > 0)

        if overall_success:
            logger.info("\n🎉 COMPLETE BACKFILL SUCCESSFUL!")
            logger.info("🚀 All staging tables ready for production ETL")
            logger.info("🚀 Ready for Airflow DAG deployment")

            if tables_with_data == 6:
                logger.info("🌟 PERFECT: All 6 tables have data!")
        else:
            logger.info("\n⚠️ BACKFILL COMPLETED WITH ISSUES")
            logger.info("🔧 Please review and fix issues before production deployment")

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Complete Backfill for All Staging Tables')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to backfill (default: 30)')
    parser.add_argument('--test', action='store_true',
                       help='Run in test mode with limited data (max 1 page per endpoint)')
    args = parser.parse_args()

    # Adjust days for test mode
    if args.test:
        logger.info("🧪 Running in TEST MODE - limited data extraction")
        days_back = min(args.days, 7)  # Max 7 days for test
    else:
        days_back = args.days

    orchestrator = CompleteBackfillOrchestrator(days_back=days_back)
    success = orchestrator.run_complete_backfill()

    if success:
        print("\n" + "="*70)
        print("🎉 SUCCESS: Complete backfill finished successfully!")
        print("📊 All staging tables populated with data")
        print("🚀 Ready for production ETL deployment")
        print("🚀 Ready for Airflow DAG deployment")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("❌ FAILED: Backfill encountered errors")
        print("🔧 Please check logs and fix issues")
        print("="*70)

    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
