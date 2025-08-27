#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Historical Backfill Script with UPSERT Logic
Backfill dữ liệu lịch sử từ 01/07/2024 với khả năng handle duplicates
"""

import sys
import os
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Any
import pyodbc

# Add project root to Python path
sys.path.append('.')

from config.settings import settings
from src.extractors.misa_crm_extractor import MISACRMExtractor
from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
from src.transformers.misa_crm_transformer import MISACRMTransformer
from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
from src.utils.logging import setup_logging

logger = setup_logging("historical_backfill")

class HistoricalBackfillOrchestrator:
    """
    Orchestrates historical backfill với UPSERT logic
    """
    
    def __init__(self, start_date: str = "2024-07-01", batch_days: int = 30):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = date.today()
        self.batch_days = batch_days
        
        # Calculate total days and batches
        self.total_days = (self.end_date - self.start_date).days
        self.total_batches = (self.total_days // batch_days) + 1
        
        # Initialize components
        self.misa_extractor = None
        self.misa_transformer = None
        self.tiktok_extractor = None
        self.tiktok_transformer = None
        
        # Database connection
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={settings.sql_server_host},{settings.sql_server_port};"
            f"DATABASE={settings.sql_server_database};"
            f"UID={settings.sql_server_username};"
            f"PWD={settings.sql_server_password};"
            f"TrustServerCertificate=yes"
        )
        
        # Results tracking
        self.results = {
            'batches_completed': 0,
            'total_records_processed': 0,
            'misa_crm_records': 0,
            'tiktok_shop_records': 0,
            'errors': []
        }
    
    def initialize_components(self) -> bool:
        """Initialize all ETL components"""
        try:
            logger.info("🔧 Initializing ETL components...")
            
            # MISA CRM components
            self.misa_extractor = MISACRMExtractor()
            self.misa_transformer = MISACRMTransformer()
            
            # TikTok Shop components
            self.tiktok_extractor = TikTokShopOrderExtractor()
            self.tiktok_transformer = TikTokShopOrderTransformer()
            
            logger.info("✅ All ETL components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize components: {e}")
            return False
    
    def upsert_misa_data(self, df: pd.DataFrame, table_name: str, primary_keys: List[str]) -> bool:
        """
        UPSERT MISA CRM data với proper duplicate handling
        """
        try:
            connection = pyodbc.connect(self.connection_string)
            cursor = connection.cursor()
            
            # Get table columns
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = 'staging' 
                AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            
            db_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
            matching_columns = [col for col in db_columns if col in df.columns]
            
            if not matching_columns:
                logger.error(f"❌ No matching columns for {table_name}")
                return False
            
            # Build MERGE statement for UPSERT
            merge_sql = self._build_merge_statement(table_name, matching_columns, primary_keys)
            
            # Prepare data for MERGE
            inserted_count = 0
            updated_count = 0
            
            for _, row in df.iterrows():
                try:
                    # Prepare values
                    values = []
                    for col in matching_columns:
                        value = row[col] if col in row else None
                        if pd.isna(value):
                            values.append(None)
                        else:
                            values.append(value)
                    
                    # Execute MERGE
                    cursor.execute(merge_sql, values + values)  # values twice for INSERT and UPDATE
                    result = cursor.fetchone()
                    
                    if result and result[0] == 'INSERT':
                        inserted_count += 1
                    elif result and result[0] == 'UPDATE':
                        updated_count += 1
                        
                except Exception as e:
                    logger.warning(f"⚠️ Row merge failed: {str(e)[:100]}...")
                    continue
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"   ✅ UPSERT completed: {inserted_count} inserted, {updated_count} updated")
            return True
            
        except Exception as e:
            logger.error(f"❌ UPSERT failed for {table_name}: {e}")
            return False
    
    def _build_merge_statement(self, table_name: str, columns: List[str], primary_keys: List[str]) -> str:
        """Build SQL MERGE statement for UPSERT"""
        
        # Build column lists
        column_list = ', '.join(columns)
        value_placeholders = ', '.join(['?' for _ in columns])
        
        # Build ON condition for primary keys
        on_conditions = []
        for pk in primary_keys:
            if pk in columns:
                on_conditions.append(f"target.{pk} = source.{pk}")
        
        on_clause = ' AND '.join(on_conditions) if on_conditions else "1=0"
        
        # Build UPDATE SET clause (exclude primary keys)
        update_columns = [col for col in columns if col not in primary_keys]
        update_set = ', '.join([f"{col} = source.{col}" for col in update_columns])
        
        merge_sql = f"""
        MERGE staging.{table_name} AS target
        USING (SELECT {value_placeholders} AS ({column_list})) AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set}, etl_updated_at = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT ({column_list})
            VALUES ({', '.join([f'source.{col}' for col in columns])})
        OUTPUT $action;
        """
        
        return merge_sql
    
    def process_batch(self, batch_start: date, batch_end: date, batch_num: int) -> Dict[str, Any]:
        """Process một batch của historical data"""
        logger.info(f"\n📦 BATCH {batch_num}/{self.total_batches}")
        logger.info(f"📅 Period: {batch_start.strftime('%Y-%m-%d')} to {batch_end.strftime('%Y-%m-%d')}")
        logger.info("-" * 60)
        
        batch_results = {
            'batch_num': batch_num,
            'start_date': batch_start,
            'end_date': batch_end,
            'misa_success': 0,
            'tiktok_success': False,
            'records_processed': 0
        }
        
        try:
            # MISA CRM Processing
            logger.info("🏢 Processing MISA CRM data...")
            
            misa_endpoints = [
                {'name': 'customers', 'table': 'misa_customers', 'pk': ['id']},
                {'name': 'sale_orders', 'table': 'misa_sale_orders_flattened', 'pk': ['order_id', 'item_id']},
                {'name': 'contacts', 'table': 'misa_contacts', 'pk': ['id']},
                {'name': 'stocks', 'table': 'misa_stocks', 'pk': ['stock_code']},
                {'name': 'products', 'table': 'misa_products', 'pk': ['id']}
            ]
            
            for endpoint in misa_endpoints:
                try:
                    logger.info(f"   🔄 Processing {endpoint['name']}...")
                    
                    # Extract (limited pages for batch processing)
                    raw_data = self.misa_extractor.extract_all_data_from_endpoint(
                        endpoint['name'], 
                        max_pages=5  # Limit for batch processing
                    )
                    
                    if not raw_data:
                        logger.info(f"   ⚠️ No data for {endpoint['name']}")
                        continue
                    
                    # Transform
                    if endpoint['name'] == 'sale_orders':
                        transformed_data = self.misa_transformer.transform_sale_orders_flattened(raw_data)
                    else:
                        transform_method = getattr(self.misa_transformer, f'transform_{endpoint["name"]}')
                        transformed_data = transform_method(raw_data)
                    
                    df = pd.DataFrame(transformed_data)
                    
                    # UPSERT
                    success = self.upsert_misa_data(df, endpoint['table'], endpoint['pk'])
                    
                    if success:
                        batch_results['misa_success'] += 1
                        batch_results['records_processed'] += len(df)
                        logger.info(f"   ✅ {endpoint['name']}: {len(df)} records processed")
                    else:
                        logger.error(f"   ❌ {endpoint['name']}: Failed")
                        
                except Exception as e:
                    logger.error(f"   ❌ {endpoint['name']} error: {e}")
                    continue
            
            # TikTok Shop Processing
            logger.info("\n🛒 Processing TikTok Shop data...")
            try:
                days_in_batch = (batch_end - batch_start).days
                raw_orders = self.tiktok_extractor.extract_recent_orders(days_back=days_in_batch)
                
                if raw_orders:
                    transformed_df = self.tiktok_transformer.transform_orders_to_dataframe(raw_orders)
                    
                    # Use existing TikTok loader (has deduplication)
                    from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
                    tiktok_loader = TikTokShopOrderLoader()
                    success = tiktok_loader.load_incremental_orders(transformed_df)
                    
                    if success:
                        batch_results['tiktok_success'] = True
                        batch_results['records_processed'] += len(transformed_df)
                        logger.info(f"   ✅ TikTok Shop: {len(transformed_df)} records processed")
                    else:
                        logger.error(f"   ❌ TikTok Shop: Failed")
                else:
                    logger.info("   ⚠️ No TikTok Shop data for this period")
                    
            except Exception as e:
                logger.error(f"   ❌ TikTok Shop error: {e}")
            
            return batch_results
            
        except Exception as e:
            logger.error(f"❌ Batch {batch_num} failed: {e}")
            batch_results['error'] = str(e)
            return batch_results
    
    def run_historical_backfill(self) -> bool:
        """Run complete historical backfill"""
        logger.info("🚀 STARTING HISTORICAL BACKFILL")
        logger.info("=" * 70)
        logger.info(f"📅 Period: {self.start_date} to {self.end_date}")
        logger.info(f"📊 Total days: {self.total_days}")
        logger.info(f"📦 Batch size: {self.batch_days} days")
        logger.info(f"📦 Total batches: {self.total_batches}")
        logger.info("")
        
        try:
            # Initialize components
            if not self.initialize_components():
                return False
            
            # Process batches
            current_date = self.start_date
            
            for batch_num in range(1, self.total_batches + 1):
                batch_end = min(current_date + timedelta(days=self.batch_days), self.end_date)
                
                batch_result = self.process_batch(current_date, batch_end, batch_num)
                
                # Update results
                self.results['batches_completed'] += 1
                self.results['total_records_processed'] += batch_result.get('records_processed', 0)
                
                # Move to next batch
                current_date = batch_end + timedelta(days=1)
                
                if current_date > self.end_date:
                    break
            
            # Final summary
            self.generate_final_summary()
            return True
            
        except Exception as e:
            logger.error(f"❌ Historical backfill failed: {e}")
            return False
    
    def generate_final_summary(self):
        """Generate final summary"""
        logger.info("\n" + "="*70)
        logger.info("📊 HISTORICAL BACKFILL SUMMARY")
        logger.info("="*70)
        logger.info(f"📦 Batches completed: {self.results['batches_completed']}/{self.total_batches}")
        logger.info(f"📊 Total records processed: {self.results['total_records_processed']:,}")
        logger.info(f"📅 Period covered: {self.start_date} to {self.end_date}")
        logger.info(f"🎯 Success rate: {(self.results['batches_completed']/self.total_batches)*100:.1f}%")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Historical Backfill from 01/07/2024')
    parser.add_argument('--start-date', default='2024-07-01', 
                       help='Start date (YYYY-MM-DD, default: 2024-07-01)')
    parser.add_argument('--batch-days', type=int, default=30, 
                       help='Days per batch (default: 30)')
    parser.add_argument('--test', action='store_true', 
                       help='Test mode with smaller batches')
    
    args = parser.parse_args()
    
    if args.test:
        batch_days = 7
        logger.info("🧪 Running in TEST MODE")
    else:
        batch_days = args.batch_days
    
    orchestrator = HistoricalBackfillOrchestrator(
        start_date=args.start_date,
        batch_days=batch_days
    )
    
    success = orchestrator.run_historical_backfill()
    
    if success:
        print("\n" + "="*70)
        print("🎉 SUCCESS: Historical backfill completed!")
        print("📊 All historical data processed")
        print("🚀 Ready for incremental ETL setup")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("❌ FAILED: Historical backfill encountered errors")
        print("🔧 Please check logs and retry")
        print("="*70)
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
