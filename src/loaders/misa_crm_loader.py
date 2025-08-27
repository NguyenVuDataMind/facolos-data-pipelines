#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MISA CRM Data Loader
Tích hợp với TikTok Shop Infrastructure - Cấu trúc src/
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import sys
import os

# Import shared utilities
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging(__name__)

class MISACRMLoader:
    """
    MISA CRM Data Loader - Tương tự TikTok Shop Loader pattern
    """
    
    def __init__(self):
        """Khởi tạo MISA CRM Loader"""
        self.db_engine = create_engine(settings.sql_server_connection_string)
        
        # Table mapping
        self.table_mappings = {
            'customers': settings.get_misa_crm_table_full_name('customers'),
            'sale_orders_flattened': settings.get_misa_crm_table_full_name('sale_orders_flattened'),
            'contacts': settings.get_misa_crm_table_full_name('contacts'),
            'stocks': settings.get_misa_crm_table_full_name('stocks'),
            'products': settings.get_misa_crm_table_full_name('products')
        }
        
        logger.info(f"Khởi tạo MISA CRM Loader cho {settings.company_name}")
        logger.info(f"Database: {settings.sql_server_host}")
    
    def _get_table_info(self, table_full_name: str) -> Dict[str, Any]:
        """
        Lấy thông tin về table (schema, table name)
        
        Args:
            table_full_name: Tên đầy đủ của table (schema.table)
            
        Returns:
            Dict chứa schema và table name
        """
        parts = table_full_name.split('.')
        if len(parts) == 2:
            return {'schema': parts[0], 'table': parts[1]}
        else:
            return {'schema': 'staging', 'table': table_full_name}
    
    def truncate_table(self, endpoint: str) -> bool:
        """
        Truncate staging table cho endpoint
        
        Args:
            endpoint: Tên endpoint
            
        Returns:
            True nếu thành công
        """
        if endpoint not in self.table_mappings:
            logger.error(f"Không tìm thấy table mapping cho endpoint: {endpoint}")
            return False
        
        table_full_name = self.table_mappings[endpoint]
        
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_full_name}"))
                conn.commit()
            
            logger.info(f"✅ Truncated table {table_full_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi khi truncate table {table_full_name}: {e}")
            return False
    
    def load_dataframe_to_staging(self, df: pd.DataFrame, endpoint: str, 
                                 if_exists: str = 'append') -> bool:
        """
        Load DataFrame vào staging table
        
        Args:
            df: DataFrame cần load
            endpoint: Tên endpoint
            if_exists: Hành động nếu table đã tồn tại ('append', 'replace', 'fail')
            
        Returns:
            True nếu thành công
        """
        if df.empty:
            logger.warning(f"DataFrame rỗng cho endpoint {endpoint}")
            return True
        
        if endpoint not in self.table_mappings:
            logger.error(f"Không tìm thấy table mapping cho endpoint: {endpoint}")
            return False
        
        table_full_name = self.table_mappings[endpoint]
        table_info = self._get_table_info(table_full_name)
        
        try:
            # Load data using pandas to_sql
            df.to_sql(
                name=table_info['table'],
                con=self.db_engine,
                schema=table_info['schema'],
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=settings.misa_crm_etl_batch_size
            )
            
            logger.info(f"✅ Loaded {len(df)} records to {table_full_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi khi load data vào {table_full_name}: {e}")
            # Try alternative loading method for all tables (SQLAlchemy engine issue)
            logger.info(f"🔄 Trying alternative pyodbc loading method for {endpoint}...")
            return self._load_with_pyodbc(df, table_full_name)

    def _load_with_pyodbc(self, df: pd.DataFrame, table_full_name: str) -> bool:
        """
        Alternative loading method using pyodbc for composite key tables
        """
        try:
            import pyodbc

            # Create connection string for pyodbc
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

            # Get table info
            table_info = self._get_table_info(table_full_name)
            schema = table_info['schema']
            table = table_info['table']

            # Get table columns (excluding computed columns)
            cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table}'
                AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsComputed') = 0
                ORDER BY ORDINAL_POSITION
            """)

            db_columns = [row.COLUMN_NAME for row in cursor.fetchall()]

            # Match DataFrame columns with database columns
            matching_columns = [col for col in db_columns if col in df.columns]

            if not matching_columns:
                logger.error(f"❌ No matching columns found between DataFrame and {table_full_name}")
                return False

            # Prepare insert statement
            placeholders = ', '.join(['?' for _ in matching_columns])
            insert_sql = f"INSERT INTO {schema}.{table} ({', '.join(matching_columns)}) VALUES ({placeholders})"

            # Insert data in batches
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                batch_data = []

                for _, row in batch_df.iterrows():
                    row_data = []
                    for col in matching_columns:
                        value = row[col] if col in row else None
                        # Handle NaN values
                        if pd.isna(value):
                            row_data.append(None)
                        else:
                            row_data.append(value)
                    batch_data.append(row_data)

                # Execute batch insert
                cursor.executemany(insert_sql, batch_data)
                connection.commit()
                total_inserted += len(batch_data)

                logger.info(f"   Inserted batch {i//batch_size + 1}: {len(batch_data)} rows")

            cursor.close()
            connection.close()

            logger.info(f"✅ Successfully loaded {total_inserted} records to {table_full_name} using pyodbc")
            return True

        except Exception as e:
            logger.error(f"❌ pyodbc loading failed for {table_full_name}: {e}")
            return False

    def load_all_data_to_staging(self, transformed_data: Dict[str, pd.DataFrame],
                                truncate_first: bool = False) -> Dict[str, int]:
        """
        Load tất cả transformed data vào staging tables
        
        Args:
            transformed_data: Dict với key là endpoint name, value là DataFrame
            truncate_first: Có truncate tables trước khi load không
            
        Returns:
            Dict với số records đã load cho mỗi endpoint
        """
        logger.info("Bắt đầu load tất cả data vào staging tables...")
        
        loaded_counts = {}
        
        for endpoint, df in transformed_data.items():
            if df.empty:
                logger.warning(f"DataFrame rỗng cho {endpoint}, bỏ qua")
                loaded_counts[endpoint] = 0
                continue
            
            try:
                # Truncate table nếu được yêu cầu
                if truncate_first:
                    self.truncate_table(endpoint)
                
                # Load data
                success = self.load_dataframe_to_staging(df, endpoint, if_exists='append')
                
                if success:
                    loaded_counts[endpoint] = len(df)
                    logger.info(f"✅ {endpoint}: {len(df)} records loaded")
                else:
                    loaded_counts[endpoint] = 0
                    logger.error(f"❌ {endpoint}: Load thất bại")
                
            except Exception as e:
                logger.error(f"❌ Exception khi load {endpoint}: {e}")
                loaded_counts[endpoint] = 0
        
        total_loaded = sum(loaded_counts.values())
        logger.info(f"✅ Load hoàn thành: {total_loaded} tổng records")
        
        return loaded_counts
    
    def validate_loaded_data(self, loaded_counts: Dict[str, int]) -> Dict[str, Any]:
        """
        Validate dữ liệu đã load vào staging tables
        
        Args:
            loaded_counts: Dict với số records đã load
            
        Returns:
            Dict với validation results
        """
        logger.info("Đang validate dữ liệu đã load...")
        
        validation_results = {
            'total_expected_records': sum(loaded_counts.values()),
            'total_actual_records': 0,
            'table_validations': {},
            'validation_passed': True
        }
        
        for endpoint, expected_count in loaded_counts.items():
            if endpoint not in self.table_mappings:
                continue
            
            table_full_name = self.table_mappings[endpoint]
            
            try:
                with self.db_engine.connect() as conn:
                    # Count records in table
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_full_name}"))
                    actual_count = result.fetchone()[0]
                    
                    # Check latest ETL batch
                    result = conn.execute(text(f"SELECT MAX(etl_created_at) FROM {table_full_name}"))
                    latest_etl_time = result.fetchone()[0]
                    
                    table_validation = {
                        'expected_count': expected_count,
                        'actual_count': actual_count,
                        'count_match': actual_count >= expected_count,  # Allow for existing data
                        'latest_etl_time': latest_etl_time,
                        'has_recent_data': latest_etl_time and (datetime.now() - latest_etl_time).total_seconds() < 3600  # Within 1 hour
                    }
                    
                    validation_results['table_validations'][endpoint] = table_validation
                    validation_results['total_actual_records'] += actual_count
                    
                    if not table_validation['count_match'] or not table_validation['has_recent_data']:
                        validation_results['validation_passed'] = False
                    
                    logger.info(f"📊 {endpoint}: Expected {expected_count}, Actual {actual_count}, Latest ETL: {latest_etl_time}")
                    
            except Exception as e:
                logger.error(f"❌ Lỗi khi validate {endpoint}: {e}")
                validation_results['validation_passed'] = False
                validation_results['table_validations'][endpoint] = {
                    'error': str(e)
                }
        
        logger.info(f"📊 Validation tổng thể: {'✅ PASSED' if validation_results['validation_passed'] else '❌ FAILED'}")
        
        return validation_results
    
    def get_staging_data_summary(self) -> Dict[str, Any]:
        """
        Lấy tóm tắt dữ liệu trong staging tables
        
        Returns:
            Dict với thông tin tóm tắt
        """
        logger.info("Đang lấy tóm tắt dữ liệu staging...")
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'tables': {},
            'total_records': 0
        }
        
        for endpoint, table_full_name in self.table_mappings.items():
            try:
                with self.db_engine.connect() as conn:
                    # Basic counts
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_full_name}"))
                    total_count = result.fetchone()[0]
                    
                    # Latest ETL info
                    result = conn.execute(text(f"""
                        SELECT 
                            MAX(etl_created_at) as latest_etl,
                            COUNT(DISTINCT etl_batch_id) as batch_count
                        FROM {table_full_name}
                    """))
                    etl_info = result.fetchone()
                    
                    # Recent data (last 24 hours)
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) 
                        FROM {table_full_name} 
                        WHERE etl_created_at >= DATEADD(day, -1, GETDATE())
                    """))
                    recent_count = result.fetchone()[0]
                    
                    table_summary = {
                        'total_records': total_count,
                        'recent_records_24h': recent_count,
                        'latest_etl_time': etl_info[0],
                        'total_batches': etl_info[1]
                    }
                    
                    summary['tables'][endpoint] = table_summary
                    summary['total_records'] += total_count
                    
                    logger.info(f"📊 {endpoint}: {total_count} records, {recent_count} recent")
                    
            except Exception as e:
                logger.error(f"❌ Lỗi khi lấy summary cho {endpoint}: {e}")
                summary['tables'][endpoint] = {'error': str(e)}
        
        logger.info(f"📊 Tổng records trong staging: {summary['total_records']}")
        
        return summary
    
    def cleanup_old_data(self, retention_days: int = None) -> Dict[str, int]:
        """
        Cleanup dữ liệu cũ trong staging tables
        
        Args:
            retention_days: Số ngày giữ lại dữ liệu (None = sử dụng config)
            
        Returns:
            Dict với số records đã xóa
        """
        if retention_days is None:
            retention_days = settings.misa_crm_data_retention_days
        
        logger.info(f"Đang cleanup dữ liệu cũ hơn {retention_days} ngày...")
        
        deleted_counts = {}
        
        for endpoint, table_full_name in self.table_mappings.items():
            try:
                with self.db_engine.connect() as conn:
                    # Delete old data
                    result = conn.execute(text(f"""
                        DELETE FROM {table_full_name} 
                        WHERE etl_created_at < DATEADD(day, -{retention_days}, GETDATE())
                    """))
                    
                    deleted_count = result.rowcount
                    deleted_counts[endpoint] = deleted_count
                    
                    conn.commit()
                    
                    if deleted_count > 0:
                        logger.info(f"🗑️ {endpoint}: Đã xóa {deleted_count} records cũ")
                    else:
                        logger.info(f"✅ {endpoint}: Không có dữ liệu cũ cần xóa")
                    
            except Exception as e:
                logger.error(f"❌ Lỗi khi cleanup {endpoint}: {e}")
                deleted_counts[endpoint] = 0
        
        total_deleted = sum(deleted_counts.values())
        logger.info(f"🗑️ Cleanup hoàn thành: {total_deleted} tổng records đã xóa")
        
        return deleted_counts
    
    def test_database_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            True nếu connection thành công
        """
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                test_value = result.fetchone()[0]
                
                if test_value == 1:
                    logger.info("✅ Database connection test thành công")
                    return True
                else:
                    logger.error("❌ Database connection test thất bại")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Database connection error: {e}")
            return False
