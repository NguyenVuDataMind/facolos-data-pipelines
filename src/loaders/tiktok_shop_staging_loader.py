"""
Database loader cho dữ liệu đơn hàng TikTok Shop
Tải dữ liệu đã chuyển đổi vào bảng staging SQL Server
"""

import pandas as pd
import logging
import sys
import os
from typing import Optional, Dict, Any

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.database import db_manager
from src.utils.logging import setup_logging
from config.settings import settings

logger = setup_logging("tiktok_shop_loader")

class TikTokShopOrderLoader:
    """Tải dữ liệu đơn hàng TikTok Shop vào database staging"""
    
    def __init__(self):
        self.db = db_manager
        self.staging_schema = settings.staging_schema
        self.staging_table = settings.staging_table_orders
        
    def initialize_staging(self) -> bool:
        """
        Khởi tạo môi trường staging (schema và tables)
        
        Returns:
            True nếu thành công, False nếu ngược lại
        """
        try:
            logger.info("Đang khởi tạo môi trường staging...")
            
            # Khởi tạo kết nối database
            if not self.db.initialize():
                logger.error("Khởi tạo kết nối database thất bại")
                return False
                
            # Tạo staging schema
            if not self.db.create_staging_schema():
                logger.error("Tạo staging schema thất bại")
                return False
                
            # Thực thi script tạo bảng staging
            table_script_path = "sql/staging/create_tiktok_orders_table.sql"
            if not self.db.execute_sql_file(table_script_path):
                logger.error("Tạo bảng staging thất bại")
                return False
                
            logger.info("Khởi tạo môi trường staging thành công")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khởi tạo môi trường staging: {str(e)}")
            return False
            
    def load_orders(self, 
                   df: pd.DataFrame, 
                   load_mode: str = 'append',
                   validate_before_load: bool = True) -> bool:
        """
        Load order data into staging table
        
        Args:
            df: DataFrame with order data
            load_mode: Loading mode ('append', 'replace', 'truncate_insert')
            validate_before_load: Whether to validate data before loading
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to load")
                return True
                
            logger.info(f"Loading {len(df)} rows into staging table...")
            
            # Validate data if requested
            if validate_before_load:
                if not self._validate_dataframe(df):
                    logger.error("DataFrame validation failed")
                    return False
                    
            # Prepare DataFrame for loading
            df_prepared = self._prepare_dataframe_for_load(df)
            
            # Handle different load modes
            if load_mode == 'truncate_insert':
                # Truncate table first, then insert
                if not self.db.truncate_table(self.staging_table, self.staging_schema):
                    logger.error("Failed to truncate staging table")
                    return False
                load_mode = 'append'
            elif load_mode == 'replace':
                # This will drop and recreate the table
                pass
            elif load_mode == 'append':
                # Default append mode
                pass
            else:
                logger.error(f"Unsupported load mode: {load_mode}")
                return False
                
            # Load data into database
            success = self.db.insert_dataframe(
                df_prepared,
                self.staging_table,
                self.staging_schema,
                if_exists=load_mode
            )
            
            if success:
                logger.info(f"Successfully loaded {len(df_prepared)} rows")
                return True
            else:
                logger.error("Failed to load data into staging table")
                return False
                
        except Exception as e:
            logger.error(f"Error loading orders: {str(e)}")
            return False
            
    def load_incremental_orders(self, df: pd.DataFrame) -> bool:
        """
        Load orders incrementally (avoiding duplicates)
        
        Args:
            df: DataFrame with order data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to load")
                return True
                
            logger.info(f"Loading {len(df)} rows incrementally...")
            
            # For incremental loads, we'll use MERGE logic or check for existing records
            # For now, we'll use simple append mode and rely on database constraints
            # to handle duplicates (if any are defined)
            
            return self.load_orders(df, load_mode='append', validate_before_load=True)
            
        except Exception as e:
            logger.error(f"Error in incremental load: {str(e)}")
            return False
            
    def get_load_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about loaded data
        
        Returns:
            Dictionary with load statistics
        """
        try:
            stats = {}
            
            # Get row count
            with self.db.get_connection() as conn:
                result = conn.execute(
                    f"SELECT COUNT(*) as row_count FROM [{self.staging_schema}].[{self.staging_table}]"
                ).fetchone()
                stats['total_rows'] = result.row_count
                
                # Get unique orders count
                result = conn.execute(
                    f"SELECT COUNT(DISTINCT order_id) as unique_orders FROM [{self.staging_schema}].[{self.staging_table}]"
                ).fetchone()
                stats['unique_orders'] = result.unique_orders
                
                # Get date range
                result = conn.execute(
                    f"""
                    SELECT 
                        MIN(create_time) as min_create_time,
                        MAX(create_time) as max_create_time,
                        MIN(etl_created_at) as min_etl_time,
                        MAX(etl_created_at) as max_etl_time
                    FROM [{self.staging_schema}].[{self.staging_table}]
                    """
                ).fetchone()
                
                if result:
                    stats.update({
                        'min_create_time': result.min_create_time,
                        'max_create_time': result.max_create_time,
                        'min_etl_time': result.min_etl_time,
                        'max_etl_time': result.max_etl_time
                    })
                    
            logger.info(f"Load statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting load statistics: {str(e)}")
            return {}
            
    def _validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate DataFrame before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check required columns
            required_columns = [
                'order_id', 'etl_batch_id', 'etl_created_at', 'etl_updated_at'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False
                
            # Check for null order_ids
            null_order_ids = df['order_id'].isnull().sum()
            if null_order_ids > 0:
                logger.error(f"Found {null_order_ids} rows with null order_id")
                return False
                
            # Check data types and ranges
            numeric_columns = [
                'original_shipping_fee', 'original_total_product_price',
                'seller_discount', 'shipping_fee', 'total_amount',
                'item_quantity', 'item_unit_price'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    # Check for negative values where they shouldn't be
                    if col in ['item_quantity'] and (df[col] < 0).any():
                        logger.warning(f"Found negative values in {col}")
                        
            logger.info("DataFrame validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error validating DataFrame: {str(e)}")
            return False
            
    def _prepare_dataframe_for_load(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for database loading
        
        Args:
            df: Original DataFrame
            
        Returns:
            Prepared DataFrame
        """
        try:
            df_prepared = df.copy()
            
            # Ensure proper data types
            # Convert timestamp columns
            timestamp_columns = ['create_time', 'update_time', 'delivery_time', 
                               'collection_time', 'delivery_due_time']
            
            for col in timestamp_columns:
                if col in df_prepared.columns:
                    # TikTok timestamps are in seconds, convert to proper datetime if needed
                    pass  # Keep as integer for now, can convert later if needed
                    
            # Ensure string columns are not too long
            string_columns = {
                'order_id': 50,
                'order_status': 50,
                'currency': 10,
                'item_id': 50,
                'item_sku_id': 50,
                'item_name': 500,
                'item_sku_name': 500
            }
            
            for col, max_length in string_columns.items():
                if col in df_prepared.columns:
                    df_prepared[col] = df_prepared[col].astype(str).str[:max_length]
                    
            # Handle None/NaN values appropriately
            df_prepared = df_prepared.where(pd.notnull(df_prepared), None)
            
            return df_prepared
            
        except Exception as e:
            logger.error(f"Error preparing DataFrame: {str(e)}")
            return df
            
    def test_connection(self) -> bool:
        """
        Kiểm tra kết nối database và truy cập bảng staging
        
        Returns:
            True nếu thành công, False nếu ngược lại
        """
        try:
            logger.info("Đang kiểm tra kết nối database...")
            
            # Kiểm tra kết nối cơ bản
            if not self.db.initialize():
                logger.error("✗ Kết nối database thất bại")
                return False
                
            logger.info("✓ Kết nối database thành công")
            
            # Kiểm tra sự tồn tại của bảng
            if not self.db.table_exists(self.staging_table, self.staging_schema):
                logger.error(f"✗ Bảng staging {self.staging_schema}.{self.staging_table} không tồn tại")
                return False
                
            logger.info(f"✓ Bảng staging {self.staging_schema}.{self.staging_table} tồn tại")
            
            # Kiểm tra truy vấn thống kê
            stats = self.get_load_statistics()
            logger.info(f"✓ Thống kê bảng hiện tại: {stats}")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Kiểm tra kết nối database thất bại: {str(e)}")
            return False
