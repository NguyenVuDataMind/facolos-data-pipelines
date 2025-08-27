#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MISA CRM Data Transformer
Tích hợp với TikTok Shop Infrastructure - Cấu trúc src/
"""

import pandas as pd
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

class MISACRMTransformer:
    """
    MISA CRM Data Transformer - Tương tự TikTok Shop Transformer pattern
    """
    
    def __init__(self):
        """Khởi tạo MISA CRM Transformer"""
        self.batch_id = None
        logger.info(f"Khởi tạo MISA CRM Transformer cho {settings.company_name}")
    
    def set_batch_id(self, batch_id: str):
        """Set batch ID cho transformation session"""
        self.batch_id = batch_id
        logger.info(f"Set batch ID: {batch_id}")
    
    def _add_etl_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Thêm ETL metadata vào DataFrame - tương tự TikTok Shop pattern
        
        Args:
            df: DataFrame cần thêm metadata
            
        Returns:
            DataFrame với ETL metadata
        """
        df = df.copy()
        df['etl_batch_id'] = self.batch_id
        df['etl_created_at'] = datetime.now()
        df['etl_updated_at'] = datetime.now()
        df['etl_source'] = 'misa_crm_api'
        
        return df
    
    def transform_customers(self, customers_data: List[Dict]) -> pd.DataFrame:
        """
        Transform customers data
        
        Args:
            customers_data: Raw customers data từ API
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transform {len(customers_data)} customers...")
        
        if not customers_data:
            logger.warning("Không có dữ liệu customers để transform")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(customers_data)
        
        # Data type conversions
        numeric_columns = [
            'annual_revenue', 'debt', 'debt_limit', 'number_of_days_owed',
            'number_orders', 'order_sales', 'average_order_value',
            'average_number_of_days_between_purchases', 'number_days_without_purchase',
            'billing_long', 'billing_lat', 'shipping_long', 'shipping_lat', 'total_score'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Date columns
        date_columns = [
            'purchase_date_recent', 'purchase_date_first', 'customer_since_date',
            'last_interaction_date', 'last_visit_date', 'last_call_date',
            'issued_on', 'celebrate_date', 'created_date', 'modified_date', 'last_modified_date'
        ]
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Boolean columns
        boolean_columns = [
            'is_personal', 'inactive', 'is_public', 'is_distributor', 'is_portal_access'
        ]
        
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool, errors='ignore')
        
        # Add ETL metadata
        df = self._add_etl_metadata(df)
        
        logger.info(f"Transform customers hoàn thành: {len(df)} records")
        return df
    
    def transform_sale_orders_flattened(self, sale_orders_data: List[Dict]) -> pd.DataFrame:
        """
        Transform sale orders với nested data flattening
        
        Args:
            sale_orders_data: Raw sale orders data từ API
            
        Returns:
            Flattened DataFrame với mỗi dòng là 1 order item
        """
        logger.info(f"Transform và flatten {len(sale_orders_data)} sale orders...")
        
        if not sale_orders_data:
            logger.warning("Không có dữ liệu sale orders để transform")
            return pd.DataFrame()
        
        flattened_rows = []
        
        for order in sale_orders_data:
            # Tách dữ liệu chính của order (loại bỏ nested data)
            order_main = {k: v for k, v in order.items() if k != 'sale_order_product_mappings'}
            
            # Lấy nested product mappings
            product_mappings = order.get('sale_order_product_mappings', [])
            
            if product_mappings:
                # Tạo một dòng cho mỗi product item
                for item in product_mappings:
                    flattened_row = {}
                    
                    # Thêm thông tin chính của order (prefix với order_)
                    for key, value in order_main.items():
                        flattened_row[f"order_{key}"] = value
                    
                    # Thêm thông tin chi tiết của item (prefix với item_)
                    for key, value in item.items():
                        flattened_row[f"item_{key}"] = value
                    
                    # Thêm metadata
                    flattened_row['has_multiple_items'] = len(product_mappings) > 1
                    flattened_row['total_items_in_order'] = len(product_mappings)
                    
                    flattened_rows.append(flattened_row)
            else:
                # Order không có items - tạo dòng với item fields = NULL
                flattened_row = {}
                
                # Thêm thông tin chính của order
                for key, value in order_main.items():
                    flattened_row[f"order_{key}"] = value
                
                # Thêm item fields với giá trị NULL
                item_fields = [
                    'id', 'product_code', 'unit', 'price', 'amount', 'total',
                    'tax_percent', 'discount_percent', 'stock_name', 'description'
                ]
                for field in item_fields:
                    flattened_row[f"item_{field}"] = None
                
                # Thêm metadata
                flattened_row['has_multiple_items'] = False
                flattened_row['total_items_in_order'] = 0
                
                flattened_rows.append(flattened_row)
        
        df = pd.DataFrame(flattened_rows)
        
        if df.empty:
            logger.warning("DataFrame rỗng sau khi flatten")
            return df
        
        # Data type conversions cho order fields
        order_numeric_columns = [
            'order_sale_order_amount', 'order_total_summary', 'order_tax_summary',
            'order_discount_summary', 'order_to_currency_summary', 'order_total_receipted_amount',
            'order_balance_receipt_amount', 'order_invoiced_amount', 'order_un_invoiced_amount',
            'order_exchange_rate'
        ]
        
        for col in order_numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Data type conversions cho item fields
        item_numeric_columns = [
            'item_price', 'item_amount', 'item_usage_unit_amount', 'item_usage_unit_price',
            'item_total', 'item_to_currency', 'item_discount', 'item_tax',
            'item_discount_percent', 'item_price_after_tax', 'item_price_after_discount',
            'item_to_currency_after_discount', 'item_height', 'item_width', 'item_length',
            'item_radius', 'item_mass', 'item_exist_amount', 'item_shipping_amount',
            'item_ratio', 'item_custom_field1', 'item_produced_quantity', 'item_quantity_ordered'
        ]
        
        for col in item_numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Date columns
        order_date_columns = [
            'order_sale_order_date', 'order_due_date', 'order_book_date',
            'order_deadline_date', 'order_delivery_date', 'order_paid_date',
            'order_invoice_date', 'order_production_date'
        ]
        
        for col in order_date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        item_date_columns = ['item_expire_date']
        
        for col in item_date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Boolean columns
        boolean_columns = ['order_is_use_currency', 'item_is_promotion']
        
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool, errors='ignore')
        
        # Add ETL metadata
        df = self._add_etl_metadata(df)
        
        logger.info(f"Transform sale orders flattened hoàn thành: {len(df)} rows (từ {len(sale_orders_data)} orders)")
        return df
    
    def transform_contacts(self, contacts_data: List[Dict]) -> pd.DataFrame:
        """Transform contacts data"""
        logger.info(f"Transform {len(contacts_data)} contacts...")
        
        if not contacts_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(contacts_data)
        
        # Data type conversions
        numeric_columns = [
            'mailing_long', 'mailing_lat', 'shipping_long', 'shipping_lat',
            'total_score', 'number_days_not_interacted'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Date columns
        date_columns = [
            'date_of_birth', 'customer_since_date', 'last_interaction_date',
            'last_visit_date', 'last_call_date', 'created_date', 'modified_date'
        ]
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Boolean columns
        boolean_columns = ['email_opt_out', 'phone_opt_out', 'inactive', 'is_public']
        
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool, errors='ignore')
        
        df = self._add_etl_metadata(df)
        
        logger.info(f"Transform contacts hoàn thành: {len(df)} records")
        return df
    
    def transform_stocks(self, stocks_data: List[Dict]) -> pd.DataFrame:
        """Transform stocks data"""
        logger.info(f"Transform {len(stocks_data)} stocks...")
        
        if not stocks_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(stocks_data)
        
        # Date columns
        date_columns = ['created_date', 'modified_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Boolean columns
        boolean_columns = ['inactive']
        
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool, errors='ignore')
        
        df = self._add_etl_metadata(df)
        
        logger.info(f"Transform stocks hoàn thành: {len(df)} records")
        return df
    
    def transform_products(self, products_data: List[Dict]) -> pd.DataFrame:
        """Transform products data"""
        logger.info(f"Transform {len(products_data)} products...")
        
        if not products_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(products_data)
        
        # Data type conversions
        numeric_columns = [
            'unit_price', 'purchased_price', 'unit_cost', 'unit_price1',
            'unit_price2', 'unit_price_fixed'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Date columns
        date_columns = ['created_date', 'modified_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Boolean columns
        boolean_columns = [
            'price_after_tax', 'is_use_tax', 'is_follow_serial_number',
            'is_set_product', 'inactive', 'is_public'
        ]
        
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool, errors='ignore')
        
        df = self._add_etl_metadata(df)
        
        logger.info(f"Transform products hoàn thành: {len(df)} records")
        return df
    
    def transform_all_endpoints(self, raw_data: Dict[str, List[Dict]], batch_id: str) -> Dict[str, pd.DataFrame]:
        """
        Transform tất cả endpoint data
        
        Args:
            raw_data: Dict với key là endpoint name, value là raw data
            batch_id: Batch ID cho session này
            
        Returns:
            Dict với key là endpoint name, value là transformed DataFrame
        """
        self.set_batch_id(batch_id)
        
        logger.info("Bắt đầu transform tất cả endpoint data...")
        
        transformed_data = {}
        
        # Transform từng endpoint
        if 'customers' in raw_data and raw_data['customers']:
            transformed_data['customers'] = self.transform_customers(raw_data['customers'])
        
        if 'sale_orders' in raw_data and raw_data['sale_orders']:
            transformed_data['sale_orders_flattened'] = self.transform_sale_orders_flattened(raw_data['sale_orders'])
        
        if 'contacts' in raw_data and raw_data['contacts']:
            transformed_data['contacts'] = self.transform_contacts(raw_data['contacts'])
        
        if 'stocks' in raw_data and raw_data['stocks']:
            transformed_data['stocks'] = self.transform_stocks(raw_data['stocks'])
        
        if 'products' in raw_data and raw_data['products']:
            transformed_data['products'] = self.transform_products(raw_data['products'])
        
        # Summary
        total_rows = sum(len(df) for df in transformed_data.values())
        logger.info(f"Transform hoàn thành tất cả endpoints: {total_rows} tổng rows")
        
        return transformed_data
    
    def validate_flattened_data(self, flattened_df: pd.DataFrame, original_orders: List[Dict]) -> Dict[str, Any]:
        """
        Validate flattened sale orders data
        
        Args:
            flattened_df: Flattened DataFrame
            original_orders: Original orders data
            
        Returns:
            Dict với validation results
        """
        logger.info("Đang validate flattened data...")
        
        validation_results = {
            'total_original_orders': len(original_orders),
            'total_flattened_rows': len(flattened_df),
            'unique_orders_in_flattened': 0,
            'orders_with_multiple_items': 0,
            'orders_without_items': 0,
            'total_items_original': 0,
            'total_items_flattened': 0,
            'validation_passed': False
        }
        
        if flattened_df.empty:
            logger.warning("Flattened DataFrame rỗng")
            return validation_results
        
        validation_results['unique_orders_in_flattened'] = flattened_df['order_id'].nunique()
        
        # Count items trong original data
        for order in original_orders:
            items = order.get('sale_order_product_mappings', [])
            validation_results['total_items_original'] += len(items)
            
            if len(items) > 1:
                validation_results['orders_with_multiple_items'] += 1
            elif len(items) == 0:
                validation_results['orders_without_items'] += 1
        
        # Count items trong flattened data
        validation_results['total_items_flattened'] = len(flattened_df[flattened_df['item_id'].notna()])
        
        # Validation checks
        checks = [
            validation_results['unique_orders_in_flattened'] == validation_results['total_original_orders'],
            validation_results['total_items_flattened'] == validation_results['total_items_original']
        ]
        
        validation_results['validation_passed'] = all(checks)
        
        logger.info(f"📊 Validation Results:")
        logger.info(f"   Original orders: {validation_results['total_original_orders']}")
        logger.info(f"   Flattened rows: {validation_results['total_flattened_rows']}")
        logger.info(f"   Unique orders: {validation_results['unique_orders_in_flattened']}")
        logger.info(f"   Original items: {validation_results['total_items_original']}")
        logger.info(f"   Flattened items: {validation_results['total_items_flattened']}")
        logger.info(f"   Validation: {'✅ PASSED' if validation_results['validation_passed'] else '❌ FAILED'}")
        
        return validation_results
