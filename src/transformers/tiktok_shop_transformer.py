"""
Chuyển đổi dữ liệu cho dữ liệu đơn hàng TikTok Shop
Thực hiện cấu trúc phẳng (Option B) từ notebook
"""

import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
from src.utils.logging import setup_logging

logger = setup_logging("tiktok_shop_transformer")

class TikTokShopOrderTransformer:
    """Chuyển đổi dữ liệu đơn hàng TikTok Shop thành định dạng staging phẳng"""
    
    def __init__(self):
        self.batch_id = str(uuid.uuid4())
        
    def transform_orders_to_dataframe(self, orders: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Chuyển đổi danh sách từ điển đơn hàng thành DataFrame phẳng
        
        Args:
            orders: Danh sách từ điển đơn hàng từ API
            
        Returns:
            DataFrame phẳng sẵn sàng để tải vào database
        """
        try:
            if not orders:
                logger.warning("Không có đơn hàng để chuyển đổi")
                return pd.DataFrame()
                
            flattened_rows = []
            
            for order in orders:
                # Trích xuất thông tin cấp đơn hàng
                order_info = self._extract_order_info(order)
                
                # Trích xuất thông tin người nhận
                recipient_info = self._extract_recipient_info(order)
                
                # Trích xuất line items (làm phẳng mỗi item)
                line_items = order.get('line_items', [])
                
                if not line_items:
                    # Nếu không có line items, tạo một hàng chỉ với thông tin đơn hàng
                    row = {**order_info, **recipient_info}
                    row.update(self._get_empty_item_fields())
                    flattened_rows.append(row)
                else:
                    # Tạo một hàng cho mỗi line item
                    for item in line_items:
                        item_info = self._extract_item_info(item)
                        
                        row = {**order_info, **recipient_info, **item_info}
                        flattened_rows.append(row)
                        
            # Chuyển đổi thành DataFrame
            df = pd.DataFrame(flattened_rows)
            
            # Thêm metadata ETL
            df = self._add_etl_metadata(df)
            
            logger.info(f"Đã chuyển đổi {len(orders)} đơn hàng thành {len(df)} hàng")
            return df
            
        except Exception as e:
            logger.error(f"Lỗi chuyển đổi đơn hàng thành DataFrame: {str(e)}")
            return pd.DataFrame()
            
    def _extract_order_info(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Trích xuất thông tin cấp đơn hàng"""
        order_amount = order.get('order_amount', {})
        
        return {
            'order_id': order.get('order_id'),
            'order_status': order.get('order_status'),
            'buyer_message': order.get('buyer_message'),
            'cancel_reason': order.get('cancel_reason'),
            'cancel_user': order.get('cancel_user'),
            'collection_time': order.get('collection_time'),
            'create_time': order.get('create_time'),
            'delivery_due_time': order.get('delivery_due_time'),
            'delivery_time': order.get('delivery_time'),
            'fulfillment_type': order.get('fulfillment_type'),
            'order_line_type': order.get('order_line_type'),
            'payment_method': order.get('payment_method'),
            'payment_method_name': order.get('payment_method_name'),
            'remark': order.get('remark'),
            'request_cancel_reason': order.get('request_cancel_reason'),
            'split_or_combine_tag': order.get('split_or_combine_tag'),
            'update_time': order.get('update_time'),
            'warehouse_id': order.get('warehouse_id'),
            
            # Số tiền đơn hàng
            'currency': order_amount.get('currency'),
            'original_shipping_fee': self._safe_float(order_amount.get('original_shipping_fee')),
            'original_total_product_price': self._safe_float(order_amount.get('original_total_product_price')),
            'seller_discount': self._safe_float(order_amount.get('seller_discount')),
            'shipping_fee': self._safe_float(order_amount.get('shipping_fee')),
            'shipping_fee_platform_discount': self._safe_float(order_amount.get('shipping_fee_platform_discount')),
            'shipping_fee_seller_discount': self._safe_float(order_amount.get('shipping_fee_seller_discount')),
            'subtotal_after_seller_discounts': self._safe_float(order_amount.get('subtotal_after_seller_discounts')),
            'tax_amount': self._safe_float(order_amount.get('tax_amount')),
            'total_amount': self._safe_float(order_amount.get('total_amount'))
        }
        
    def _extract_recipient_info(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Trích xuất thông tin người nhận/giao hàng"""
        recipient = order.get('recipient_address', {})
        
        return {
            'recipient_address_detail': recipient.get('detail'),
            'recipient_address_region_code': recipient.get('region_code'),
            'recipient_address_state': recipient.get('state'),
            'recipient_address_city': recipient.get('city'),
            'recipient_address_town': recipient.get('town'),
            'recipient_address_district': recipient.get('district'),
            'recipient_address_zipcode': recipient.get('zipcode'),
            'recipient_name': recipient.get('name'),
            'recipient_phone': recipient.get('phone'),
            'recipient_phone_number': recipient.get('phone_number')
        }
        
    def _extract_item_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Trích xuất thông tin line item"""
        sku_info = item.get('sku_info', {})
        sales_attributes = sku_info.get('sales_attributes', [])
        
        # Chuyển đổi sales attributes thành chuỗi JSON
        sales_attributes_json = json.dumps(sales_attributes) if sales_attributes else None
        
        return {
            'item_id': item.get('product_id'),
            'item_name': item.get('product_name'),
            'item_sku_id': item.get('sku_id'),
            'item_sku_image': sku_info.get('sku_image'),
            'item_sku_name': sku_info.get('sku_name'),
            'item_quantity': self._safe_int(item.get('quantity')),
            'item_unit_price': self._safe_float(item.get('unit_price')),
            'item_currency': item.get('currency'),
            'item_is_gift': item.get('is_gift'),
            'item_platform_discount': self._safe_float(item.get('platform_discount')),
            'item_seller_discount': self._safe_float(item.get('seller_discount')),
            'item_sku_sales_attributes': sales_attributes_json
        }
        
    def _get_empty_item_fields(self) -> Dict[str, Any]:
        """Lấy các trường item trống cho đơn hàng không có line items"""
        return {
            'item_id': None,
            'item_name': None,
            'item_sku_id': None,
            'item_sku_image': None,
            'item_sku_name': None,
            'item_quantity': None,
            'item_unit_price': None,
            'item_currency': None,
            'item_is_gift': None,
            'item_platform_discount': None,
            'item_seller_discount': None,
            'item_sku_sales_attributes': None
        }
        
    def _add_etl_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ETL metadata columns"""
        now = datetime.utcnow()
        
        df['etl_batch_id'] = self.batch_id
        df['etl_created_at'] = now
        df['etl_updated_at'] = now
        
        return df
        
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
            
    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None or value == '':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
            
    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate transformed DataFrame
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty")
                return False
                
            # Check required columns
            required_columns = ['order_id', 'etl_batch_id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False
                
            # Check for null order_ids
            null_order_ids = df['order_id'].isnull().sum()
            if null_order_ids > 0:
                logger.warning(f"Found {null_order_ids} rows with null order_id")
                
            # Log data quality metrics
            total_rows = len(df)
            unique_orders = df['order_id'].nunique()
            
            logger.info(f"DataFrame validation: {total_rows} rows, {unique_orders} unique orders")
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating DataFrame: {str(e)}")
            return False
