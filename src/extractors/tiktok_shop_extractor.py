"""
Trích xuất dữ liệu từ TikTok Shop API
Dựa trên implementation từ notebook đã hoạt động (Steps 6-8)
"""

import requests
import time
import json
import pandas as pd
import logging
import sys
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.auth import TikTokAuthenticator
from src.utils.logging import setup_logging
from config.settings import settings

logger = setup_logging("tiktok_shop_extractor")

class TikTokShopOrderExtractor:
    """Trích xuất dữ liệu đơn hàng từ TikTok Shop API"""
    
    def __init__(self):
        self.auth = TikTokAuthenticator()
        self.base_url = "https://open-api.tiktokglobalshop.com"
        
    def search_orders_for_ids(self, 
                             start_time: int, 
                             end_time: int,
                             order_status: Optional[str] = None,
                             page_size: int = 50) -> List[str]:
        """
        Tìm kiếm ID đơn hàng trong khoảng thời gian
        
        Args:
            start_time: Timestamp bắt đầu
            end_time: Timestamp kết thúc
            order_status: Lọc theo trạng thái đơn hàng (tùy chọn)
            page_size: Kích thước trang cho phân trang
            
        Returns:
            Danh sách ID đơn hàng
        """
        try:
            # Đảm bảo token hợp lệ
            if not self.auth.ensure_valid_token():
                logger.error("Không thể đảm bảo token xác thực hợp lệ")
                return []
                
            all_order_ids = []
            cursor = ""
            has_more = True
            
            while has_more:
                url = f"{self.base_url}/order/202309/orders/search"
                
                params = {
                    'app_key': self.auth.app_key,
                    'timestamp': str(int(time.time())),
                    'access_token': self.auth.access_token,
                    'shop_cipher': self.auth.shop_cipher,
                    'version': '202309',
                    'create_time_from': str(start_time),
                    'create_time_to': str(end_time),
                    'page_size': str(page_size),
                    'sort_field': 'create_time',
                    'sort_order': 'ASC'
                }
                
                if cursor:
                    params['cursor'] = cursor
                    
                if order_status:
                    params['order_status'] = order_status
                
                # Tạo chữ ký
                signature = self.auth.generate_signature('/order/202309/orders/search', params)
                params['sign'] = signature
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('code') == 0:
                        orders = data.get('data', {}).get('orders', [])
                        order_ids = [order['order_id'] for order in orders]
                        all_order_ids.extend(order_ids)
                        
                        # Kiểm tra phân trang
                        has_more = data.get('data', {}).get('has_more', False)
                        cursor = data.get('data', {}).get('cursor', '')
                        
                        logger.info(f"Đã lấy {len(order_ids)} ID đơn hàng, tổng: {len(all_order_ids)}")
                        
                        # Giới hạn tốc độ
                        time.sleep(0.1)
                    else:
                        logger.error(f"Lỗi API trong search_orders: {data.get('message')}")
                        break
                else:
                    logger.error(f"Lỗi HTTP trong search_orders: {response.status_code}")
                    break
                    
            logger.info(f"Tổng số ID đơn hàng đã lấy: {len(all_order_ids)}")
            return all_order_ids
            
        except Exception as e:
            logger.error(f"Ngoại lệ trong search_orders_for_ids: {str(e)}")
            return []
            
    def get_order_details_with_ids(self, order_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get detailed order information for a list of order IDs
        
        Args:
            order_ids: List of order IDs to fetch details for
            
        Returns:
            List of order detail dictionaries
        """
        try:
            if not self.auth.ensure_valid_token():
                logger.error("Failed to ensure valid authentication token")
                return []
                
            # TikTok API supports up to 50 order IDs per request
            batch_size = 50
            all_orders = []
            
            for i in range(0, len(order_ids), batch_size):
                batch_ids = order_ids[i:i + batch_size]
                
                url = f"{self.base_url}/order/202309/orders"
                
                params = {
                    'app_key': self.auth.app_key,
                    'timestamp': str(int(time.time())),
                    'access_token': self.auth.access_token,
                    'shop_cipher': self.auth.shop_cipher,
                    'version': '202309',
                    'order_id_list': ','.join(batch_ids)
                }
                
                # Generate signature
                signature = self.auth.generate_signature('/order/202309/orders', params)
                params['sign'] = signature
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('code') == 0:
                        orders = data.get('data', {}).get('order_list', [])
                        all_orders.extend(orders)
                        
                        logger.info(f"Retrieved details for batch {i//batch_size + 1}, "
                                  f"orders: {len(orders)}, total: {len(all_orders)}")
                        
                        # Rate limiting
                        time.sleep(0.2)
                    else:
                        logger.error(f"API error in get_order_details: {data.get('message')}")
                else:
                    logger.error(f"HTTP error in get_order_details: {response.status_code}")
                    
            logger.info(f"Total order details retrieved: {len(all_orders)}")
            return all_orders
            
        except Exception as e:
            logger.error(f"Exception in get_order_details_with_ids: {str(e)}")
            return []
            
    def extract_orders_for_period(self, 
                                 start_date: datetime, 
                                 end_date: datetime) -> List[Dict[str, Any]]:
        """
        Extract all orders for a specific time period
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of order dictionaries
        """
        try:
            # Convert to timestamps
            start_timestamp = int(start_date.timestamp())
            end_timestamp = int(end_date.timestamp())
            
            logger.info(f"Extracting orders from {start_date} to {end_date}")
            
            # Step 1: Search for order IDs
            order_ids = self.search_orders_for_ids(start_timestamp, end_timestamp)
            
            if not order_ids:
                logger.warning("No order IDs found for the specified period")
                return []
                
            # Step 2: Get detailed order information
            orders = self.get_order_details_with_ids(order_ids)
            
            logger.info(f"Successfully extracted {len(orders)} orders")
            return orders
            
        except Exception as e:
            logger.error(f"Exception in extract_orders_for_period: {str(e)}")
            return []
            
    def extract_recent_orders(self, days_back: int = 1) -> List[Dict[str, Any]]:
        """
        Extract orders from recent days
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of order dictionaries
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        return self.extract_orders_for_period(start_date, end_date)
        
    def test_api_connection(self) -> bool:
        """
        Kiểm tra kết nối API và xác thực
        
        Returns:
            True nếu kết nối thành công, False nếu ngược lại
        """
        try:
            logger.info("Đang kiểm tra kết nối TikTok API...")
            
            # Kiểm tra xác thực và lấy shop cipher
            if self.auth.ensure_valid_token():
                logger.info("✓ Xác thực thành công")
                logger.info(f"✓ Shop cipher: {self.auth.shop_cipher}")
                
                # Kiểm tra một API call đơn giản
                test_start = datetime.now() - timedelta(days=1)
                test_end = datetime.now()
                
                order_ids = self.search_orders_for_ids(
                    int(test_start.timestamp()),
                    int(test_end.timestamp()),
                    page_size=1
                )
                
                logger.info(f"✓ API call thành công, tìm thấy {len(order_ids)} đơn hàng trong 24h qua")
                return True
            else:
                logger.error("✗ Xác thực thất bại")
                return False
                
        except Exception as e:
            logger.error(f"✗ Kiểm tra kết nối API thất bại: {str(e)}")
            return False
