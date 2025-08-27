#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MISA CRM Data Extractor
Tích hợp với TikTok Shop Infrastructure - Cấu trúc src/
"""

import requests
import jwt
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import sys
import os

# Import shared utilities
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging(__name__)

class MISACRMExtractor:
    """
    MISA CRM Data Extractor - Tương tự TikTok Shop Extractor pattern
    """
    
    def __init__(self):
        """Khởi tạo MISA CRM Extractor"""
        self.credentials = settings.get_data_source_credentials('misa_crm')
        self.client_id = self.credentials['client_id']
        self.client_secret = self.credentials['client_secret']
        self.base_url = self.credentials['base_url']
        
        self.access_token = None
        self.token_expires_at = None
        
        # Endpoint configuration
        self.endpoints = {
            'customers': '/Customers',
            'sale_orders': '/SaleOrders',
            'contacts': '/Contacts',
            'stocks': '/Stocks',
            'products': '/Products'
        }
        
        logger.info(f"Khởi tạo MISA CRM Extractor cho {settings.company_name}")
    
    def _decode_token_expiry(self, token: str) -> Optional[datetime]:
        """Decode JWT token để lấy thời gian hết hạn"""
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            exp_timestamp = decoded.get('exp')
            
            if exp_timestamp:
                return datetime.fromtimestamp(exp_timestamp)
            else:
                logger.warning("Token không có thời gian hết hạn, sử dụng mặc định 1 giờ")
                return datetime.now() + timedelta(hours=1)
                
        except Exception as e:
            logger.warning(f"Không thể decode token: {e}, sử dụng mặc định 1 giờ")
            return datetime.now() + timedelta(hours=1)
    
    def _is_token_expired(self) -> bool:
        """Kiểm tra token có hết hạn không"""
        if not self.access_token or not self.token_expires_at:
            return True
        
        buffer_time = datetime.now() + timedelta(seconds=settings.misa_crm_token_refresh_buffer)
        return buffer_time >= self.token_expires_at
    
    def get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        """Lấy access token với khả năng auto refresh"""
        if not force_refresh and not self._is_token_expired():
            logger.debug("Sử dụng token hiện tại còn hạn")
            return self.access_token
        
        logger.info("Đang yêu cầu access token mới từ MISA CRM API")
        
        url = f"{self.base_url}/Account"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        headers = {"Content-Type": "application/json"}
        
        try:
            response = requests.post(url, json=data, headers=headers, timeout=settings.api_timeout)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success') and 'data' in result:
                    self.access_token = result['data']
                    self.token_expires_at = self._decode_token_expiry(self.access_token)
                    
                    logger.info(f"Lấy token thành công, hết hạn lúc: {self.token_expires_at}")
                    return self.access_token
                else:
                    logger.error(f"Định dạng response không hợp lệ: {result}")
                    return None
            else:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Exception khi yêu cầu token: {str(e)}")
            return None
    
    def _get_headers(self) -> Dict[str, str]:
        """Lấy headers cho API requests với token validation"""
        token = self.get_access_token()
        if not token:
            raise ValueError("Không thể lấy access token hợp lệ")
        
        return {
            "Authorization": f"Bearer {token}",
            "Clientid": self.client_id,
            "Content-Type": "application/json"
        }
    
    def _make_request_with_retry(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Thực hiện HTTP request với retry logic và token refresh"""
        for attempt in range(settings.api_retry_attempts):
            try:
                headers = self._get_headers()
                kwargs['headers'] = headers
                kwargs['timeout'] = kwargs.get('timeout', settings.api_timeout)
                
                response = requests.request(method, url, **kwargs)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 401:
                    logger.warning(f"Lần thử {attempt + 1}: Token hết hạn, refresh và thử lại")
                    self.get_access_token(force_refresh=True)
                    if attempt < settings.api_retry_attempts - 1:
                        time.sleep(settings.api_retry_delay)
                        continue
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    return None
                    
            except Exception as e:
                logger.error(f"Lần thử {attempt + 1} thất bại: {str(e)}")
                if attempt < settings.api_retry_attempts - 1:
                    time.sleep(settings.api_retry_delay * (attempt + 1))
                    continue
                else:
                    logger.error("Tất cả lần thử đều thất bại")
                    return None
        
        return None
    
    def extract_endpoint_data(self, endpoint_name: str, page: int = 0, page_size: int = None, **params) -> Optional[Dict]:
        """
        Lấy dữ liệu từ MISA CRM endpoint
        
        Args:
            endpoint_name: Tên endpoint ('customers', 'sale_orders', etc.)
            page: Số trang (bắt đầu từ 0)
            page_size: Số bản ghi trên trang
            **params: Tham số bổ sung
            
        Returns:
            Dữ liệu response hoặc None nếu thất bại
        """
        if endpoint_name not in self.endpoints:
            logger.error(f"Endpoint không hợp lệ: {endpoint_name}")
            return None
        
        if page_size is None:
            page_size = settings.misa_crm_page_size
        
        url = f"{self.base_url}{self.endpoints[endpoint_name]}"
        
        # Setup parameters
        request_params = {
            "page": page,
            "pageSize": min(page_size, settings.misa_crm_max_page_size)
        }
        request_params.update(params)
        
        # Xử lý đặc biệt cho stocks (không có phân trang)
        if endpoint_name == 'stocks':
            request_params = {}
        
        logger.debug(f"Đang yêu cầu dữ liệu {endpoint_name} (trang {page}, kích thước {page_size})")
        
        response = self._make_request_with_retry('GET', url, params=request_params)
        
        if response:
            result = response.json()
            logger.debug(f"Lấy dữ liệu {endpoint_name} thành công")
            return result
        else:
            logger.error(f"Thất bại khi lấy dữ liệu {endpoint_name}")
            return None
    
    def extract_all_data_from_endpoint(self, endpoint_name: str, max_pages: int = None) -> List[Dict]:
        """
        Lấy tất cả dữ liệu từ endpoint với phân trang
        
        Args:
            endpoint_name: Tên endpoint
            max_pages: Số trang tối đa (None = không giới hạn)
            
        Returns:
            Danh sách tất cả records
        """
        all_data = []
        page = 0
        
        logger.info(f"Bắt đầu lấy dữ liệu bulk từ {endpoint_name}")
        
        while True:
            if max_pages and page >= max_pages:
                logger.info(f"Đã đạt giới hạn trang tối đa: {max_pages}")
                break
            
            result = self.extract_endpoint_data(endpoint_name, page=page, page_size=settings.misa_crm_page_size)
            
            if not result or 'data' not in result or not result['data']:
                logger.info(f"Không còn dữ liệu ở trang {page}")
                break
            
            batch_data = result['data']
            all_data.extend(batch_data)
            
            logger.debug(f"Đã lấy {len(batch_data)} records từ trang {page} (tổng: {len(all_data)})")
            
            # Kiểm tra trang cuối
            if len(batch_data) < settings.misa_crm_page_size:
                logger.info(f"Đã đến trang cuối: {page}")
                break
            
            page += 1
            time.sleep(settings.etl_rate_limit_delay)  # Rate limiting
        
        logger.info(f"Hoàn thành lấy dữ liệu bulk: {len(all_data)} tổng records từ {endpoint_name}")
        return all_data
    
    def extract_incremental_data(self, endpoint_name: str, lookback_hours: int = None) -> List[Dict]:
        """
        Lấy dữ liệu incremental (chỉ dữ liệu mới/thay đổi)
        
        Args:
            endpoint_name: Tên endpoint
            lookback_hours: Số giờ nhìn lại (None = sử dụng config mặc định)
            
        Returns:
            Danh sách records đã thay đổi
        """
        if lookback_hours is None:
            lookback_hours = settings.misa_crm_incremental_lookback_hours
        
        logger.info(f"Lấy dữ liệu incremental từ {endpoint_name} (lookback: {lookback_hours} giờ)")
        
        # Lấy tất cả dữ liệu (với giới hạn trang để tránh quá tải)
        all_data = self.extract_all_data_from_endpoint(endpoint_name, max_pages=10)
        
        if not all_data:
            return []
        
        # Lọc theo modified_date
        cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
        filtered_data = []
        
        for record in all_data:
            modified_date_str = record.get('modified_date')
            if modified_date_str:
                try:
                    modified_date = datetime.fromisoformat(modified_date_str.replace('Z', '+00:00'))
                    if modified_date >= cutoff_time:
                        filtered_data.append(record)
                except:
                    # Bao gồm record nếu không parse được date
                    filtered_data.append(record)
            else:
                # Bao gồm record nếu không có modified_date
                filtered_data.append(record)
        
        logger.info(f"Lọc incremental: {len(filtered_data)}/{len(all_data)} records")
        return filtered_data
    
    def health_check(self) -> Dict[str, Any]:
        """
        Thực hiện health check trên MISA CRM API
        
        Returns:
            Kết quả health check
        """
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'token_status': 'unknown',
            'endpoints_status': {},
            'overall_status': 'unknown'
        }
        
        try:
            # Kiểm tra token
            token = self.get_access_token()
            if token:
                health_status['token_status'] = 'healthy'
                health_status['token_expires_at'] = self.token_expires_at.isoformat() if self.token_expires_at else None
            else:
                health_status['token_status'] = 'unhealthy'
                health_status['overall_status'] = 'unhealthy'
                return health_status
            
            # Kiểm tra từng endpoint
            for endpoint_name in self.endpoints.keys():
                try:
                    result = self.extract_endpoint_data(endpoint_name, page=0, page_size=1)
                    if result and 'data' in result:
                        health_status['endpoints_status'][endpoint_name] = 'healthy'
                    else:
                        health_status['endpoints_status'][endpoint_name] = 'unhealthy'
                except Exception as e:
                    health_status['endpoints_status'][endpoint_name] = f'error: {str(e)}'
            
            # Trạng thái tổng thể
            unhealthy_endpoints = [k for k, v in health_status['endpoints_status'].items() if v != 'healthy']
            if not unhealthy_endpoints:
                health_status['overall_status'] = 'healthy'
            else:
                health_status['overall_status'] = f'degraded: {unhealthy_endpoints}'
            
        except Exception as e:
            health_status['overall_status'] = f'error: {str(e)}'
        
        logger.info(f"Health check hoàn thành: {health_status['overall_status']}")
        return health_status
