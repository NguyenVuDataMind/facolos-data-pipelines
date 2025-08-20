"""
Utility functions for TikTok Shop API authentication and signature generation
Based on the working notebook implementation
"""

import hmac
import hashlib
import time
import json
import requests
import logging
import sys
import os
from typing import Dict, Any, Optional

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.settings import settings

logger = logging.getLogger(__name__)

class TikTokAuthenticator:
    """Handle TikTok Shop API authentication and token management"""
    
    def __init__(self):
        self.app_key = settings.tiktok_app_key
        self.app_secret = settings.tiktok_app_secret
        self.access_token = settings.tiktok_access_token
        self.refresh_token = settings.tiktok_refresh_token
        self.shop_cipher = settings.tiktok_shop_cipher
        
    def generate_signature(self, path: str, params: Dict[str, Any]) -> str:
        """
        Generate HMAC-SHA256 signature for TikTok API requests
        
        Args:
            path: API endpoint path
            params: Request parameters
            
        Returns:
            Generated signature string
        """
        # Ensure timestamp is included
        if 'timestamp' not in params:
            params['timestamp'] = str(int(time.time()))
            
        # Sort parameters by key
        sorted_params = sorted(params.items())
        
        # Create query string
        query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
        
        # Create string to sign: path + query_string
        string_to_sign = path + query_string
        
        logger.debug(f"String to sign: {string_to_sign}")
        
        # Generate HMAC-SHA256 signature
        signature = hmac.new(
            self.app_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
        
    def refresh_access_token(self) -> bool:
        """
        Refresh the access token using refresh token
        
        Returns:
            True if successful, False otherwise
        """
        try:
            url = "https://open-api.tiktokglobalshop.com/authorization/202309/token/refresh"
            
            params = {
                'app_key': self.app_key,
                'timestamp': str(int(time.time())),
                'refresh_token': self.refresh_token,
                'grant_type': 'refresh_token'
            }
            
            # Generate signature
            signature = self.generate_signature('/authorization/202309/token/refresh', params)
            params['sign'] = signature
            
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(url, json=params, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 0:
                    # Update tokens
                    self.access_token = data['data']['access_token']
                    self.refresh_token = data['data']['refresh_token']
                    
                    logger.info("Access token refreshed successfully")
                    return True
                else:
                    logger.error(f"Token refresh failed: {data.get('message')}")
                    return False
            else:
                logger.error(f"Token refresh HTTP error: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Exception during token refresh: {str(e)}")
            return False
            
    def get_shop_cipher(self) -> Optional[str]:
        """
        Get shop cipher for the authenticated shop
        
        Returns:
            Shop cipher string or None if failed
        """
        try:
            url = "https://open-api.tiktokglobalshop.com/shop/202309/shops"
            
            params = {
                'app_key': self.app_key,
                'timestamp': str(int(time.time())),
                'access_token': self.access_token,
                'version': '202309'
            }
            
            # Generate signature
            signature = self.generate_signature('/shop/202309/shops', params)
            params['sign'] = signature
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 0 and data.get('data', {}).get('shops'):
                    shop_cipher = data['data']['shops'][0]['cipher']
                    self.shop_cipher = shop_cipher
                    logger.info(f"Shop cipher retrieved: {shop_cipher}")
                    return shop_cipher
                else:
                    logger.error(f"Get shop cipher failed: {data.get('message')}")
                    return None
            else:
                logger.error(f"Get shop cipher HTTP error: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Exception getting shop cipher: {str(e)}")
            return None
            
    def ensure_valid_token(self) -> bool:
        """
        Ensure we have a valid access token, refresh if necessary
        
        Returns:
            True if we have a valid token, False otherwise
        """
        # Try to get shop cipher as a test of token validity
        if self.get_shop_cipher():
            return True
            
        # If that failed, try to refresh the token
        logger.info("Token appears invalid, attempting refresh...")
        if self.refresh_access_token():
            # Test again
            return self.get_shop_cipher() is not None
            
        return False
