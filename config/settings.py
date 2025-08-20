"""
Cấu hình settings cho Facolos Enterprise ETL System
Quản lý tất cả các settings từ environment variables cho multiple data sources
"""
import os
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class Settings:
    """Quản lý settings tập trung cho Facolos Enterprise ETL"""
    
    def __init__(self):
        # ========================
        # ENTERPRISE SETTINGS
        # ========================
        self.company_name: str = os.getenv('COMPANY_NAME', 'Facolos')
        self.environment: str = os.getenv('ENVIRONMENT', 'development')  # development, staging, production
        
        # ========================
        # TIKTOK SHOP API CREDENTIALS
        # ========================
        self.tiktok_shop_app_key: str = os.getenv('TIKTOK_APP_KEY', '6h2cosrovhjab')
        self.tiktok_shop_app_secret: str = os.getenv('TIKTOK_APP_SECRET', '')
        self.tiktok_shop_access_token: str = os.getenv('TIKTOK_ACCESS_TOKEN', '')
        self.tiktok_shop_refresh_token: str = os.getenv('TIKTOK_REFRESH_TOKEN', '')
        self.tiktok_shop_cipher: str = os.getenv('TIKTOK_SHOP_CIPHER', '')
        
        # ========================
        # FUTURE: OTHER PLATFORM CREDENTIALS
        # ========================
        self.shopee_app_key: str = os.getenv('SHOPEE_APP_KEY', '')
        self.shopee_app_secret: str = os.getenv('SHOPEE_APP_SECRET', '')
        
        self.tiktok_ads_app_id: str = os.getenv('TIKTOK_ADS_APP_ID', '')
        self.tiktok_ads_secret: str = os.getenv('TIKTOK_ADS_SECRET', '')
        
        self.facebook_ads_app_id: str = os.getenv('FACEBOOK_ADS_APP_ID', '')
        self.facebook_ads_app_secret: str = os.getenv('FACEBOOK_ADS_APP_SECRET', '')
        
        self.google_analytics_client_id: str = os.getenv('GOOGLE_ANALYTICS_CLIENT_ID', '')
        self.google_analytics_client_secret: str = os.getenv('GOOGLE_ANALYTICS_CLIENT_SECRET', '')
        
        # ========================
        # DATABASE SETTINGS
        # ========================
        self.sql_server_host: str = os.getenv('SQL_SERVER_HOST', 'localhost')
        self.sql_server_port: int = int(os.getenv('SQL_SERVER_PORT', '1433'))
        self.sql_server_database: str = os.getenv('SQL_SERVER_DATABASE', 'Facolos_Database')
        self.sql_server_username: str = os.getenv('SQL_SERVER_USERNAME', 'sa')
        self.sql_server_password: str = os.getenv('SQL_SERVER_PASSWORD', 'FacolosDB2024!')
        
        # ========================
        # ENTERPRISE SCHEMA MAPPINGS
        # ========================
        self.schema_mappings: Dict[str, str] = {
            # E-Commerce Platforms
            'tiktok_shop': 'Facolos_TikTok_Shop',
            'shopee': 'Facolos_Shopee',
            'lazada': 'Facolos_Lazada',
            'sendo': 'Facolos_Sendo',
            
            # Advertising Platforms
            'tiktok_ads': 'Facolos_TikTok_Ads',
            'facebook_ads': 'Facolos_Facebook_Ads',
            'google_ads': 'Facolos_Google_Ads',
            
            # Analytics Platforms
            'google_analytics': 'Facolos_Google_Analytics',
            'facebook_pixel': 'Facolos_Facebook_Pixel',
            
            # CRM/ERP Systems
            'misa_crm': 'Facolos_MISA_CRM',
            'internal_erp': 'Facolos_Internal_ERP',
            
            # Data Marts
            'data_mart': 'Facolos_Data_Mart',
            'etl_control': 'Facolos_ETL_Control'
        }
        
        # ========================
        # CURRENT ACTIVE: TIKTOK SHOP SETTINGS
        # ========================
        self.tiktok_shop_schema: str = self.schema_mappings['tiktok_shop']
        self.tiktok_shop_orders_table: str = 'orders'
        self.tiktok_shop_products_table: str = 'products'  # Future
        self.tiktok_shop_customers_table: str = 'customers'  # Future
        
        # ========================
        # API SETTINGS
        # ========================
        self.tiktok_shop_api_base_url: str = os.getenv('TIKTOK_SHOP_API_BASE_URL', 'https://open-api.tiktokglobalshop.com')
        self.api_timeout: int = int(os.getenv('API_TIMEOUT', '30'))
        self.api_retry_attempts: int = int(os.getenv('API_RETRY_ATTEMPTS', '3'))
        self.api_retry_delay: int = int(os.getenv('API_RETRY_DELAY', '5'))
        
        # ========================
        # ETL SETTINGS
        # ========================
        self.etl_batch_size: int = int(os.getenv('ETL_BATCH_SIZE', '1000'))
        self.etl_page_size: int = int(os.getenv('ETL_PAGE_SIZE', '50'))
        self.etl_max_days_back: int = int(os.getenv('ETL_MAX_DAYS_BACK', '30'))
        self.etl_default_frequency_hours: int = int(os.getenv('ETL_DEFAULT_FREQUENCY_HOURS', '6'))
        
        # ========================
        # LOGGING SETTINGS
        # ========================
        self.log_level: str = os.getenv('LOG_LEVEL', 'INFO')
        self.log_format: str = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log_file_path: str = os.getenv('LOG_FILE_PATH', 'logs/facolos_enterprise_etl.log')
        
    @property
    def sql_server_connection_string(self) -> str:
        """Tạo SQL Server connection string"""
        return (
            f"mssql+pyodbc://{self.sql_server_username}:{self.sql_server_password}@"
            f"{self.sql_server_host}:{self.sql_server_port}/{self.sql_server_database}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
    
    @property
    def tiktok_shop_orders_full_table_name(self) -> str:
        """Tên đầy đủ của bảng TikTok Shop orders"""
        return f"{self.tiktok_shop_schema}.{self.tiktok_shop_orders_table}"
    
    def get_table_full_name(self, data_source: str, table_name: str) -> str:
        """
        Lấy tên đầy đủ của bảng theo data source
        
        Args:
            data_source: tên data source (vd: 'tiktok_shop', 'shopee')
            table_name: tên bảng (vd: 'orders', 'products')
            
        Returns:
            Tên đầy đủ schema.table
        """
        schema = self.schema_mappings.get(data_source)
        if not schema:
            raise ValueError(f"Không tìm thấy schema mapping cho data source: {data_source}")
        
        return f"{schema}.{table_name}"
    
    @property
    def tiktok_shop_api_headers(self) -> Dict[str, str]:
        """Headers mặc định cho TikTok Shop API requests"""
        return {
            'Content-Type': 'application/json',
            'User-Agent': f'{self.company_name}ETL/1.0'
        }
    
    def get_data_source_credentials(self, data_source: str) -> Dict[str, str]:
        """
        Lấy credentials theo data source
        
        Args:
            data_source: tên data source
            
        Returns:
            Dictionary chứa credentials
        """
        credentials_mapping = {
            'tiktok_shop': {
                'app_key': self.tiktok_shop_app_key,
                'app_secret': self.tiktok_shop_app_secret,
                'access_token': self.tiktok_shop_access_token,
                'refresh_token': self.tiktok_shop_refresh_token,
                'shop_cipher': self.tiktok_shop_cipher
            },
            'shopee': {
                'app_key': self.shopee_app_key,
                'app_secret': self.shopee_app_secret
            },
            'tiktok_ads': {
                'app_id': self.tiktok_ads_app_id,
                'secret': self.tiktok_ads_secret
            }
        }
        
        return credentials_mapping.get(data_source, {})
    
    def validate_required_settings(self) -> bool:
        """
        Xác thực các settings bắt buộc
        
        Returns:
            True nếu tất cả settings required đều có
        """
        required_settings = {
            'sql_server_password': 'SQL Server Password',
            'company_name': 'Company Name'
        }
        
        missing_settings = []
        
        for setting_key, setting_name in required_settings.items():
            if not getattr(self, setting_key):
                missing_settings.append(setting_name)
        
        if missing_settings:
            missing_list = ', '.join(missing_settings)
            raise ValueError(f"Thiếu các settings bắt buộc: {missing_list}")
        
        logger.info("Tất cả settings required đã được cấu hình")
        return True
    
    def validate_data_source_credentials(self, data_source: str) -> bool:
        """
        Xác thực credentials của một data source cụ thể
        
        Args:
            data_source: tên data source cần validate
            
        Returns:
            True nếu credentials hợp lệ
        """
        if data_source == 'tiktok_shop':
            required_fields = ['app_secret', 'access_token']
            credentials = self.get_data_source_credentials(data_source)
            
            missing_fields = [field for field in required_fields if not credentials.get(field)]
            
            if missing_fields:
                missing_list = ', '.join(missing_fields)
                raise ValueError(f"TikTok Shop thiếu credentials: {missing_list}")
                
            logger.info("TikTok Shop credentials hợp lệ")
            return True
        
        # Add validation for other data sources in the future
        logger.info(f"Validation cho {data_source} chưa được implement")
        return True
    
    def get_active_data_sources(self) -> List[str]:
        """
        Lấy danh sách data sources đang active (có credentials)
        
        Returns:
            List các data source names
        """
        active_sources = []
        
        # Check TikTok Shop
        if self.tiktok_shop_app_secret and self.tiktok_shop_access_token:
            active_sources.append('tiktok_shop')
        
        # Add checks for other data sources when implemented
        if self.shopee_app_key and self.shopee_app_secret:
            active_sources.append('shopee')
            
        if self.tiktok_ads_app_id and self.tiktok_ads_secret:
            active_sources.append('tiktok_ads')
        
        return active_sources
    
    def get_env_summary(self) -> Dict[str, Any]:
        """
        Lấy tóm tắt environment settings (không bao gồm sensitive data)
        
        Returns:
            Dictionary chứa thông tin settings summary
        """
        return {
            'company_name': self.company_name,
            'environment': self.environment,
            'sql_server_host': self.sql_server_host,
            'sql_server_database': self.sql_server_database,
            'active_data_sources': self.get_active_data_sources(),
            'available_schemas': list(self.schema_mappings.keys()),
            'etl_batch_size': self.etl_batch_size,
            'api_timeout': self.api_timeout,
            'log_level': self.log_level,
            'has_db_password': bool(self.sql_server_password),
            'tiktok_shop_configured': bool(self.tiktok_shop_app_secret and self.tiktok_shop_access_token)
        }

# Global settings instance
settings = Settings()

# Validate settings khi import
try:
    if settings.sql_server_password:
        active_sources = settings.get_active_data_sources()
        if active_sources:
            logger.info(f"Facolos Enterprise ETL Settings initialized. Active sources: {', '.join(active_sources)}")
        else:
            logger.warning("Không có data source nào được cấu hình. Hãy kiểm tra file .env")
    else:
        logger.warning("Database settings chưa được cấu hình. Hãy kiểm tra file .env")
except Exception as e:
    logger.error(f"Lỗi khởi tạo settings: {str(e)}")
