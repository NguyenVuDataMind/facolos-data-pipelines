"""
Apache Airflow DAG cho TikTok Shop Orders ETL
Điều phối việc trích xuất, chuyển đổi và tải dữ liệu đơn hàng TikTok Shop
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import logging
import json
import os
import pandas as pd

# Import các module ETL của chúng ta - TikTok Shop specific
import sys
import os
sys.path.append('/opt/airflow')

from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor
from src.transformers.tiktok_shop_transformer import TikTokShopOrderTransformer
from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader
from src.utils.logging import setup_logging

# Thiết lập logging cho TikTok Shop
logger = setup_logging("tiktok_shop_etl_dag")

# Tham số mặc định cho DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Tạo DAG cho TikTok Shop
dag = DAG(
    'tiktok_shop_orders_etl',
    default_args=default_args,
    description='Trích xuất đơn hàng TikTok Shop và tải vào staging',
    schedule_interval='0 */6 * * *',  # Chạy mỗi 6 giờ
    max_active_runs=1,
    tags=['tiktok-shop', 'orders', 'etl', 'staging']
)

def extract_tiktok_shop_orders(**context) -> str:
    """
    Trích xuất đơn hàng TikTok Shop từ API
    
    Returns:
        Chuỗi JSON của các đơn hàng TikTok Shop đã trích xuất
    """
    try:
        logger.info("Bắt đầu trích xuất đơn hàng TikTok Shop...")
        
        # Lấy tham số trích xuất cho TikTok Shop
        execution_date = context['execution_date']
        days_back = Variable.get('tiktok_shop_extraction_days_back', default_var=1)
        
        # Khởi tạo TikTok Shop extractor
        shop_extractor = TikTokShopOrderExtractor()
        
        # Kiểm tra kết nối TikTok Shop API trước
        if not shop_extractor.test_api_connection():
            raise Exception("Kiểm tra kết nối TikTok Shop API thất bại")
            
        # Trích xuất đơn hàng TikTok Shop
        shop_orders = shop_extractor.extract_recent_orders(days_back=int(days_back))
        
        if not shop_orders:
            logger.warning("Không có đơn hàng TikTok Shop nào được trích xuất")
            return "[]"
            
        logger.info(f"Đã trích xuất thành công {len(shop_orders)} đơn hàng TikTok Shop")
        
        # Lưu trữ đơn hàng TikTok Shop trong XCom (cho dataset nhỏ) hoặc temporary storage
        shop_orders_json = json.dumps(shop_orders)
        
        # Đối với dataset lớn, cân nhắc lưu vào file storage tạm thời
        # và truyền đường dẫn file thay thế
        
        return shop_orders_json
        
    except Exception as e:
        logger.error(f"Lỗi trích xuất đơn hàng TikTok Shop: {str(e)}")
        raise

def transform_tiktok_shop_orders(**context) -> str:
    """
    Chuyển đổi đơn hàng TikTok Shop đã trích xuất sang định dạng staging
    
    Returns:
        Đường dẫn file parquet chứa dữ liệu TikTok Shop đã chuyển đổi
    """
    try:
        logger.info("Bắt đầu chuyển đổi đơn hàng TikTok Shop...")
        
        # Lấy đơn hàng TikTok Shop từ task trước đó
        shop_orders_json = context['task_instance'].xcom_pull(task_ids='extract_shop_orders')
        shop_orders = json.loads(shop_orders_json) if shop_orders_json else []
        
        if not shop_orders:
            logger.warning("Không có đơn hàng TikTok Shop để chuyển đổi")
            return "Không có đơn hàng TikTok Shop để chuyển đổi"
            
        # Khởi tạo TikTok Shop transformer
        transformer = TikTokShopOrderTransformer()
        
        # Chuyển đổi đơn hàng TikTok Shop thành DataFrame
        shop_df = transformer.transform_orders_to_dataframe(shop_orders)
        
        if shop_df.empty:
            logger.warning("Quá trình chuyển đổi TikTok Shop tạo ra DataFrame trống")
            return "DataFrame TikTok Shop trống sau khi chuyển đổi"
            
        # Xác thực DataFrame cho TikTok Shop
        if not transformer.validate_dataframe(shop_df):
            raise Exception("Xác thực DataFrame TikTok Shop thất bại")
            
        logger.info(f"Đã chuyển đổi thành công {len(shop_orders)} đơn hàng TikTok Shop thành {len(shop_df)} hàng")
        
        # Lưu DataFrame TikTok Shop vào file tạm thời cho task tiếp theo
        temp_file = f"/tmp/tiktok_shop_orders_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}.parquet"
        shop_df.to_parquet(temp_file, index=False)
        
        # Trả về đường dẫn file qua XCom
        return temp_file
        
    except Exception as e:
        logger.error(f"Lỗi chuyển đổi đơn hàng TikTok Shop: {str(e)}")
        raise

def load_tiktok_shop_orders(**context) -> str:
    """
    Tải đơn hàng TikTok Shop đã chuyển đổi vào database staging
    
    Returns:
        Thông báo thành công với thống kê tải dữ liệu TikTok Shop
    """
    try:
        logger.info("Bắt đầu tải đơn hàng TikTok Shop...")
        
        # Lấy đường dẫn file DataFrame TikTok Shop từ task trước đó
        temp_file = context['task_instance'].xcom_pull(task_ids='transform_shop_orders')
        
        if not temp_file or not os.path.exists(temp_file):
            logger.warning("Không có file dữ liệu TikTok Shop để tải")
            return "Không có dữ liệu TikTok Shop để tải"
            
        # Đọc DataFrame TikTok Shop
        shop_df = pd.read_parquet(temp_file)
        
        if shop_df.empty:
            logger.warning("DataFrame TikTok Shop trống")
            return "DataFrame TikTok Shop trống"
            
        # Khởi tạo TikTok Shop loader
        loader = TikTokShopOrderLoader()
        
        # Kiểm tra kết nối database cho TikTok Shop
        if not loader.test_connection():
            raise Exception("Kiểm tra kết nối database cho TikTok Shop thất bại")
            
        # Khởi tạo staging TikTok Shop nếu cần
        if not loader.initialize_staging():
            raise Exception("Khởi tạo môi trường staging TikTok Shop thất bại")
            
        # Tải dữ liệu TikTok Shop theo kiểu incremental
        if not loader.load_incremental_orders(shop_df):
            raise Exception("Tải đơn hàng TikTok Shop vào staging thất bại")
            
        # Lấy thống kê tải dữ liệu TikTok Shop
        shop_stats = loader.get_load_statistics()
        
        # Dọn dẹp file tạm thời
        try:
            os.remove(temp_file)
        except Exception as e:
            logger.warning(f"Không thể dọn dẹp file tạm TikTok Shop: {str(e)}")
            
        logger.info(f"Đã tải đơn hàng TikTok Shop thành công. Thống kê: {shop_stats}")
        return f"Hoàn thành tải dữ liệu TikTok Shop. Thống kê: {shop_stats}"
        
    except Exception as e:
        logger.error(f"Lỗi tải đơn hàng TikTok Shop: {str(e)}")
        raise

def test_tiktok_shop_api_connection(**context) -> str:
    """
    Kiểm tra kết nối TikTok Shop API trước khi bắt đầu ETL
    
    Returns:
        Thông báo thành công
    """
    try:
        logger.info("Đang kiểm tra kết nối TikTok Shop API...")
        
        extractor = TikTokShopOrderExtractor()
        
        if not extractor.test_api_connection():
            raise Exception("Kiểm tra kết nối TikTok Shop API thất bại")
            
        logger.info("Kiểm tra kết nối TikTok Shop API thành công")
        return "Kiểm tra kết nối TikTok Shop API thành công"
        
    except Exception as e:
        logger.error(f"Kiểm tra kết nối TikTok Shop API thất bại: {str(e)}")
        raise

def test_database_connection(**context) -> str:
    """
    Kiểm tra kết nối database trước khi bắt đầu ETL TikTok Shop
    
    Returns:
        Thông báo thành công
    """
    try:
        logger.info("Đang kiểm tra kết nối database cho TikTok Shop...")
        
        loader = TikTokShopOrderLoader()
        
        if not loader.test_connection():
            raise Exception("Kiểm tra kết nối database cho TikTok Shop thất bại")
            
        logger.info("Kiểm tra kết nối database cho TikTok Shop thành công")
        return "Kiểm tra kết nối database cho TikTok Shop thành công"
        
    except Exception as e:
        logger.error(f"Kiểm tra kết nối database cho TikTok Shop thất bại: {str(e)}")
        raise

# Định nghĩa các task
test_api_task = PythonOperator(
    task_id='test_tiktok_shop_api_connection',
    python_callable=test_tiktok_shop_api_connection,
    dag=dag
)

test_db_task = PythonOperator(
    task_id='test_database_connection',
    python_callable=test_database_connection,
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_tiktok_shop_orders',
    python_callable=extract_tiktok_shop_orders,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_tiktok_shop_orders',
    python_callable=transform_tiktok_shop_orders,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_tiktok_shop_orders',
    python_callable=load_tiktok_shop_orders,
    dag=dag
)

# Thiết lập các phụ thuộc task
[test_api_task, test_db_task] >> extract_task >> transform_task >> load_task
