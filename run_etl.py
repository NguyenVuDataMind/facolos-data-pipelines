"""
Chương trình chạy ETL thủ công cho việc thử nghiệm và phát triển
Có thể được sử dụng để chạy quy trình ETL bên ngoài Airflow
"""

import sys
import os
from datetime import datetime, timedelta
import argparse

# Thêm thư mục gốc dự án vào path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractors.tiktok_extractor import TikTokOrderExtractor
from src.transformers.tiktok_transformer import TikTokOrderTransformer
from src.loaders.staging_loader import TikTokOrderLoader
from src.utils.logging import setup_logging, get_log_file_path

def run_full_etl(days_back: int = 1, load_mode: str = 'append') -> bool:
    """
    Chạy quy trình ETL đầy đủ thủ công
    
    Args:
        days_back: Số ngày để quay lại tìm đơn hàng
        load_mode: Chế độ tải database ('append', 'replace', 'truncate_insert')
        
    Returns:
        True nếu thành công, False nếu ngược lại
    """
    # Thiết lập logging
    log_file = get_log_file_path("manual_etl")
    logger = setup_logging("manual_etl", log_file)
    
    try:
        logger.info("="*50)
        logger.info("Bắt đầu quy trình ETL TikTok thủ công")
        logger.info(f"Tham số: days_back={days_back}, load_mode={load_mode}")
        logger.info("="*50)
        
        # Bước 1: Trích xuất
        logger.info("Bước 1: Trích xuất đơn hàng từ TikTok API...")
        extractor = TikTokOrderExtractor()
        
        # Kiểm tra kết nối trước
        if not extractor.test_api_connection():
            logger.error("Kiểm tra kết nối API thất bại")
            return False
            
        # Trích xuất đơn hàng
        orders = extractor.extract_recent_orders(days_back=days_back)
        
        if not orders:
            logger.warning("Không có đơn hàng được trích xuất")
            return True  # Không phải lỗi, chỉ là không có dữ liệu
            
        logger.info(f"Đã trích xuất thành công {len(orders)} đơn hàng")
        
        # Bước 2: Chuyển đổi
        logger.info("Bước 2: Chuyển đổi đơn hàng...")
        transformer = TikTokOrderTransformer()
        
        df = transformer.transform_orders_to_dataframe(orders)
        
        if df.empty:
            logger.warning("Quá trình chuyển đổi tạo ra DataFrame trống")
            return True
            
        if not transformer.validate_dataframe(df):
            logger.error("Xác thực DataFrame thất bại")
            return False
            
        logger.info(f"Đã chuyển đổi thành công thành {len(df)} hàng")
        
        # Bước 3: Tải
        logger.info("Bước 3: Tải vào database staging...")
        loader = TikTokOrderLoader()
        
        # Kiểm tra kết nối database
        if not loader.test_connection():
            logger.error("Kiểm tra kết nối database thất bại")
            return False
            
        # Khởi tạo staging nếu cần
        if not loader.initialize_staging():
            logger.error("Khởi tạo staging thất bại")
            return False
            
        # Tải dữ liệu
        if not loader.load_orders(df, load_mode=load_mode):
            logger.error("Tải đơn hàng thất bại")
            return False
            
        # Lấy thống kê cuối cùng
        stats = loader.get_load_statistics()
        
        logger.info("="*50)
        logger.info("Quy trình ETL hoàn thành thành công!")
        logger.info(f"Thống kê cuối cùng: {stats}")
        logger.info("="*50)
        
        return True
        
    except Exception as e:
        logger.error(f"Quy trình ETL thất bại: {str(e)}", exc_info=True)
        return False

def test_connections() -> bool:
    """
    Test all connections (API and Database)
    
    Returns:
        True if all tests pass, False otherwise
    """
    logger = setup_logging("connection_test")
    
    try:
        logger.info("Testing all connections...")
        
        # Test API
        logger.info("Testing TikTok API connection...")
        extractor = TikTokOrderExtractor()
        api_ok = extractor.test_api_connection()
        
        # Test Database
        logger.info("Testing database connection...")
        loader = TikTokOrderLoader()
        db_ok = loader.test_connection()
        
        if api_ok and db_ok:
            logger.info("✓ All connection tests passed!")
            return True
        else:
            logger.error("✗ Some connection tests failed")
            return False
            
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return False

def main():
    """Main function for command-line interface"""
    parser = argparse.ArgumentParser(description="TikTok ETL Manual Runner")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # ETL command
    etl_parser = subparsers.add_parser('etl', help='Run full ETL process')
    etl_parser.add_argument('--days-back', type=int, default=1, 
                           help='Number of days to look back for orders')
    etl_parser.add_argument('--load-mode', choices=['append', 'replace', 'truncate_insert'],
                           default='append', help='Database load mode')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test connections')
    
    # Extract only command
    extract_parser = subparsers.add_parser('extract', help='Extract orders only')
    extract_parser.add_argument('--days-back', type=int, default=1)
    
    args = parser.parse_args()
    
    if args.command == 'etl':
        success = run_full_etl(days_back=args.days_back, load_mode=args.load_mode)
        exit(0 if success else 1)
        
    elif args.command == 'test':
        success = test_connections()
        exit(0 if success else 1)
        
    elif args.command == 'extract':
        logger = setup_logging("extract_only")
        try:
            extractor = TikTokOrderExtractor()
            orders = extractor.extract_recent_orders(days_back=args.days_back)
            print(f"Extracted {len(orders)} orders")
            
            # Optionally save to file
            import json
            filename = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(orders, f, indent=2)
            print(f"Saved to {filename}")
            
        except Exception as e:
            logger.error(f"Extract failed: {str(e)}")
            exit(1)
            
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
