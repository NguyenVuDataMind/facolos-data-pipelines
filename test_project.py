"""
Test script để kiểm tra project configuration và dependencies
Chạy script này để verify rằng project đã self-contained
"""

import sys
import os

# Add src to path để test imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test tất cả imports cần thiết"""
    print("🔍 Testing imports...")
    
    try:
        # Test config import
        from config.settings import settings
        print("✅ Config import: OK")
        
        # Test auth import
        from src.utils.auth import TikTokAuthenticator
        print("✅ Auth import: OK")
        
        print("✅ All imports successful!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_settings():
    """Test settings configuration"""
    print("\n🔧 Testing settings...")
    
    try:
        from config.settings import settings
        
        # Test basic settings
        print(f"App Key: {settings.tiktok_app_key}")
        print(f"SQL Host: {settings.sql_server_host}")
        print(f"Staging Table: {settings.staging_table_full_name}")
        
        # Test settings summary
        summary = settings.get_env_summary()
        print("Settings summary:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
        
        print("✅ Settings configuration: OK")
        return True
        
    except Exception as e:
        print(f"❌ Settings error: {e}")
        return False

def test_auth():
    """Test authentication module"""
    print("\n🔐 Testing authentication...")
    
    try:
        from src.utils.auth import TikTokAuthenticator
        
        # Initialize authenticator
        auth = TikTokAuthenticator()
        print(f"App Key: {auth.app_key}")
        print(f"Has App Secret: {bool(auth.app_secret)}")
        print(f"Has Access Token: {bool(auth.access_token)}")
        
        # Test signature generation
        test_params = {'test': 'value', 'timestamp': '1234567890'}
        signature = auth.generate_signature('/test/path', test_params)
        print(f"Test signature generated: {signature[:10]}...")
        
        print("✅ Authentication module: OK")
        return True
        
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return False

def check_project_structure():
    """Kiểm tra cấu trúc project"""
    print("\n📁 Checking project structure...")
    
    required_files = [
        'config/__init__.py',
        'config/settings.py',
        'src/utils/auth.py',
        'src/extractors/tiktok_shop_extractor.py',
        'src/transformers/tiktok_shop_transformer.py',
        'src/loaders/tiktok_shop_staging_loader.py',
        'dags/tiktok_shop_orders_etl_dag.py',
        'Dockerfile',
        'docker-compose.yml',
        '.env',
        'requirements.txt'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("❌ Missing files:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        return False
    else:
        print("✅ All required files present")
        return True

def main():
    """Main test function"""
    print("🚀 TikTok Shop ETL Project Self-Contained Test")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 4
    
    # Run tests
    if check_project_structure():
        tests_passed += 1
    
    if test_imports():
        tests_passed += 1
    
    if test_settings():
        tests_passed += 1
    
    if test_auth():
        tests_passed += 1
    
    # Summary
    print("\n" + "=" * 50)
    print(f"🎯 Test Results: {tests_passed}/{total_tests} passed")
    
    if tests_passed == total_tests:
        print("✅ PROJECT IS SELF-CONTAINED AND READY!")
        print("💡 Bạn có thể deploy project này ở bất kỳ đâu")
        print("💡 Chỉ cần cấu hình .env file với credentials")
    else:
        print("❌ Project cần sửa chữa trước khi deploy")
    
    return tests_passed == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
