#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete Database Setup Script
Optimal architecture cho database setup với proper sequencing
"""

import sys
import os
import pyodbc
from typing import Optional, List, Dict

# Add project root to Python path
sys.path.append('.')

from config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging("complete_database_setup")

class DatabaseSetupOrchestrator:
    """
    Orchestrates complete database setup với proper error handling,
    verification, và environment awareness
    """
    
    def __init__(self):
        self.connection = None
        self.setup_steps = [
            {
                'name': 'Database Creation',
                'function': self._create_database,
                'required': True,
                'description': 'Tạo Facolos_Staging database nếu chưa tồn tại'
            },
            {
                'name': 'Core Schemas',
                'function': self._create_core_schemas,
                'required': True,
                'description': 'Tạo tất cả schemas (staging, shopee, lazada, etc.)',
                'sql_file': 'sql/staging/create_facolos_enterprise_schemas.sql'
            },
            {
                'name': 'MISA CRM Tables',
                'function': self._create_misa_tables,
                'required': True,
                'description': 'Tạo 5 bảng MISA CRM trong staging schema',
                'sql_file': 'sql/staging/create_misa_crm_tables.sql'
            },
            {
                'name': 'TikTok Shop Tables',
                'function': self._create_tiktok_tables,
                'required': True,
                'description': 'Tạo TikTok Shop tables trong staging schema',
                'sql_file': 'sql/staging/create_tiktok_shop_orders_table.sql'
            },
            {
                'name': 'Verification',
                'function': self._verify_setup,
                'required': True,
                'description': 'Verify tất cả tables và indexes đã được tạo'
            }
        ]
    
    def get_connection(self, database: str = None, for_create_db: bool = False) -> Optional[pyodbc.Connection]:
        """Get database connection with improved error handling"""
        try:
            target_db = database or settings.sql_server_database

            # For database creation, always use master
            if for_create_db:
                target_db = 'master'

            # Try multiple connection approaches
            connection_configs = [
                # Approach 1: SQL Authentication with port
                {
                    'name': 'SQL Auth with port',
                    'conn_str': (
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={settings.sql_server_host},{settings.sql_server_port};"
                        f"DATABASE={target_db};"
                        f"UID={settings.sql_server_username};"
                        f"PWD={settings.sql_server_password};"
                        f"TrustServerCertificate=yes"
                    )
                },
                # Approach 2: SQL Authentication without port
                {
                    'name': 'SQL Auth without port',
                    'conn_str': (
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={settings.sql_server_host};"
                        f"DATABASE={target_db};"
                        f"UID={settings.sql_server_username};"
                        f"PWD={settings.sql_server_password};"
                        f"TrustServerCertificate=yes"
                    )
                },
                # Approach 3: Windows Authentication with port
                {
                    'name': 'Windows Auth with port',
                    'conn_str': (
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={settings.sql_server_host},{settings.sql_server_port};"
                        f"DATABASE={target_db};"
                        f"Trusted_Connection=yes;"
                        f"TrustServerCertificate=yes"
                    )
                },
                # Approach 4: Windows Authentication without port
                {
                    'name': 'Windows Auth without port',
                    'conn_str': (
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                        f"SERVER={settings.sql_server_host};"
                        f"DATABASE={target_db};"
                        f"Trusted_Connection=yes;"
                        f"TrustServerCertificate=yes"
                    )
                }
            ]

            for i, config in enumerate(connection_configs, 1):
                try:
                    logger.info(f"🔄 Trying {config['name']} (approach {i}/{len(connection_configs)})...")
                    connection = pyodbc.connect(config['conn_str'])

                    # Test connection with a simple query
                    cursor = connection.cursor()
                    cursor.execute("SELECT @@VERSION")
                    version = cursor.fetchone()[0]
                    cursor.close()

                    logger.info(f"✅ Connected to {target_db} using {config['name']}")
                    logger.info(f"   SQL Server: {version[:50]}...")
                    return connection

                except Exception as e:
                    logger.warning(f"⚠️ {config['name']} failed: {str(e)[:80]}...")
                    continue

            logger.error("❌ All connection approaches failed")
            logger.error("💡 Please check:")
            logger.error("   1. SQL Server is running and accessible")
            logger.error("   2. Credentials are correct")
            logger.error("   3. Network connectivity")
            logger.error("   4. SQL Server allows remote connections")
            return None

        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return None
    
    def _create_database(self) -> bool:
        """Step 1: Create database if not exists"""
        try:
            logger.info("🔧 Step 1: Creating/verifying Facolos_Staging database...")

            # Connect to master database with special flag
            connection = self.get_connection('master', for_create_db=True)
            if not connection:
                logger.error("❌ Cannot connect to master database")
                return False

            try:
                cursor = connection.cursor()

                # Check if database exists
                cursor.execute("""
                    SELECT name FROM sys.databases
                    WHERE name = 'Facolos_Staging'
                """)

                if cursor.fetchone():
                    logger.info("✅ Facolos_Staging database already exists")
                    cursor.close()
                    connection.close()
                    return True

                # Database doesn't exist, create it
                logger.info("🔧 Creating Facolos_Staging database...")

                # Close current cursor
                cursor.close()

                # Enable autocommit for CREATE DATABASE
                connection.autocommit = True
                cursor = connection.cursor()

                # Create database
                cursor.execute("CREATE DATABASE Facolos_Staging")
                logger.info("✅ Facolos_Staging database created successfully")

                # Verify creation
                cursor.execute("""
                    SELECT name FROM sys.databases
                    WHERE name = 'Facolos_Staging'
                """)

                if cursor.fetchone():
                    logger.info("✅ Database creation verified")
                else:
                    logger.error("❌ Database creation verification failed")
                    return False

                cursor.close()
                connection.close()
                return True

            except Exception as e:
                logger.error(f"❌ Error during database creation: {e}")
                if connection:
                    connection.close()
                return False

        except Exception as e:
            logger.error(f"❌ Database creation failed: {e}")
            return False
    
    def _execute_sql_file(self, sql_file: str, step_name: str) -> bool:
        """Execute SQL file with proper error handling"""
        try:
            if not os.path.exists(sql_file):
                logger.error(f"❌ SQL file not found: {sql_file}")
                logger.error(f"   Expected path: {os.path.abspath(sql_file)}")
                return False

            logger.info(f"📄 Executing {step_name}: {sql_file}")

            # Read file with proper encoding
            try:
                with open(sql_file, 'r', encoding='utf-8') as file:
                    sql_content = file.read()
            except UnicodeDecodeError:
                # Try with different encoding if UTF-8 fails
                with open(sql_file, 'r', encoding='utf-8-sig') as file:
                    sql_content = file.read()

            if not sql_content.strip():
                logger.warning(f"⚠️ SQL file is empty: {sql_file}")
                return True

            # Split by GO statements
            sql_batches = [batch.strip() for batch in sql_content.split('GO') if batch.strip()]
            logger.info(f"📊 Found {len(sql_batches)} SQL batches to execute")

            if not self.connection:
                logger.error("❌ No database connection available")
                return False

            cursor = self.connection.cursor()
            successful_batches = 0

            for i, batch in enumerate(sql_batches, 1):
                if batch.strip():
                    logger.info(f"🔄 Executing batch {i}/{len(sql_batches)}")
                    try:
                        cursor.execute(batch)
                        self.connection.commit()
                        successful_batches += 1
                        logger.debug(f"✅ Batch {i} completed")
                    except Exception as e:
                        # Log error but continue with next batch for some types of errors
                        error_msg = str(e).lower()
                        if 'already exists' in error_msg or 'duplicate' in error_msg:
                            logger.warning(f"⚠️ Batch {i} warning (object already exists): {str(e)[:100]}...")
                            successful_batches += 1
                            continue
                        else:
                            logger.error(f"❌ Error in batch {i}: {e}")
                            logger.error(f"Batch content preview: {batch[:200]}...")
                            cursor.close()
                            return False

            cursor.close()
            logger.info(f"✅ {step_name} completed successfully ({successful_batches}/{len(sql_batches)} batches)")
            return True

        except Exception as e:
            logger.error(f"❌ Error executing {step_name}: {e}")
            return False
    
    def _create_core_schemas(self) -> bool:
        """Step 2: Create core schemas"""
        logger.info("🏗️ Step 2: Creating core schemas...")
        return self._execute_sql_file(
            'sql/staging/create_facolos_enterprise_schemas.sql',
            'Core Schemas'
        )
    
    def _create_misa_tables(self) -> bool:
        """Step 3: Create MISA CRM tables"""
        logger.info("🏢 Step 3: Creating MISA CRM tables...")
        return self._execute_sql_file(
            'sql/staging/create_misa_crm_tables.sql',
            'MISA CRM Tables'
        )
    
    def _create_tiktok_tables(self) -> bool:
        """Step 4: Create TikTok Shop tables"""
        logger.info("🛒 Step 4: Creating TikTok Shop tables...")
        return self._execute_sql_file(
            'sql/staging/create_tiktok_shop_orders_table.sql',
            'TikTok Shop Tables'
        )
    
    def _verify_setup(self) -> bool:
        """Step 5: Verify complete setup"""
        try:
            logger.info("🔍 Step 5: Verifying complete setup...")
            
            # Expected tables (production-ready)
            expected_tables = {
                'misa_customers': 'MISA CRM Customers',
                'misa_sale_orders_flattened': 'MISA CRM Sale Orders (Flattened)',
                'misa_contacts': 'MISA CRM Contacts',
                'misa_stocks': 'MISA CRM Stocks',
                'misa_products': 'MISA CRM Products',
                'tiktok_shop_order_detail': 'TikTok Shop Order Details'
            }
            
            cursor = self.connection.cursor()
            
            results = {}
            for table, description in expected_tables.items():
                cursor.execute("""
                    SELECT COUNT(*) as table_exists
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'staging' 
                    AND TABLE_NAME = ?
                """, table)
                
                result = cursor.fetchone()
                exists = result.table_exists > 0
                results[table] = exists
                
                status = "✅" if exists else "❌"
                logger.info(f"{status} {description}: staging.{table}")
            
            cursor.close()
            
            # Summary
            total_tables = len(expected_tables)
            created_tables = sum(results.values())
            
            logger.info(f"📊 Setup Summary: {created_tables}/{total_tables} tables created")
            
            return created_tables == total_tables
            
        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return False
    
    def execute_complete_setup(self) -> bool:
        """Execute complete database setup"""
        logger.info("🚀 Starting COMPLETE database setup...")
        logger.info("=" * 60)

        # Debug: Show current settings
        logger.info("📋 Current database settings:")
        logger.info(f"   Host: {settings.sql_server_host}")
        logger.info(f"   Port: {settings.sql_server_port}")
        logger.info(f"   Database: {settings.sql_server_database}")
        logger.info(f"   Username: {settings.sql_server_username}")
        logger.info(f"   Password: {'***' if settings.sql_server_password else 'NOT SET'}")
        logger.info("")
        
        try:
            for i, step in enumerate(self.setup_steps, 1):
                logger.info(f"\n📋 Step {i}/{len(self.setup_steps)}: {step['name']}")
                logger.info(f"   {step['description']}")
                logger.info("-" * 40)

                # For steps 2-5, ensure we have connection to target database
                if i > 1:
                    if not self.connection:
                        logger.info("🔄 Connecting to target database...")
                        self.connection = self.get_connection()
                        if not self.connection:
                            logger.error("❌ Cannot connect to target database")
                            return False

                # Execute step
                try:
                    success = step['function']()
                except Exception as step_error:
                    logger.error(f"❌ Exception in step {step['name']}: {step_error}")
                    success = False

                if not success:
                    if step.get('required', True):
                        logger.error(f"❌ Required step failed: {step['name']}")
                        logger.error("🛑 Stopping setup due to critical failure")
                        return False
                    else:
                        logger.warning(f"⚠️ Optional step failed: {step['name']}")
                        logger.info("▶️ Continuing with next step...")

                logger.info(f"✅ Step {i} completed: {step['name']}")

                # Small delay between steps for stability
                import time
                time.sleep(0.5)

            logger.info("\n" + "="*60)
            logger.info("🎉 COMPLETE DATABASE SETUP SUCCESSFUL!")
            logger.info("🚀 Database is ready for ETL pipeline deployment")
            logger.info("="*60)
            return True

        except Exception as e:
            logger.error(f"❌ Setup failed with exception: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False

        finally:
            if self.connection:
                try:
                    self.connection.close()
                    logger.info("🔌 Database connection closed")
                except:
                    pass

def main():
    """Main function"""
    orchestrator = DatabaseSetupOrchestrator()
    success = orchestrator.execute_complete_setup()
    
    if success:
        print("\n" + "="*60)
        print("🎉 SUCCESS: Complete database setup finished!")
        print("🚀 Ready for ETL pipeline deployment")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("❌ FAILED: Database setup encountered errors")
        print("🔧 Please check logs and fix issues")
        print("="*60)
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
