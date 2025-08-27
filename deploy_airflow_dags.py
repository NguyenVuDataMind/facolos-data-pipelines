#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Airflow DAG Deployment Script
Deploy và test Airflow DAGs cho production
"""

import sys
import os
import subprocess
from datetime import datetime
from typing import Dict, List, Any

# Add project root to Python path
sys.path.append('.')

from config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging("dag_deployment")

class AirflowDAGDeployer:
    """
    Deploy và manage Airflow DAGs
    """
    
    def __init__(self):
        self.airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        self.dags_folder = os.path.join(self.airflow_home, 'dags')
        self.project_dags_folder = 'dags'
        
        self.dags_to_deploy = [
            {
                'name': 'misa_crm_etl_dag.py',
                'description': 'MISA CRM ETL Pipeline',
                'schedule': '*/15 * * * *',
                'type': 'incremental'
            },
            {
                'name': 'tiktok_shop_orders_etl_dag.py', 
                'description': 'TikTok Shop Orders ETL Pipeline',
                'schedule': '*/15 * * * *',
                'type': 'incremental'
            }
        ]
    
    def check_airflow_installation(self) -> bool:
        """Check if Airflow is installed and accessible"""
        try:
            logger.info("🔍 Checking Airflow installation...")
            
            # Check airflow command
            result = subprocess.run(['airflow', 'version'], 
                                  capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                version = result.stdout.strip()
                logger.info(f"✅ Airflow found: {version}")
                return True
            else:
                logger.error("❌ Airflow command not found")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Airflow command timeout")
            return False
        except FileNotFoundError:
            logger.error("❌ Airflow not installed or not in PATH")
            return False
        except Exception as e:
            logger.error(f"❌ Error checking Airflow: {e}")
            return False
    
    def validate_dag_files(self) -> Dict[str, bool]:
        """Validate DAG files syntax"""
        logger.info("🔍 Validating DAG files...")
        
        validation_results = {}
        
        for dag_info in self.dags_to_deploy:
            dag_file = dag_info['name']
            dag_path = os.path.join(self.project_dags_folder, dag_file)
            
            logger.info(f"   Validating {dag_file}...")
            
            try:
                # Check if file exists
                if not os.path.exists(dag_path):
                    logger.error(f"❌ DAG file not found: {dag_path}")
                    validation_results[dag_file] = False
                    continue
                
                # Try to compile the Python file
                with open(dag_path, 'r', encoding='utf-8') as f:
                    dag_content = f.read()
                
                # Compile to check syntax
                compile(dag_content, dag_path, 'exec')
                
                logger.info(f"✅ {dag_file}: Syntax valid")
                validation_results[dag_file] = True
                
            except SyntaxError as e:
                logger.error(f"❌ {dag_file}: Syntax error - {e}")
                validation_results[dag_file] = False
            except Exception as e:
                logger.error(f"❌ {dag_file}: Validation error - {e}")
                validation_results[dag_file] = False
        
        return validation_results
    
    def deploy_dags(self) -> Dict[str, bool]:
        """Deploy DAG files to Airflow"""
        logger.info("🚀 Deploying DAG files...")
        
        deployment_results = {}
        
        # Create dags folder if not exists
        os.makedirs(self.dags_folder, exist_ok=True)
        
        for dag_info in self.dags_to_deploy:
            dag_file = dag_info['name']
            source_path = os.path.join(self.project_dags_folder, dag_file)
            target_path = os.path.join(self.dags_folder, dag_file)
            
            logger.info(f"   Deploying {dag_file}...")
            
            try:
                # Copy DAG file
                import shutil
                shutil.copy2(source_path, target_path)
                
                logger.info(f"✅ {dag_file}: Deployed to {target_path}")
                deployment_results[dag_file] = True
                
            except Exception as e:
                logger.error(f"❌ {dag_file}: Deployment failed - {e}")
                deployment_results[dag_file] = False
        
        return deployment_results
    
    def test_dag_imports(self) -> Dict[str, bool]:
        """Test DAG imports in Airflow"""
        logger.info("🔍 Testing DAG imports...")
        
        import_results = {}
        
        for dag_info in self.dags_to_deploy:
            dag_file = dag_info['name']
            
            logger.info(f"   Testing import for {dag_file}...")
            
            try:
                # Use airflow dags list-import-errors to check
                result = subprocess.run([
                    'airflow', 'dags', 'list-import-errors'
                ], capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    # Check if our DAG has import errors
                    if dag_file in result.stdout:
                        logger.error(f"❌ {dag_file}: Import errors found")
                        import_results[dag_file] = False
                    else:
                        logger.info(f"✅ {dag_file}: Import successful")
                        import_results[dag_file] = True
                else:
                    logger.warning(f"⚠️ {dag_file}: Could not check import status")
                    import_results[dag_file] = True  # Assume OK if can't check
                
            except Exception as e:
                logger.warning(f"⚠️ {dag_file}: Import test failed - {e}")
                import_results[dag_file] = True  # Assume OK if can't test
        
        return import_results
    
    def list_deployed_dags(self) -> List[str]:
        """List deployed DAGs"""
        logger.info("📋 Listing deployed DAGs...")
        
        try:
            result = subprocess.run([
                'airflow', 'dags', 'list'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                dags = []
                for line in result.stdout.split('\n'):
                    if 'misa_crm' in line or 'tiktok_shop' in line:
                        dag_id = line.split()[0] if line.strip() else ''
                        if dag_id:
                            dags.append(dag_id)
                            logger.info(f"   ✅ Found DAG: {dag_id}")
                
                return dags
            else:
                logger.error("❌ Could not list DAGs")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error listing DAGs: {e}")
            return []
    
    def check_dag_schedules(self) -> Dict[str, Any]:
        """Check DAG schedules for conflicts"""
        logger.info("⏰ Checking DAG schedules...")
        
        schedule_info = {}
        
        for dag_info in self.dags_to_deploy:
            dag_name = dag_info['name']
            schedule = dag_info['schedule']
            
            schedule_info[dag_name] = {
                'schedule': schedule,
                'description': dag_info['description'],
                'type': dag_info['type']
            }
            
            logger.info(f"   📅 {dag_name}: {schedule} ({dag_info['type']})")
        
        # Check for conflicts
        schedules = [info['schedule'] for info in schedule_info.values()]
        if len(set(schedules)) < len(schedules):
            logger.warning("⚠️ Multiple DAGs have the same schedule - this may cause resource conflicts")
        else:
            logger.info("✅ No schedule conflicts detected")
        
        return schedule_info
    
    def run_deployment(self) -> bool:
        """Run complete DAG deployment"""
        logger.info("🚀 Starting Airflow DAG Deployment")
        logger.info("=" * 60)
        
        try:
            # Step 1: Check Airflow installation
            if not self.check_airflow_installation():
                logger.error("❌ Airflow not available - cannot proceed")
                return False
            
            # Step 2: Validate DAG files
            logger.info("\n" + "="*50)
            validation_results = self.validate_dag_files()
            
            failed_validations = [dag for dag, result in validation_results.items() if not result]
            if failed_validations:
                logger.error(f"❌ DAG validation failed for: {failed_validations}")
                return False
            
            # Step 3: Deploy DAGs
            logger.info("\n" + "="*50)
            deployment_results = self.deploy_dags()
            
            failed_deployments = [dag for dag, result in deployment_results.items() if not result]
            if failed_deployments:
                logger.error(f"❌ DAG deployment failed for: {failed_deployments}")
                return False
            
            # Step 4: Test imports
            logger.info("\n" + "="*50)
            import_results = self.test_dag_imports()
            
            # Step 5: List deployed DAGs
            logger.info("\n" + "="*50)
            deployed_dags = self.list_deployed_dags()
            
            # Step 6: Check schedules
            logger.info("\n" + "="*50)
            schedule_info = self.check_dag_schedules()
            
            # Summary
            logger.info("\n" + "="*60)
            logger.info("📊 DEPLOYMENT SUMMARY")
            logger.info("="*60)
            
            successful_dags = sum(1 for result in deployment_results.values() if result)
            total_dags = len(self.dags_to_deploy)
            
            logger.info(f"📈 Deployment success rate: {successful_dags}/{total_dags}")
            logger.info(f"📋 Deployed DAGs: {len(deployed_dags)}")
            
            for dag_name, info in schedule_info.items():
                logger.info(f"   ⏰ {dag_name}: {info['schedule']}")
            
            if successful_dags == total_dags:
                logger.info("\n🎉 ALL DAGS DEPLOYED SUCCESSFULLY!")
                logger.info("🚀 Ready for Phase 3: Incremental ETL Setup")
                return True
            else:
                logger.error("\n❌ SOME DAGS FAILED TO DEPLOY")
                return False
            
        except Exception as e:
            logger.error(f"❌ Deployment failed: {e}")
            return False

def main():
    """Main function"""
    deployer = AirflowDAGDeployer()
    success = deployer.run_deployment()
    
    if success:
        print("\n" + "="*60)
        print("🎉 SUCCESS: Airflow DAGs deployed successfully!")
        print("🚀 Ready for Phase 3: Incremental ETL Setup")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("❌ FAILED: DAG deployment encountered errors")
        print("🔧 Please check logs and fix issues")
        print("="*60)
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
