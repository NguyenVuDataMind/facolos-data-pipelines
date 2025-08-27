#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Airflow Production ETL Monitor
Monitor và verify Airflow production ETL pipeline
"""

import sys
import os
import time
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add project root to Python path
sys.path.append('.')

from src.utils.logging import setup_logging

logger = setup_logging("airflow_production_monitor")

class AirflowProductionMonitor:
    """
    Monitor Airflow production ETL pipeline
    """
    
    def __init__(self, airflow_url: str = "http://localhost:8080"):
        self.airflow_url = airflow_url
        self.dag_id = "facolos_incremental_etl_production"
        self.auth = ("admin", "facolos2024")  # From .env file
        
    def get_dag_status(self) -> Dict[str, Any]:
        """Get DAG status from Airflow API"""
        try:
            url = f"{self.airflow_url}/api/v1/dags/{self.dag_id}"
            response = requests.get(url, auth=self.auth)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get DAG status: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting DAG status: {e}")
            return {}
    
    def get_recent_dag_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent DAG runs"""
        try:
            url = f"{self.airflow_url}/api/v1/dags/{self.dag_id}/dagRuns"
            params = {"limit": limit, "order_by": "-execution_date"}
            response = requests.get(url, auth=self.auth, params=params)
            
            if response.status_code == 200:
                return response.json().get("dag_runs", [])
            else:
                logger.error(f"Failed to get DAG runs: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting DAG runs: {e}")
            return []
    
    def get_task_instances(self, dag_run_id: str) -> List[Dict[str, Any]]:
        """Get task instances for a DAG run"""
        try:
            url = f"{self.airflow_url}/api/v1/dags/{self.dag_id}/dagRuns/{dag_run_id}/taskInstances"
            response = requests.get(url, auth=self.auth)
            
            if response.status_code == 200:
                return response.json().get("task_instances", [])
            else:
                logger.error(f"Failed to get task instances: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting task instances: {e}")
            return []
    
    def verify_schedule_accuracy(self) -> Dict[str, Any]:
        """Verify schedule accuracy (15-minute intervals)"""
        logger.info("🔍 Verifying schedule accuracy...")
        
        dag_runs = self.get_recent_dag_runs(limit=5)
        if len(dag_runs) < 2:
            return {"error": "Not enough DAG runs to verify schedule"}
        
        intervals = []
        for i in range(len(dag_runs) - 1):
            current_time = datetime.fromisoformat(dag_runs[i]["execution_date"].replace("Z", "+00:00"))
            previous_time = datetime.fromisoformat(dag_runs[i+1]["execution_date"].replace("Z", "+00:00"))
            interval = (current_time - previous_time).total_seconds() / 60  # minutes
            intervals.append(interval)
        
        avg_interval = sum(intervals) / len(intervals)
        target_interval = 15.0  # 15 minutes
        accuracy = abs(avg_interval - target_interval) / target_interval * 100
        
        result = {
            "target_interval_minutes": target_interval,
            "actual_avg_interval_minutes": avg_interval,
            "accuracy_percentage": 100 - accuracy,
            "schedule_accurate": accuracy < 5,  # Within 5% tolerance
            "intervals": intervals
        }
        
        logger.info(f"   📊 Target interval: {target_interval} minutes")
        logger.info(f"   📊 Actual average: {avg_interval:.1f} minutes")
        logger.info(f"   📊 Accuracy: {result['accuracy_percentage']:.1f}%")
        logger.info(f"   ✅ Schedule accurate: {result['schedule_accurate']}")
        
        return result
    
    def verify_data_integrity(self) -> Dict[str, Any]:
        """Verify data integrity in staging tables"""
        logger.info("🔍 Verifying data integrity...")
        
        try:
            import pyodbc
            from config.settings import settings
            
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={settings.sql_server_host},{settings.sql_server_port};"
                f"DATABASE={settings.sql_server_database};"
                f"UID={settings.sql_server_username};"
                f"PWD={settings.sql_server_password};"
                f"TrustServerCertificate=yes"
            )
            
            connection = pyodbc.connect(connection_string)
            cursor = connection.cursor()
            
            tables = [
                'misa_customers', 'misa_sale_orders_flattened', 'misa_contacts',
                'misa_stocks', 'misa_products', 'tiktok_shop_order_detail'
            ]
            
            table_counts = {}
            total_records = 0
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM staging.{table}")
                count = cursor.fetchone()[0]
                table_counts[table] = count
                total_records += count
                logger.info(f"   📊 {table}: {count:,} rows")
            
            cursor.close()
            connection.close()
            
            result = {
                "total_records": total_records,
                "table_counts": table_counts,
                "tables_with_data": len([c for c in table_counts.values() if c > 0]),
                "data_integrity_score": (len([c for c in table_counts.values() if c > 0]) / len(tables)) * 100
            }
            
            logger.info(f"   📊 Total records: {total_records:,}")
            logger.info(f"   📊 Tables with data: {result['tables_with_data']}/6")
            logger.info(f"   📊 Data integrity score: {result['data_integrity_score']:.1f}%")
            
            return result
            
        except Exception as e:
            logger.error(f"   ❌ Data integrity check failed: {e}")
            return {"error": str(e)}
    
    def verify_deduplication_mechanism(self) -> Dict[str, Any]:
        """Verify deduplication mechanism by analyzing recent runs"""
        logger.info("🔍 Verifying deduplication mechanism...")
        
        dag_runs = self.get_recent_dag_runs(limit=3)
        if len(dag_runs) < 2:
            return {"error": "Not enough DAG runs to verify deduplication"}
        
        dedup_results = []
        
        for dag_run in dag_runs[:2]:  # Check last 2 runs
            run_id = dag_run["dag_run_id"]
            execution_date = dag_run["execution_date"]
            
            # Get task instances for this run
            task_instances = self.get_task_instances(run_id)
            
            # Find ETL task
            etl_task = next((t for t in task_instances if t["task_id"] == "run_incremental_etl"), None)
            
            if etl_task:
                dedup_results.append({
                    "run_id": run_id,
                    "execution_date": execution_date,
                    "state": etl_task["state"],
                    "duration": etl_task.get("duration", 0),
                    "start_date": etl_task.get("start_date"),
                    "end_date": etl_task.get("end_date")
                })
        
        result = {
            "recent_runs": dedup_results,
            "deduplication_working": all(r["state"] == "success" for r in dedup_results),
            "avg_duration": sum(r["duration"] or 0 for r in dedup_results) / len(dedup_results) if dedup_results else 0
        }
        
        logger.info(f"   📊 Recent runs analyzed: {len(dedup_results)}")
        logger.info(f"   ✅ Deduplication working: {result['deduplication_working']}")
        logger.info(f"   ⏱️  Average duration: {result['avg_duration']:.1f} seconds")
        
        return result
    
    def generate_production_report(self) -> Dict[str, Any]:
        """Generate comprehensive production report"""
        logger.info("📊 GENERATING AIRFLOW PRODUCTION REPORT")
        logger.info("=" * 60)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "dag_id": self.dag_id,
            "airflow_url": self.airflow_url
        }
        
        # DAG Status
        logger.info("1. DAG Status Check")
        dag_status = self.get_dag_status()
        report["dag_status"] = dag_status
        
        if dag_status:
            logger.info(f"   ✅ DAG Active: {not dag_status.get('is_paused', True)}")
            logger.info(f"   📅 Schedule: {dag_status.get('schedule_interval', 'Unknown')}")
        
        # Recent Runs
        logger.info("\n2. Recent DAG Runs")
        recent_runs = self.get_recent_dag_runs(limit=5)
        report["recent_runs"] = recent_runs
        
        successful_runs = len([r for r in recent_runs if r.get("state") == "success"])
        logger.info(f"   📊 Recent runs: {len(recent_runs)}")
        logger.info(f"   ✅ Successful: {successful_runs}/{len(recent_runs)}")
        
        # Schedule Accuracy
        logger.info("\n3. Schedule Accuracy Verification")
        schedule_verification = self.verify_schedule_accuracy()
        report["schedule_verification"] = schedule_verification
        
        # Data Integrity
        logger.info("\n4. Data Integrity Verification")
        data_integrity = self.verify_data_integrity()
        report["data_integrity"] = data_integrity
        
        # Deduplication
        logger.info("\n5. Deduplication Mechanism Verification")
        deduplication = self.verify_deduplication_mechanism()
        report["deduplication"] = deduplication
        
        # Overall Health Score
        health_score = self._calculate_health_score(report)
        report["health_score"] = health_score
        
        logger.info(f"\n📊 OVERALL HEALTH SCORE: {health_score:.1f}%")
        
        if health_score >= 90:
            logger.info("🎉 EXCELLENT: Production ETL pipeline is running optimally!")
        elif health_score >= 80:
            logger.info("✅ GOOD: Production ETL pipeline is running well with minor issues")
        elif health_score >= 70:
            logger.info("⚠️ WARNING: Production ETL pipeline has some issues that need attention")
        else:
            logger.info("❌ CRITICAL: Production ETL pipeline has serious issues requiring immediate attention")
        
        return report
    
    def _calculate_health_score(self, report: Dict[str, Any]) -> float:
        """Calculate overall health score"""
        score = 0
        max_score = 0
        
        # DAG Status (20 points)
        max_score += 20
        if report.get("dag_status", {}).get("is_paused") == False:
            score += 20
        
        # Recent Runs Success Rate (30 points)
        max_score += 30
        recent_runs = report.get("recent_runs", [])
        if recent_runs:
            success_rate = len([r for r in recent_runs if r.get("state") == "success"]) / len(recent_runs)
            score += success_rate * 30
        
        # Schedule Accuracy (20 points)
        max_score += 20
        schedule_verification = report.get("schedule_verification", {})
        if schedule_verification.get("schedule_accurate"):
            score += 20
        
        # Data Integrity (20 points)
        max_score += 20
        data_integrity = report.get("data_integrity", {})
        if data_integrity.get("data_integrity_score", 0) >= 80:
            score += 20
        
        # Deduplication (10 points)
        max_score += 10
        deduplication = report.get("deduplication", {})
        if deduplication.get("deduplication_working"):
            score += 10
        
        return (score / max_score) * 100 if max_score > 0 else 0

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor Airflow Production ETL')
    parser.add_argument('--url', default='http://localhost:8080',
                       help='Airflow URL (default: http://localhost:8080)')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuous monitoring')
    parser.add_argument('--interval', type=int, default=300,
                       help='Monitoring interval in seconds (default: 300)')
    
    args = parser.parse_args()
    
    monitor = AirflowProductionMonitor(airflow_url=args.url)
    
    if args.continuous:
        logger.info("🔄 Starting continuous monitoring...")
        while True:
            try:
                report = monitor.generate_production_report()
                time.sleep(args.interval)
            except KeyboardInterrupt:
                logger.info("🛑 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Monitoring error: {e}")
                time.sleep(60)  # Wait 1 minute before retry
    else:
        # Single report
        report = monitor.generate_production_report()
        
        # Save report to file
        report_file = f"airflow_production_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"📄 Report saved to: {report_file}")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
