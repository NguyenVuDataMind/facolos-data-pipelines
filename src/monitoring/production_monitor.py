#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production ETL Monitoring System
Hệ thống monitoring cho production ETL pipeline
"""

import sys
import os
import json
import time
import smtplib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.logging import setup_logging
from config.production import ProductionConfig
from config.settings import settings

logger = setup_logging("production_monitor")

class ProductionETLMonitor:
    """
    Production ETL monitoring và alerting system
    """
    
    def __init__(self):
        self.config = ProductionConfig()
        self.thresholds = ProductionConfig.get_performance_thresholds()
        self.metrics_history = []
        self.alert_history = []
        
        # Performance tracking
        self.consecutive_failures = 0
        self.consecutive_no_data = 0
        self.last_successful_run = None
        
    def record_cycle_metrics(self, cycle_results: Dict[str, Any]) -> Dict[str, Any]:
        """Record metrics từ một ETL cycle"""
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'cycle_duration': cycle_results.get('duration_seconds', 0),
            'total_records': cycle_results.get('total_records', 0),
            'misa_success_count': len([k for k, v in cycle_results.get('misa_crm', {}).items() 
                                     if isinstance(v, dict) and v.get('status') == 'success']),
            'tiktok_success': cycle_results.get('tiktok_shop', {}).get('orders', {}).get('status') == 'success',
            'errors': cycle_results.get('errors', []),
            'data_quality_passed': cycle_results.get('data_quality', {}).get('quality_check_passed', False)
        }
        
        # Calculate success rates
        metrics['misa_success_rate'] = metrics['misa_success_count'] / 5.0  # 5 MISA endpoints
        metrics['overall_success'] = cycle_results.get('success', False)
        
        # Store metrics
        self.metrics_history.append(metrics)
        
        # Keep only last 100 cycles
        if len(self.metrics_history) > 100:
            self.metrics_history = self.metrics_history[-100:]
        
        # Update consecutive counters
        if metrics['overall_success']:
            self.consecutive_failures = 0
            self.last_successful_run = datetime.now()
            if metrics['total_records'] > 0:
                self.consecutive_no_data = 0
            else:
                self.consecutive_no_data += 1
        else:
            self.consecutive_failures += 1
        
        # Check for alerts
        self._check_alert_conditions(metrics)
        
        return metrics
    
    def _check_alert_conditions(self, metrics: Dict[str, Any]):
        """Check các điều kiện cần alert"""
        
        alerts = []
        
        # Alert 1: Execution time quá lâu
        if metrics['cycle_duration'] > self.thresholds['max_execution_time']:
            alerts.append({
                'type': 'PERFORMANCE',
                'severity': 'WARNING',
                'message': f"Cycle execution time {metrics['cycle_duration']:.1f}s exceeds threshold {self.thresholds['max_execution_time']}s"
            })
        
        # Alert 2: Consecutive failures
        if self.consecutive_failures >= 3:
            alerts.append({
                'type': 'RELIABILITY',
                'severity': 'CRITICAL',
                'message': f"ETL pipeline has failed {self.consecutive_failures} consecutive times"
            })
        
        # Alert 3: No data for multiple cycles
        if self.consecutive_no_data >= 5:
            alerts.append({
                'type': 'DATA_QUALITY',
                'severity': 'WARNING',
                'message': f"No new data processed for {self.consecutive_no_data} consecutive cycles"
            })
        
        # Alert 4: Low success rate
        if len(self.metrics_history) >= 10:
            recent_success_rate = sum(m['overall_success'] for m in self.metrics_history[-10:]) / 10
            if recent_success_rate < 0.8:  # Less than 80% success rate
                alerts.append({
                    'type': 'RELIABILITY',
                    'severity': 'WARNING',
                    'message': f"Success rate in last 10 cycles: {recent_success_rate*100:.1f}% (below 80% threshold)"
                })
        
        # Send alerts
        for alert in alerts:
            self._send_alert(alert, metrics)
    
    def _send_alert(self, alert: Dict[str, Any], metrics: Dict[str, Any]):
        """Send alert notification"""
        
        alert_id = f"{alert['type']}_{alert['severity']}_{int(time.time())}"
        
        # Avoid duplicate alerts
        recent_alerts = [a for a in self.alert_history if a['timestamp'] > datetime.now() - timedelta(hours=1)]
        if any(a['type'] == alert['type'] and a['severity'] == alert['severity'] for a in recent_alerts):
            return  # Skip duplicate alert
        
        # Record alert
        alert_record = {
            'id': alert_id,
            'timestamp': datetime.now(),
            'type': alert['type'],
            'severity': alert['severity'],
            'message': alert['message'],
            'metrics': metrics
        }
        self.alert_history.append(alert_record)
        
        # Send email alert
        if ProductionConfig.ENABLE_EMAIL_ALERTS:
            self._send_email_alert(alert_record)
        
        # Send Slack alert (if configured)
        if ProductionConfig.ENABLE_SLACK_ALERTS:
            self._send_slack_alert(alert_record)
        
        logger.warning(f"🚨 ALERT: {alert['severity']} - {alert['message']}")
    
    def _send_email_alert(self, alert: Dict[str, Any]):
        """Send email alert"""
        try:
            subject = f"🚨 Facolos ETL Alert - {alert['severity']}: {alert['type']}"
            
            body = f"""
            ETL Pipeline Alert
            ==================
            
            Severity: {alert['severity']}
            Type: {alert['type']}
            Time: {alert['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}
            
            Message: {alert['message']}
            
            Recent Metrics:
            - Cycle Duration: {alert['metrics']['cycle_duration']:.1f}s
            - Records Processed: {alert['metrics']['total_records']}
            - MISA Success Rate: {alert['metrics']['misa_success_rate']*100:.1f}%
            - TikTok Success: {alert['metrics']['tiktok_success']}
            - Consecutive Failures: {self.consecutive_failures}
            - Consecutive No Data: {self.consecutive_no_data}
            
            Please check the ETL pipeline immediately.
            
            Facolos ETL Monitoring System
            """
            
            # Note: Email sending would require SMTP configuration
            logger.info(f"📧 Email alert prepared: {subject}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send email alert: {e}")
    
    def _send_slack_alert(self, alert: Dict[str, Any]):
        """Send Slack alert"""
        try:
            # Note: Slack integration would require webhook setup
            logger.info(f"📱 Slack alert prepared: {alert['type']} - {alert['severity']}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send Slack alert: {e}")
    
    def get_performance_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get performance summary cho last N hours"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_metrics = [
            m for m in self.metrics_history 
            if datetime.fromisoformat(m['timestamp']) > cutoff_time
        ]
        
        if not recent_metrics:
            return {'error': 'No metrics available for the specified period'}
        
        total_cycles = len(recent_metrics)
        successful_cycles = sum(1 for m in recent_metrics if m['overall_success'])
        total_records = sum(m['total_records'] for m in recent_metrics)
        avg_duration = sum(m['cycle_duration'] for m in recent_metrics) / total_cycles
        
        return {
            'period_hours': hours,
            'total_cycles': total_cycles,
            'successful_cycles': successful_cycles,
            'success_rate': successful_cycles / total_cycles * 100,
            'total_records_processed': total_records,
            'avg_records_per_cycle': total_records / total_cycles if total_cycles > 0 else 0,
            'avg_cycle_duration': avg_duration,
            'consecutive_failures': self.consecutive_failures,
            'consecutive_no_data': self.consecutive_no_data,
            'last_successful_run': self.last_successful_run.isoformat() if self.last_successful_run else None,
            'alert_count': len([a for a in self.alert_history if a['timestamp'] > cutoff_time])
        }
    
    def generate_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive health report"""
        
        summary_24h = self.get_performance_summary(24)
        summary_1h = self.get_performance_summary(1)
        
        # Health status determination
        health_status = "HEALTHY"
        if self.consecutive_failures >= 3:
            health_status = "CRITICAL"
        elif self.consecutive_failures >= 1 or summary_1h.get('success_rate', 100) < 80:
            health_status = "WARNING"
        
        return {
            'timestamp': datetime.now().isoformat(),
            'health_status': health_status,
            'summary_24h': summary_24h,
            'summary_1h': summary_1h,
            'recent_alerts': [
                {
                    'type': a['type'],
                    'severity': a['severity'],
                    'message': a['message'],
                    'timestamp': a['timestamp'].isoformat()
                }
                for a in self.alert_history[-10:]  # Last 10 alerts
            ],
            'system_info': {
                'environment': 'production',
                'schedule_interval': f"{ProductionConfig.INCREMENTAL_SCHEDULE_MINUTES} minutes",
                'monitoring_enabled': True,
                'alerts_enabled': ProductionConfig.ENABLE_EMAIL_ALERTS
            }
        }

# Global monitor instance
production_monitor = ProductionETLMonitor()
