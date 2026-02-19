"""
Metrics Collector - Query CV Processing Events for Daily Reporting

Queries the cv_processing_log table to collect metrics for the daily report.
"""
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from typing import Dict, List
import logging

from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects CV processing metrics from the database"""

    def __init__(self):
        self.db_config = Config.DB_CONFIG

    def get_db_connection(self):
        """Create and return database connection"""
        try:
            return mysql.connector.connect(**self.db_config)
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def get_yesterday_metrics(self) -> Dict:
        """
        Get all metrics for yesterday (last 24 hours).

        Returns:
            dict: Metrics containing:
                - cvs_received: Number of CVs received
                - cvs_rejected: Number of CVs rejected
                - cvs_parsed_success: Number of CVs successfully parsed
                - cvs_parsing_failed: Number of CVs that failed parsing
                - cvs_insertion_failed: Number of CVs that failed insertion
                - total_failed: Total failures
                - success_rate: Success rate percentage
                - failed_emails: List of failed email details
        """
        connection = None
        cursor = None

        try:
            connection = self.get_db_connection()
            cursor = connection.cursor(dictionary=True)

            # Calculate yesterday's date range
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            metrics = {
                'report_date': yesterday.strftime('%Y-%m-%d'),
                'cvs_received': 0,
                'cvs_rejected': 0,
                'cvs_parsed_success': 0,
                'cvs_parsing_failed': 0,
                'cvs_insertion_failed': 0,
                'total_failed': 0,
                'cvs_in_progress': 0,
                'success_rate': 0.0,
                'failed_emails': []
            }

            # Get count for each event type
            query = """
                SELECT event_type, COUNT(*) as count
                FROM cv_processing_log
                WHERE DATE(timestamp) = %s
                GROUP BY event_type
            """
            cursor.execute(query, (yesterday,))
            results = cursor.fetchall()

            for row in results:
                event_type = row['event_type']
                count = row['count']

                if event_type == 'cv_received':
                    metrics['cvs_received'] = count
                elif event_type == 'cv_rejected':
                    metrics['cvs_rejected'] = count
                elif event_type == 'cv_parsed_success':
                    metrics['cvs_parsed_success'] = count
                elif event_type == 'cv_parsing_failed':
                    metrics['cvs_parsing_failed'] = count
                elif event_type == 'cv_insertion_failed':
                    metrics['cvs_insertion_failed'] = count

            # Calculate total failed
            metrics['total_failed'] = (
                metrics['cvs_rejected'] +
                metrics['cvs_parsing_failed'] +
                metrics['cvs_insertion_failed']
            )

            # Calculate success rate based on COMPLETED CVs only
            # (success + failed), ignoring still-in-progress CVs
            total_completed = (
                metrics['cvs_parsed_success'] +
                metrics['cvs_parsing_failed'] +
                metrics['cvs_insertion_failed']
            )
            if total_completed > 0:
                metrics['success_rate'] = (
                    metrics['cvs_parsed_success'] / total_completed * 100
                )
            elif metrics['cvs_received'] > 0:
                # CVs received but none completed yet → still in progress
                metrics['success_rate'] = 100.0

            # Calculate in-progress CVs
            # (received but not yet parsed success OR failed = still in pipeline)
            metrics['cvs_in_progress'] = max(0,
                metrics['cvs_received'] -
                metrics['cvs_parsed_success'] -
                metrics['total_failed']
            )

            # Get failed email details
            failed_query = """
                SELECT email, event_type, error_message, timestamp
                FROM cv_processing_log
                WHERE DATE(timestamp) = %s
                AND status = 'failed'
                ORDER BY timestamp DESC
                LIMIT %s
            """
            cursor.execute(
                failed_query,
                (yesterday, Config.MAX_FAILED_EMAILS_IN_REPORT)
            )
            metrics['failed_emails'] = cursor.fetchall()

            return metrics

        except Error as e:
            logger.error(f"Error collecting metrics: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()

    def get_current_pipeline_status(self) -> Dict:
        """
        Get current status of the CV processing pipeline.

        Returns:
            dict: Pipeline status containing:
                - unprocessed_cvs: Count of CVs with is_parsed = 0
                - active_batches: Count of active OpenAI batches
                - pending_insertions: Count of parsed CVs waiting for insertion
        """
        connection = None
        cursor = None

        try:
            connection = self.get_db_connection()
            cursor = connection.cursor(dictionary=True)

            status = {
                'unprocessed_cvs': 0,
                'active_batches': 0,
                'pending_insertions': 0
            }

            # Count unprocessed CVs
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM users
                WHERE is_parsed = 0 OR is_parsed IS NULL
            """)
            result = cursor.fetchone()
            status['unprocessed_cvs'] = result['count'] if result else 0

            # Count active batches
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM batch_tracking
                WHERE batch_status != 'completed'
                AND updated_at >= DATE_SUB(NOW(), INTERVAL 48 HOUR)
            """)
            result = cursor.fetchone()
            status['active_batches'] = result['count'] if result else 0

            # Count pending insertions
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM parsed_emails
                WHERE is_inserted = 0
                AND parsed_data IS NOT NULL
            """)
            result = cursor.fetchone()
            status['pending_insertions'] = result['count'] if result else 0

            return status

        except Error as e:
            logger.error(f"Error getting pipeline status: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()

    def get_full_report_data(self) -> Dict:
        """
        Get complete report data including metrics and pipeline status.

        Returns:
            dict: Complete report data
        """
        logger.info("Collecting metrics for daily report...")

        metrics = self.get_yesterday_metrics()
        pipeline_status = self.get_current_pipeline_status()

        report_data = {
            **metrics,
            'pipeline_status': pipeline_status,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        logger.info(f"Metrics collected: {metrics['cvs_received']} CVs received, "
                   f"{metrics['cvs_parsed_success']} successful, "
                   f"{metrics['total_failed']} failed")

        return report_data


# Test the metrics collector
if __name__ == "__main__":
    print("=" * 60)
    print("METRICS COLLECTOR - TEST MODE")
    print("=" * 60)

    try:
        collector = MetricsCollector()

        print("\n1. Collecting yesterday's metrics...")
        metrics = collector.get_yesterday_metrics()
        print(f"\n   CVs Received: {metrics['cvs_received']}")
        print(f"   CVs Rejected: {metrics['cvs_rejected']}")
        print(f"   CVs Parsed Successfully: {metrics['cvs_parsed_success']}")
        print(f"   CVs Parsing Failed: {metrics['cvs_parsing_failed']}")
        print(f"   CVs Insertion Failed: {metrics['cvs_insertion_failed']}")
        print(f"   Total Failed: {metrics['total_failed']}")
        print(f"   Success Rate: {metrics['success_rate']:.2f}%")
        print(f"   Failed Emails: {len(metrics['failed_emails'])} entries")

        print("\n2. Collecting pipeline status...")
        status = collector.get_current_pipeline_status()
        print(f"\n   Unprocessed CVs: {status['unprocessed_cvs']}")
        print(f"   Active Batches: {status['active_batches']}")
        print(f"   Pending Insertions: {status['pending_insertions']}")

        print("\n3. Getting full report data...")
        report_data = collector.get_full_report_data()
        print(f"\n   ✅ Report data collected successfully")
        print(f"   Generated at: {report_data['generated_at']}")

        print("\n" + "=" * 60)
        print("✅ Test completed successfully")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
