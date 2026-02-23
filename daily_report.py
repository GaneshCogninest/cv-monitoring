#!/usr/bin/env python3
"""
Daily CV Processing Report Generator

Main script that runs daily (via cron) to:
1. Collect metrics from cv_processing_log table
2. Generate HTML report
3. Send email to configured recipients

This script should be run at 1:00 AM daily via cron:
    0 1 * * * /usr/bin/python3 /home/cv_monitoring/daily_report.py

Author: CV Monitoring System
"""
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from metrics_collector import MetricsCollector
from email_sender import EmailSender

# Setup logging — use root logger directly to override any handlers
# set by imported modules (metrics_collector, email_sender, etc.)
log_dir = Path(Config.LOG_DIR)
log_dir.mkdir(exist_ok=True)

log_file = log_dir / f"daily_report_{datetime.now().strftime('%Y%m%d')}.log"

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, Config.LOG_LEVEL))
root_logger.handlers.clear()   # remove handlers set by imported modules
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)


def main():
    """Main execution function"""
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("DAILY CV PROCESSING REPORT - STARTED")
    logger.info("=" * 60)
    logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Step 1: Validate configuration
        logger.info("\n[1/4] Validating configuration...")
        is_valid, message = Config.validate_config()
        if not is_valid:
            logger.error(f"Configuration validation failed: {message}")
            sys.exit(1)
        logger.info("✅ Configuration is valid")

        # Step 2: Collect metrics
        logger.info("\n[2/4] Collecting metrics from database...")
        collector = MetricsCollector()
        report_data = collector.get_full_report_data()
        logger.info(f"✅ Metrics collected for date: {report_data['report_date']}")
        logger.info(f"   - CVs Received: {report_data['cvs_received']}")
        logger.info(f"   - CVs Parsed: {report_data['cvs_parsed_success']}")
        logger.info(f"   - Total Failed: {report_data['total_failed']}")
        logger.info(f"   - Success Rate: {report_data['success_rate']:.2f}%")
        logger.info(f"   - Activity Updates: {report_data['activity_updated']}")
        logger.info(f"   - Activity Update Failures: {report_data['activity_update_failed']}")

        # Step 3: Generate and send email
        logger.info("\n[3/4] Generating and sending email report...")
        sender = EmailSender()
        success = sender.send_report(report_data)

        if not success:
            logger.error("❌ Failed to send email report")
            sys.exit(1)

        logger.info(f"✅ Email sent successfully to: {Config.EMAIL_TO}")

        # Step 4: Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n[4/4] Report generation completed")
        logger.info("=" * 60)
        logger.info("DAILY CV PROCESSING REPORT - COMPLETED")
        logger.info("=" * 60)
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Log file: {log_file}")

        # Exit with success
        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("\n⚠️ Report generation interrupted by user")
        sys.exit(130)

    except Exception as e:
        logger.error(f"\n❌ FATAL ERROR: {e}", exc_info=True)
        logger.error("Report generation failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
