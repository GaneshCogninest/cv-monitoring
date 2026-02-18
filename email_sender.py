"""
Email Sender - Send HTML Formatted Daily Reports via SMTP

Sends beautifully formatted HTML emails with CV processing metrics.
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict
import logging

from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class EmailSender:
    """Send HTML formatted emails via SMTP"""

    def __init__(self):
        self.smtp_host = Config.SMTP_HOST
        self.smtp_port = Config.SMTP_PORT
        self.smtp_user = Config.SMTP_USER
        self.smtp_password = Config.SMTP_PASSWORD
        self.email_from = Config.EMAIL_FROM
        self.email_to = Config.EMAIL_TO

    def generate_html_report(self, report_data: Dict) -> str:
        """
        Generate HTML email body from report data.

        Args:
            report_data (dict): Report data from MetricsCollector

        Returns:
            str: HTML formatted email body
        """
        # Extract data
        date = report_data.get('report_date', 'Unknown')
        cvs_received = report_data.get('cvs_received', 0)
        cvs_rejected = report_data.get('cvs_rejected', 0)
        cvs_parsed = report_data.get('cvs_parsed_success', 0)
        cvs_parsing_failed = report_data.get('cvs_parsing_failed', 0)
        cvs_insertion_failed = report_data.get('cvs_insertion_failed', 0)
        total_failed = report_data.get('total_failed', 0)
        success_rate = report_data.get('success_rate', 0)
        failed_emails = report_data.get('failed_emails', [])
        pipeline = report_data.get('pipeline_status', {})

        # Determine status emoji and color
        if success_rate >= 95:
            status_emoji = "🟢"
            status_color = "#28a745"
            status_text = "Excellent"
        elif success_rate >= 85:
            status_emoji = "🟡"
            status_color = "#ffc107"
            status_text = "Good"
        elif success_rate >= 70:
            status_emoji = "🟠"
            status_color = "#fd7e14"
            status_text = "Needs Attention"
        else:
            status_emoji = "🔴"
            status_color = "#dc3545"
            status_text = "Critical"

        # Generate failed emails table
        failed_emails_html = ""
        if failed_emails:
            failed_emails_html = """
            <h3 style="color: #dc3545; margin-top: 30px;">📋 Failed CV Details</h3>
            <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
                <thead>
                    <tr style="background-color: #f8f9fa;">
                        <th style="padding: 12px; border: 1px solid #dee2e6; text-align: left;">Email</th>
                        <th style="padding: 12px; border: 1px solid #dee2e6; text-align: left;">Event</th>
                        <th style="padding: 12px; border: 1px solid #dee2e6; text-align: left;">Error Message</th>
                        <th style="padding: 12px; border: 1px solid #dee2e6; text-align: left;">Time</th>
                    </tr>
                </thead>
                <tbody>
            """

            for email_data in failed_emails[:Config.MAX_FAILED_EMAILS_IN_REPORT]:
                email = email_data.get('email', 'N/A')
                event_type = email_data.get('event_type', 'N/A')
                error_msg = email_data.get('error_message', 'No details available')
                timestamp = email_data.get('timestamp', 'N/A')

                # Truncate long error messages
                if error_msg and len(error_msg) > 100:
                    error_msg = error_msg[:100] + "..."

                # Format event type
                event_display = event_type.replace('_', ' ').title()

                failed_emails_html += f"""
                    <tr>
                        <td style="padding: 10px; border: 1px solid #dee2e6; font-size: 13px;">{email}</td>
                        <td style="padding: 10px; border: 1px solid #dee2e6; font-size: 13px;">{event_display}</td>
                        <td style="padding: 10px; border: 1px solid #dee2e6; font-size: 13px; color: #dc3545;">{error_msg}</td>
                        <td style="padding: 10px; border: 1px solid #dee2e6; font-size: 13px;">{timestamp}</td>
                    </tr>
                """

            failed_emails_html += """
                </tbody>
            </table>
            """

            if len(failed_emails) > Config.MAX_FAILED_EMAILS_IN_REPORT:
                failed_emails_html += f"""
                <p style="margin-top: 10px; color: #6c757d; font-size: 14px;">
                    ℹ️ Showing top {Config.MAX_FAILED_EMAILS_IN_REPORT} failures.
                    Total failures: {len(failed_emails)}
                </p>
                """
        else:
            failed_emails_html = """
            <div style="background-color: #d4edda; border: 1px solid #c3e6cb; border-radius: 4px;
                        padding: 15px; margin-top: 30px; color: #155724;">
                <strong>✅ No failures recorded yesterday!</strong>
            </div>
            """

        # Generate HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                     background-color: #f8f9fa; margin: 0; padding: 20px;">
            <div style="max-width: 800px; margin: 0 auto; background-color: white;
                        border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">

                <!-- Header -->
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            color: white; padding: 30px; border-radius: 8px 8px 0 0;">
                    <h1 style="margin: 0; font-size: 28px;">📊 Daily CV Processing Report</h1>
                    <p style="margin: 10px 0 0 0; font-size: 16px; opacity: 0.9;">
                        Date: {date}
                    </p>
                </div>

                <!-- Content -->
                <div style="padding: 30px;">

                    <!-- Overall Status -->
                    <div style="background-color: {status_color}; color: white;
                                padding: 20px; border-radius: 6px; margin-bottom: 30px;">
                        <h2 style="margin: 0; font-size: 24px;">
                            {status_emoji} Overall Status: {status_text}
                        </h2>
                        <p style="margin: 10px 0 0 0; font-size: 32px; font-weight: bold;">
                            {success_rate:.1f}% Success Rate
                        </p>
                    </div>

                    <!-- Summary Metrics -->
                    <h3 style="color: #495057; margin-bottom: 20px;">📈 Yesterday's Summary</h3>
                    <div style="display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 30px;">

                        <div style="flex: 1; min-width: 200px; background-color: #e7f3ff;
                                    border-left: 4px solid #2196F3; padding: 15px; border-radius: 4px;">
                            <div style="font-size: 14px; color: #666; margin-bottom: 5px;">CVs Received</div>
                            <div style="font-size: 28px; font-weight: bold; color: #2196F3;">{cvs_received:,}</div>
                        </div>

                        <div style="flex: 1; min-width: 200px; background-color: #e8f5e9;
                                    border-left: 4px solid #4CAF50; padding: 15px; border-radius: 4px;">
                            <div style="font-size: 14px; color: #666; margin-bottom: 5px;">Successfully Parsed</div>
                            <div style="font-size: 28px; font-weight: bold; color: #4CAF50;">{cvs_parsed:,}</div>
                        </div>

                        <div style="flex: 1; min-width: 200px; background-color: #ffebee;
                                    border-left: 4px solid #f44336; padding: 15px; border-radius: 4px;">
                            <div style="font-size: 14px; color: #666; margin-bottom: 5px;">Total Failed</div>
                            <div style="font-size: 28px; font-weight: bold; color: #f44336;">{total_failed:,}</div>
                        </div>
                    </div>

                    <!-- Failure Breakdown -->
                    <h3 style="color: #495057; margin-bottom: 15px;">❌ Failure Breakdown</h3>
                    <table style="width: 100%; border-collapse: collapse; margin-bottom: 30px;">
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: 600;">
                                Endpoint Rejections
                            </td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; text-align: right;
                                       font-weight: bold; color: #dc3545;">
                                {cvs_rejected:,}
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: 600;">
                                Parsing Failures
                            </td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; text-align: right;
                                       font-weight: bold; color: #dc3545;">
                                {cvs_parsing_failed:,}
                            </td>
                        </tr>
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: 600;">
                                Insertion Failures
                            </td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; text-align: right;
                                       font-weight: bold; color: #dc3545;">
                                {cvs_insertion_failed:,}
                            </td>
                        </tr>
                    </table>

                    <!-- Failed Emails List -->
                    {failed_emails_html}

                    <!-- Current Pipeline Status -->
                    <h3 style="color: #495057; margin-top: 40px; margin-bottom: 15px;">
                        ⏱️ Current Pipeline Status
                    </h3>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: 600;">
                                Unprocessed CVs (is_parsed = 0)
                            </td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; text-align: right;
                                       font-weight: bold; color: #17a2b8;">
                                {pipeline.get('unprocessed_cvs', 0):,}
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: 600;">
                                Active OpenAI Batches
                            </td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; text-align: right;
                                       font-weight: bold; color: #17a2b8;">
                                {pipeline.get('active_batches', 0):,}
                            </td>
                        </tr>
                        <tr style="background-color: #f8f9fa;">
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: 600;">
                                Pending Insertions
                            </td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; text-align: right;
                                       font-weight: bold; color: #17a2b8;">
                                {pipeline.get('pending_insertions', 0):,}
                            </td>
                        </tr>
                    </table>

                </div>

                <!-- Footer -->
                <div style="background-color: #f8f9fa; padding: 20px; border-radius: 0 0 8px 8px;
                            text-align: center; color: #6c757d; font-size: 14px;">
                    <p style="margin: 0;">
                        🤖 Generated by CV Monitoring System | {report_data.get('generated_at', 'N/A')}
                    </p>
                    <p style="margin: 5px 0 0 0;">
                        Powered by Hire Intelligence
                    </p>
                </div>

            </div>
        </body>
        </html>
        """
        return html

    def send_report(self, report_data: Dict) -> bool:
        """
        Send the daily report email.

        Args:
            report_data (dict): Report data from MetricsCollector

        Returns:
            bool: True if sent successfully, False otherwise
        """
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.email_from
            msg['To'] = self.email_to
            msg['Subject'] = (
                f"{Config.REPORT_SUBJECT_PREFIX} - "
                f"{report_data.get('report_date', 'Unknown')}"
            )

            # Generate HTML body
            html_body = self.generate_html_report(report_data)
            msg.attach(MIMEText(html_body, 'html'))

            # Connect to SMTP server and send
            logger.info(f"Connecting to SMTP server: {self.smtp_host}:{self.smtp_port}")
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.starttls()

            logger.info("Authenticating...")
            server.login(self.smtp_user, self.smtp_password)

            logger.info(f"Sending email to: {self.email_to}")
            server.send_message(msg)
            server.quit()

            logger.info("✅ Email sent successfully!")
            return True

        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication failed: {e}")
            return False

        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error sending email: {e}")
            return False


# Test the email sender
if __name__ == "__main__":
    print("=" * 60)
    print("EMAIL SENDER - TEST MODE")
    print("=" * 60)

    # Create test report data
    test_data = {
        'report_date': '2026-02-06',
        'cvs_received': 1250,
        'cvs_rejected': 15,
        'cvs_parsed_success': 1180,
        'cvs_parsing_failed': 40,
        'cvs_insertion_failed': 15,
        'total_failed': 70,
        'success_rate': 94.4,
        'failed_emails': [
            {
                'email': 'john@example.com',
                'event_type': 'cv_rejected',
                'error_message': 'A newer CV already exists',
                'timestamp': '2026-02-06 14:30:00'
            },
            {
                'email': 'jane@example.com',
                'event_type': 'cv_parsing_failed',
                'error_message': 'Invalid JSON format',
                'timestamp': '2026-02-06 15:45:00'
            }
        ],
        'pipeline_status': {
            'unprocessed_cvs': 1500,
            'active_batches': 3,
            'pending_insertions': 250
        },
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    sender = EmailSender()
    success = sender.send_report(test_data)

    if success:
        print("\n✅ Test email sent successfully!")
        print(f"Check inbox: {Config.EMAIL_TO}")
    else:
        print("\n❌ Failed to send test email")
