"""
Configuration File for CV Monitoring System

Contains database and email settings.
For production deployment, use environment variables instead of hardcoded values.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for CV Monitoring System"""

    # ==========================================
    # DATABASE CONFIGURATION
    # ==========================================
    DB_CONFIG = {
        "host": os.getenv("DB_HOST", "cvshortlistdb.cfyi6a2mqf7a.ap-south-1.rds.amazonaws.com"),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", "mysql12345"),
        "database": os.getenv("DB_NAME", "cv_shortlist"),
    }

    # ==========================================
    # EMAIL CONFIGURATION (SMTP)
    # ==========================================
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
    SMTP_USER = os.getenv("SMTP_USER", "ganeshnaikgan2000@gmail.com")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "cneccxfundunfyku")

    # Email addresses
    EMAIL_FROM = os.getenv("EMAIL_FROM", "ganeshnaikgan2000@gmail.com")
    EMAIL_TO = os.getenv("EMAIL_TO", "ganeshnaik1602@gmail.com")

    # Additional recipients (comma-separated in .env)
    EMAIL_CC = os.getenv("EMAIL_CC", "")  # Optional CC recipients
    EMAIL_BCC = os.getenv("EMAIL_BCC", "")  # Optional BCC recipients

    # ==========================================
    # REPORT CONFIGURATION
    # ==========================================
    REPORT_SUBJECT_PREFIX = os.getenv("REPORT_SUBJECT_PREFIX", "Daily CV Processing Report")
    REPORT_TIMEZONE = os.getenv("REPORT_TIMEZONE", "Asia/Kolkata")

    # Maximum number of failed emails to show in report
    MAX_FAILED_EMAILS_IN_REPORT = int(os.getenv("MAX_FAILED_EMAILS_IN_REPORT", 50))

    # ==========================================
    # LOGGING CONFIGURATION
    # ==========================================
    LOG_DIR = os.getenv("LOG_DIR", "/home/ganesh/cv_monitoring/logs")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def get_email_recipients(cls):
        """
        Get all email recipients as a list.

        Returns:
            list: All email recipients (TO, CC, BCC)
        """
        recipients = [cls.EMAIL_TO]

        if cls.EMAIL_CC:
            recipients.extend([email.strip() for email in cls.EMAIL_CC.split(",")])

        if cls.EMAIL_BCC:
            recipients.extend([email.strip() for email in cls.EMAIL_BCC.split(",")])

        return recipients

    @classmethod
    def validate_config(cls):
        """
        Validate that all required configuration is present.

        Returns:
            tuple: (is_valid, error_message)
        """
        required_fields = {
            "SMTP_HOST": cls.SMTP_HOST,
            "SMTP_PORT": cls.SMTP_PORT,
            "SMTP_USER": cls.SMTP_USER,
            "SMTP_PASSWORD": cls.SMTP_PASSWORD,
            "EMAIL_FROM": cls.EMAIL_FROM,
            "EMAIL_TO": cls.EMAIL_TO,
        }

        missing_fields = []
        for field, value in required_fields.items():
            if not value:
                missing_fields.append(field)

        if missing_fields:
            return False, f"Missing required configuration: {', '.join(missing_fields)}"

        return True, "Configuration is valid"

    @classmethod
    def print_config(cls):
        """Print current configuration (masks sensitive data)"""
        print("=" * 60)
        print("CV MONITORING - CURRENT CONFIGURATION")
        print("=" * 60)
        print("\n[Database]")
        print(f"  Host: {cls.DB_CONFIG['host']}")
        print(f"  Database: {cls.DB_CONFIG['database']}")
        print(f"  User: {cls.DB_CONFIG['user']}")
        print(f"  Password: {'*' * len(cls.DB_CONFIG['password'])}")

        print("\n[SMTP]")
        print(f"  Host: {cls.SMTP_HOST}")
        print(f"  Port: {cls.SMTP_PORT}")
        print(f"  User: {cls.SMTP_USER}")
        print(f"  Password: {'*' * len(cls.SMTP_PASSWORD)}")

        print("\n[Email]")
        print(f"  From: {cls.EMAIL_FROM}")
        print(f"  To: {cls.EMAIL_TO}")
        if cls.EMAIL_CC:
            print(f"  CC: {cls.EMAIL_CC}")
        if cls.EMAIL_BCC:
            print(f"  BCC: {cls.EMAIL_BCC}")

        print("\n[Report Settings]")
        print(f"  Subject Prefix: {cls.REPORT_SUBJECT_PREFIX}")
        print(f"  Timezone: {cls.REPORT_TIMEZONE}")
        print(f"  Max Failed Emails: {cls.MAX_FAILED_EMAILS_IN_REPORT}")

        print("\n[Logging]")
        print(f"  Log Directory: {cls.LOG_DIR}")
        print(f"  Log Level: {cls.LOG_LEVEL}")
        print("=" * 60)


# Test configuration on import
if __name__ == "__main__":
    Config.print_config()
    is_valid, message = Config.validate_config()
    print(f"\n{'✅' if is_valid else '❌'} Configuration: {message}")
