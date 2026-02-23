"""
CV Event Tracker - Helper Module for Logging CV Processing Events

SAFETY: This module is designed to NEVER break the main CV processing pipeline.
All database operations are wrapped in try-except blocks that log errors but don't raise exceptions.

Usage:
    from cv_event_tracker import log_cv_event

    # Log successful CV reception
    log_cv_event(
        email="john@example.com",
        event_type="cv_received",
        status="success",
        user_id="USER123"
    )

    # Log CV rejection
    log_cv_event(
        email="jane@example.com",
        event_type="cv_rejected",
        status="failed",
        error_message="A newer CV already exists",
        user_id="USER456"
    )
"""
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "host": "cvshortlistdb.cfyi6a2mqf7a.ap-south-1.rds.amazonaws.com",
    "user": "root",
    "password": "mysql12345",
    "database": "cv_shortlist",
}

def log_cv_event(
    email: str,
    event_type: str,
    status: str,
    user_id: str = None,
    error_message: str = None
):
    """
    Log a CV processing event to the cv_processing_log table.

    CRITICAL: This function MUST NOT raise exceptions or break the calling code.
    All errors are caught and logged, but never propagated.

    Args:
        email (str): Candidate's email address
        event_type (str): One of: cv_received, cv_rejected, cv_parsed_success,
                         cv_parsing_failed, cv_insertion_failed
        status (str): Either 'success' or 'failed'
        user_id (str, optional): User ID from the request
        error_message (str, optional): Error details if status is 'failed'

    Returns:
        bool: True if logged successfully, False otherwise
    """
    connection = None
    cursor = None

    try:
        # Validate event_type
        valid_events = [
            'cv_received',
            'cv_rejected',
            'cv_parsed_success',
            'cv_parsing_failed',
            'cv_insertion_failed',
            'activity_updated',
            'activity_update_failed'
        ]
        if event_type not in valid_events:
            logger.error(
                f"[EVENT_TRACKER] Invalid event_type: {event_type}. "
                f"Must be one of {valid_events}"
            )
            return False

        # Validate status
        if status not in ['success', 'failed']:
            logger.error(
                f"[EVENT_TRACKER] Invalid status: {status}. "
                f"Must be 'success' or 'failed'"
            )
            return False

        # Connect to database
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Insert event
        query = """
            INSERT INTO cv_processing_log
            (timestamp, email, event_type, status, error_message, user_id)
            VALUES (NOW(), %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (email, event_type, status, error_message, user_id))
        connection.commit()

        logger.info(
            f"[EVENT_TRACKER] Logged: {event_type} | {email} | {status}"
        )
        return True

    except Error as db_error:
        # Database errors should be logged but NEVER crash the main process
        logger.error(
            f"[EVENT_TRACKER] Database error (non-critical): {db_error} | "
            f"Event: {event_type} | Email: {email}"
        )
        return False

    except Exception as e:
        # ANY other error should also be caught and logged
        logger.error(
            f"[EVENT_TRACKER] Unexpected error (non-critical): {e} | "
            f"Event: {event_type} | Email: {email}"
        )
        return False

    finally:
        # Always clean up resources
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if connection and connection.is_connected():
            try:
                connection.close()
            except:
                pass


def log_bulk_events(events: list):
    """
    Log multiple events in a single transaction (more efficient for batch operations).

    CRITICAL: This function MUST NOT raise exceptions or break the calling code.

    Args:
        events (list): List of dicts with keys: email, event_type, status, user_id, error_message

    Returns:
        bool: True if logged successfully, False otherwise

    Example:
        events = [
            {
                'email': 'john@example.com',
                'event_type': 'cv_parsed_success',
                'status': 'success',
                'user_id': 'USER123'
            },
            {
                'email': 'jane@example.com',
                'event_type': 'cv_parsing_failed',
                'status': 'failed',
                'error_message': 'Invalid JSON',
                'user_id': 'USER456'
            }
        ]
        log_bulk_events(events)
    """
    connection = None
    cursor = None

    try:
        if not events or len(events) == 0:
            return True

        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        query = """
            INSERT INTO cv_processing_log
            (timestamp, email, event_type, status, error_message, user_id)
            VALUES (NOW(), %s, %s, %s, %s, %s)
        """

        # Prepare values for executemany
        values = []
        for event in events:
            values.append((
                event.get('email'),
                event.get('event_type'),
                event.get('status'),
                event.get('error_message'),
                event.get('user_id')
            ))

        cursor.executemany(query, values)
        connection.commit()

        logger.info(
            f"[EVENT_TRACKER] Bulk logged {len(events)} events successfully"
        )
        return True

    except Error as db_error:
        logger.error(
            f"[EVENT_TRACKER] Bulk logging database error (non-critical): {db_error}"
        )
        return False

    except Exception as e:
        logger.error(
            f"[EVENT_TRACKER] Bulk logging unexpected error (non-critical): {e}"
        )
        return False

    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if connection and connection.is_connected():
            try:
                connection.close()
            except:
                pass


# Test function
if __name__ == "__main__":
    print("=" * 60)
    print("CV EVENT TRACKER - TEST MODE")
    print("=" * 60)

    # Test 1: Log a successful CV received event
    print("\n1. Testing cv_received event...")
    result = log_cv_event(
        email="test@example.com",
        event_type="cv_received",
        status="success",
        user_id="TEST001"
    )
    print(f"   Result: {'✅ Success' if result else '❌ Failed'}")

    # Test 2: Log a CV rejection event
    print("\n2. Testing cv_rejected event...")
    result = log_cv_event(
        email="test@example.com",
        event_type="cv_rejected",
        status="failed",
        error_message="A newer CV already exists",
        user_id="TEST002"
    )
    print(f"   Result: {'✅ Success' if result else '❌ Failed'}")

    # Test 3: Test bulk logging
    print("\n3. Testing bulk event logging...")
    events = [
        {
            'email': 'bulk1@example.com',
            'event_type': 'cv_parsed_success',
            'status': 'success',
            'user_id': 'BULK001'
        },
        {
            'email': 'bulk2@example.com',
            'event_type': 'cv_parsing_failed',
            'status': 'failed',
            'error_message': 'Invalid JSON format',
            'user_id': 'BULK002'
        }
    ]
    result = log_bulk_events(events)
    print(f"   Result: {'✅ Success' if result else '❌ Failed'}")

    print("\n" + "=" * 60)
    print("Test completed. Check the cv_processing_log table.")
    print("=" * 60)
