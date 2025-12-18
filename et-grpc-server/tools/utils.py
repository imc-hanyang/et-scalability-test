"""
Utility functions for the EasyTrack gRPC server.

This module provides helper functions for authentication, email handling,
time operations, and file management.
"""
import base64
import datetime
import hashlib
import logging
import os
import pickle
import time
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

from google.auth.transport import requests as oauth_requests
from google.auth.transport.requests import Request
from google.oauth2 import id_token as oauth_id_token
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import errors
from googleapiclient.discovery import build

from tools import db_mgr as db
from tools import settings

# Configure module logger
logger = logging.getLogger(__name__)


def get_credentials() -> Any:
    """
    Get or refresh Google OAuth credentials.

    Loads credentials from a pickle file if available, or initiates
    an OAuth flow to obtain new credentials.

    Returns:
        Google OAuth credentials object.
    """
    credentials = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "static/google_api_credentials.json",
                ["https://www.googleapis.com/auth/gmail.send"],
            )
            credentials = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as token:
            pickle.dump(credentials, token)
    return credentials


def get_timestamp_ms() -> int:
    """
    Get the current timestamp in milliseconds.

    Returns:
        Current Unix timestamp in milliseconds.
    """
    return int(round(time.time() * 1000))


def send_email(user_id: str, message: Dict[str, Any]) -> None:
    """
    Send an email message via Gmail API.

    Args:
        user_id: User's email address. The special value "me"
            can be used to indicate the authenticated user.
        message: Message dictionary containing the email data.
    """
    try:
        service = build(
            serviceName="gmail", version="v1", credentials=get_credentials()
        )
        sent_message = (
            service.users().messages().send(userId=user_id, body=message).execute()
        )
        logger.info("Message Id: %s", sent_message["id"])
        service.close()
    except errors.HttpError as error:
        logger.error("An error occurred while sending email: %s", error)


def create_message(
    sender: str, to: str, subject: str, message_text: str
) -> Dict[str, str]:
    """
    Create a message for an email.

    Args:
        sender: Email address of the sender.
        to: Email address of the receiver.
        subject: The subject of the email message.
        message_text: The text of the email message (HTML supported).

    Returns:
        A dictionary containing a base64url encoded email object.
    """
    message = MIMEText(message_text, "html")
    message["to"] = to
    message["from"] = sender
    message["subject"] = subject
    return {
        "raw": base64.urlsafe_b64encode(
            message.as_string().encode(encoding="utf8")
        ).decode(encoding="utf8")
    }


def get_problematic_users_email_message(
    destination_email_addresses: List[str],
    db_campaign: Any,
    db_participants: List[Any],
) -> List[Dict[str, str]]:
    """
    Generate email messages for problematic users notification.

    Args:
        destination_email_addresses: List of email addresses to send warnings to.
        db_campaign: Campaign database object.
        db_participants: List of participant database objects.

    Returns:
        List of email message dictionaries ready to be sent.
    """

    def _create_warning_row(
        user_id: int,
        email: str,
        name: str,
        day_no: int,
        amount_of_data: str,
        last_heartbeat_time: str,
        last_sync_time: str,
    ) -> str:
        """Create an HTML table row for a problematic user."""
        return f"""<tr>
                    <td class="id_column">{user_id}</td>
                    <td class="name_column" title="{email}">{name}</td>
                    <td class="duration_column">{day_no} days</td>
                    <td class="amount_column">{amount_of_data} samples</td>
                    <td class="heartbeat_column">{last_heartbeat_time}</td>
                    <td class="sync_time_column">{last_sync_time}</td>
                </tr>"""

    def _calculate_day_number(join_timestamp: int) -> int:
        """Calculate the number of days since joining."""
        then = datetime.datetime.fromtimestamp(float(join_timestamp) / 1000).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        then += datetime.timedelta(days=1)

        now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        now += datetime.timedelta(days=1)

        return (now - then).days

    def _timestamp_to_readable_string(timestamp_ms: int) -> str:
        """Convert timestamp to human-readable format."""
        if timestamp_ms == 0:
            return "N/A"
        return datetime.datetime.fromtimestamp(float(timestamp_ms) / 1000).strftime(
            "%m/%d (%a), %I:%M %p"
        )

    rows = []
    for db_user in db_participants:
        rows.append(
            _create_warning_row(
                user_id=db_user["id"],
                email=db_user["email"],
                name=db_user["name"],
                day_no=_calculate_day_number(
                    join_timestamp=db.get_participant_join_timestamp(
                        db_user=db_user, db_campaign=db_campaign
                    )
                ),
                amount_of_data=f"{db.get_participants_amount_of_data(db_user=db_user, db_campaign=db_campaign):,} samples",
                last_heartbeat_time=_timestamp_to_readable_string(
                    timestamp_ms=db.get_participant_heartbeat_timestamp(
                        db_user=db_user, db_campaign=db_campaign
                    )
                ),
                last_sync_time=_timestamp_to_readable_string(
                    timestamp_ms=db.get_participant_last_sync_timestamp(
                        db_user=db_user, db_campaign=db_campaign
                    )
                ),
            )
        )

    with open("static/problematic_users_email_template.html", "r") as template_file:
        template_content = template_file.read()
        message_text = template_content.replace(
            "campaign_html_element",
            f'<h3>Campaign : <a href="http://etdb.myvnc.com/campaign/?id={db_campaign["id"]}" style="text-decoration: none;">{db_campaign["name"]}</a></h3>',
        )
        message_text = message_text.replace("table_rows", ",".join(rows))

    return [
        create_message(
            sender="easytracknoreply@gmail.com",
            to=to,
            subject="[ET warning] problematic participants detected",
            message_text=message_text,
        )
        for to in destination_email_addresses
    ]


def get_missing_ema_email_messages(
    destination_email_addresses: List[str],
    db_campaign: Any,
    warn_users: List[tuple],
) -> List[Dict[str, str]]:
    """
    Generate email messages for missing EMA submissions.

    Args:
        destination_email_addresses: List of email addresses to send warnings to.
        db_campaign: Campaign database object.
        warn_users: List of tuples (db_user, db_data_source, amount_of_data, last_submission).

    Returns:
        List of email message dictionaries ready to be sent.
    """

    def _create_warning_row(
        user_id: int,
        email: str,
        name: str,
        data_source_name: str,
        amount_of_emas: int,
        last_submission: str,
    ) -> str:
        """Create an HTML table row for a missing EMA warning."""
        return f"""<tr>
                    <td>{user_id}</td>
                    <td title="{email}">{name}</td>
                    <td>{data_source_name}</td>
                    <td>{amount_of_emas}</td>
                    <td>≥{last_submission}</td>
                </tr>"""

    rows = []
    for db_user, db_data_source, amount_of_data, last_submission in warn_users:
        rows.append(
            _create_warning_row(
                user_id=db_user["id"],
                email=db_user["email"],
                name=db_user["name"],
                data_source_name=db_data_source["name"],
                amount_of_emas=amount_of_data,
                last_submission=last_submission,
            )
        )

    with open("static/missing_ema_email_template.html", "r") as template_file:
        template_content = template_file.read()
        message_text = template_content.replace(
            "campaign_html_element",
            f'<h3>Campaign : <a href="http://etdb.myvnc.com/campaign/?id={db_campaign["id"]}" style="text-decoration: none;">{db_campaign["name"]}</a></h3>',
        )
        message_text = message_text.replace("table_rows", "\n".join(rows))

    return [
        create_message(
            sender="easytracknoreply@gmail.com",
            to=to,
            subject="[ET warning] EMA submission issue(s) detected",
            message_text=message_text,
        )
        for to in destination_email_addresses
    ]


def validate(array: List[Any]) -> bool:
    """
    Check if all elements in the array are not None.

    Args:
        array: List of values to validate.

    Returns:
        True if no element is None, False otherwise.
    """
    return None not in array


def load_google_profile(id_token: str) -> Optional[Dict[str, str]]:
    """
    Load and verify a Google user profile from an ID token.

    Args:
        id_token: Google OAuth ID token.

    Returns:
        Dictionary containing 'id_token', 'name', and 'email' if valid,
        None if verification fails.
    """
    google_id_details = oauth_id_token.verify_oauth2_token(
        id_token=id_token, request=oauth_requests.Request()
    )
    if google_id_details["iss"] not in [
        "accounts.google.com",
        "https://accounts.google.com",
    ]:
        logger.warning("Google auth failure: wrong issuer")
        return None
    return {
        "id_token": id_token,
        "name": google_id_details["name"],
        "email": google_id_details["email"],
    }


def campaign_has_started(db_campaign: Any) -> bool:
    """
    Check if a campaign has started.

    Args:
        db_campaign: Campaign database object.

    Returns:
        True if the campaign start timestamp has passed, False otherwise.
    """
    return get_timestamp_ms() >= db_campaign.startTimestamp


def get_download_file_path(file_name: str) -> str:
    """
    Get the full path for a download file.

    Creates the download directory if it doesn't exist and sets
    appropriate permissions.

    Args:
        file_name: Name of the file to download.

    Returns:
        Full path to the download file.
    """
    if not os.path.exists(settings.download_dir):
        os.mkdir(settings.download_dir)
        os.chmod(settings.download_dir, 0o777)

    file_path = os.path.join(settings.download_dir, file_name)
    with open(file_path, "w+") as fp:
        pass  # Just create the file
    os.chmod(file_path, 0o777)

    return file_path


def now_us() -> int:
    """
    Get the current timestamp in microseconds.

    Returns:
        Current Unix timestamp in microseconds.
    """
    return int(time.time() * 1000 * 1000)


def md5(value: str) -> str:
    """
    Calculate MD5 hash of a string.

    Args:
        value: String to hash.

    Returns:
        Hexadecimal MD5 hash string.
    """
    return hashlib.md5(value.encode()).hexdigest()
