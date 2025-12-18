"""
Utility functions for the EasyTrack Dashboard application.

This module provides common utility functions including:
- Timestamp conversions and formatting
- File path handling
- Input validation helpers
- Hashing utilities
"""

import datetime
import hashlib
import os
import re
import time
from typing import Any, List, Optional, Union

import pytz

from tools import settings

# Constants
TIMEZONE = "Asia/Seoul"
MILLISECONDS_PER_SECOND = 1000
MICROSECONDS_PER_SECOND = 1_000_000
DEFAULT_FILE_PERMISSIONS = 0o777
NOT_AVAILABLE = "N/A"


def datetime_to_timestamp_ms(value: datetime.datetime) -> int:
    """
    Convert a datetime object to a Unix timestamp in milliseconds.

    Args:
        value: The datetime object to convert.

    Returns:
        Unix timestamp in milliseconds.
    """
    return int(round(value.timestamp() * MILLISECONDS_PER_SECOND))


def get_timestamp_hour(timestamp_ms: int) -> int:
    """
    Extract the hour component from a Unix timestamp in milliseconds.

    Args:
        timestamp_ms: Unix timestamp in milliseconds.

    Returns:
        Hour of the day (0-23).
    """
    return datetime.datetime.fromtimestamp(timestamp_ms / MILLISECONDS_PER_SECOND).hour


def get_timestamp_ms() -> int:
    """
    Get the current Unix timestamp in milliseconds.

    Returns:
        Current Unix timestamp in milliseconds.
    """
    return int(round(time.time() * MILLISECONDS_PER_SECOND))


def calculate_day_number(join_timestamp: int) -> int:
    """
    Calculate the number of days since a participant joined a campaign.

    Args:
        join_timestamp: The timestamp (in milliseconds) when the participant joined.

    Returns:
        Number of days since joining.
    """
    timezone = pytz.timezone(TIMEZONE)

    then = datetime.datetime.fromtimestamp(
        float(join_timestamp) / MILLISECONDS_PER_SECOND,
        tz=timezone,
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    then += datetime.timedelta(days=1)

    now = datetime.datetime.now(tz=timezone).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    now += datetime.timedelta(days=1)

    return (now - then).days


def timestamp_to_readable_string(timestamp_ms: Optional[int]) -> str:
    """
    Convert a Unix timestamp to a human-readable date/time string.

    Args:
        timestamp_ms: Unix timestamp in milliseconds, or None.

    Returns:
        Formatted string like "12/25 (Mon), 03:30 PM" or "N/A" if timestamp is invalid.
    """
    if timestamp_ms is None or timestamp_ms == 0:
        return NOT_AVAILABLE

    return datetime.datetime.fromtimestamp(
        float(timestamp_ms) / MILLISECONDS_PER_SECOND,
        tz=pytz.timezone(TIMEZONE),
    ).strftime("%m/%d (%a), %I:%M %p")


def timestamp_to_web_string(timestamp_ms: int) -> str:
    """
    Convert a Unix timestamp to an HTML datetime-local input format.

    Args:
        timestamp_ms: Unix timestamp in milliseconds.

    Returns:
        Formatted string like "2024-12-25T15:30".
    """
    date_time = datetime.datetime.fromtimestamp(
        float(timestamp_ms) / MILLISECONDS_PER_SECOND,
        tz=pytz.timezone(TIMEZONE),
    )
    date_part = f"{date_time.year}-{date_time.month:02d}-{date_time.day:02d}"
    time_part = f"{date_time.hour:02d}:{date_time.minute:02d}"
    return f"{date_part}T{time_part}"


def get_download_file_path(file_name: str) -> str:
    """
    Create and return a file path for downloading data.

    Creates the download directory if it doesn't exist, creates an empty file,
    and sets appropriate permissions.

    Args:
        file_name: Name of the file to create.

    Returns:
        Absolute path to the created file.
    """
    if not os.path.exists(settings.download_dir):
        os.mkdir(settings.download_dir)
        os.chmod(settings.download_dir, DEFAULT_FILE_PERMISSIONS)

    file_path = os.path.join(settings.download_dir, file_name)
    with open(file_path, "w+") as fp:
        pass  # Create empty file

    os.chmod(file_path, DEFAULT_FILE_PERMISSIONS)

    return file_path


def is_numeric(string: str, floating: bool = False) -> bool:
    """
    Check if a string represents a valid numeric value.

    Args:
        string: The string to validate.
        floating: If True, check for floating-point format; otherwise check for integer.

    Returns:
        True if the string is a valid numeric value, False otherwise.
    """
    if floating:
        pattern = r"^[+-]?\d+\.\d+$"
    else:
        pattern = r"^[+-]?\d+$"
    return re.search(pattern=pattern, string=string) is not None


def param_check(request_body: Any, params: List[str]) -> bool:
    """
    Validate that all required parameters exist in a request body.

    Args:
        request_body: The request body (dict-like object) to check.
        params: List of required parameter names.

    Returns:
        True if all parameters are present, False otherwise.
    """
    for param in params:
        if param not in request_body:
            return False
    return True


def now_us() -> int:
    """
    Get the current Unix timestamp in microseconds.

    Returns:
        Current Unix timestamp in microseconds.
    """
    return int(time.time() * MICROSECONDS_PER_SECOND)


def md5(value: str) -> str:
    """
    Calculate the MD5 hash of a string.

    Args:
        value: The string to hash.

    Returns:
        Hexadecimal MD5 hash digest.
    """
    return hashlib.md5(value.encode()).hexdigest()
