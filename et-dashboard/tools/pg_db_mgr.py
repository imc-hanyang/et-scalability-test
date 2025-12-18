"""
PostgreSQL Database Manager for the EasyTrack Dashboard.

This module provides functions for interacting with PostgreSQL database,
including user management, campaign management, data source management,
data storage and retrieval, and statistics tracking.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import extras as psycopg2_extras

from tools import settings, utils

# Configure logging
logger = logging.getLogger(__name__)

# Database configuration
DB_HOST = "127.0.0.1"
DB_NAME = "easytrack_db"
DB_USER = "postgres"
DB_PASSWORD = "postgres"


# =============================================================================
# Common Database Operations
# =============================================================================


def get_db_connection():
    """
    Get or create a PostgreSQL database connection.

    Returns:
        The active database connection object.
    """
    if settings.db_conn is None:
        settings.db_conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        logger.info("Database initialized: %s", settings.db_conn)
    return settings.db_conn


def end() -> None:
    """Close the database connection."""
    get_db_connection().close()


def extract_value(
    row: Optional[Dict[str, Any]],
    column_name: str,
    default_value: Any = None,
) -> Any:
    """
    Safely extract a value from a database row.

    Args:
        row: The database row (dict-like) or None.
        column_name: The column name to extract.
        default_value: Value to return if row or column is None.

    Returns:
        The column value or the default value.
    """
    if row is None:
        return default_value
    elif row[column_name] is None:
        return default_value
    else:
        return row[column_name]


# =============================================================================
# 1. User Management
# =============================================================================


def create_user(id_token: str, name: str, email: str) -> Optional[Any]:
    """
    Create a new user in the database.

    Args:
        id_token: User's authentication token.
        name: User's display name.
        email: User's email address.

    Returns:
        The created user record.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'insert into "et"."user"("id_token", "name", "email") values (%s,%s,%s);',
        (id_token, name, email),
    )
    cur.close()
    get_db_connection().commit()
    return get_user(email=email)


def get_user(
    email: Optional[str] = None,
    user_id: Optional[int] = None,
) -> Optional[Any]:
    """
    Retrieve a user from the database.

    Args:
        email: The user's email address (optional).
        user_id: The user's ID (optional).

    Returns:
        The user record if found, None otherwise.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    row = None

    if user_id is not None and email is not None:
        cur.execute(
            'select * from "et"."user" where "id"=%s and "email"=%s;',
            (user_id, email),
        )
        row = cur.fetchone()
    elif user_id is not None:
        cur.execute('select * from "et"."user" where "id"=%s;', (user_id,))
        row = cur.fetchone()
    elif email is not None:
        cur.execute('select * from "et"."user" where "email"=%s;', (email,))
        row = cur.fetchone()

    cur.close()
    return row


def bind_participant_to_campaign(db_user, db_campaign) -> bool:
    """
    Bind a user to a campaign as a participant.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.

    Returns:
        True if this was a new binding, False if already bound.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select exists(select 1 from "stats"."campaign_participant_stats" '
        'where "campaign_id"=%s and "user_id"=%s);',
        (db_campaign["id"], db_user["id"]),
    )

    if cur.fetchone()[0]:
        cur.close()
        return False  # Already bound

    cur.execute(
        'insert into "stats"."campaign_participant_stats"'
        '("user_id", "campaign_id", "join_timestamp") values (%s,%s,%s) '
        'on conflict do nothing;',
        (db_user["id"], db_campaign["id"], utils.get_timestamp_ms()),
    )
    cur.execute(
        'update "et"."user" set "campaign_id" = %s where "id"=%s;',
        (db_campaign["id"], db_user["id"]),
    )
    cur.close()
    get_db_connection().commit()
    return True  # New binding


def user_is_bound_to_campaign(db_user, db_campaign) -> bool:
    """
    Check if a user is bound to a campaign as a participant.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.

    Returns:
        True if the user is a participant in the campaign, False otherwise.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select exists(select * from "stats"."campaign_participant_stats" '
        'where "user_id"=%s and "campaign_id"=%s) as "exists";',
        (db_user["id"], db_campaign["id"]),
    )
    exists = cur.fetchone()["exists"]
    cur.close()
    return exists


# =============================================================================
# 2. Campaign Management
# =============================================================================


def register_new_campaign(
    db_user_creator,
    name: str,
    notes: str,
    configurations: str,
    start_timestamp: int,
    end_timestamp: int,
    remove_inactive_users_timeout: int,
) -> None:
    """
    Register a new campaign.

    Args:
        db_user_creator: The user creating the campaign.
        name: Campaign name.
        notes: Campaign notes/description.
        configurations: JSON string of campaign configurations.
        start_timestamp: Campaign start time in milliseconds.
        end_timestamp: Campaign end time in milliseconds.
        remove_inactive_users_timeout: Timeout for removing inactive users.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'insert into "et"."campaign"'
        '("creator_id", "name", "notes", "config_json", "start_timestamp", '
        '"end_timestamp", "remove_inactive_users_timeout") '
        'values (%s,%s,%s,%s,%s,%s,%s);',
        (
            db_user_creator["id"],
            name,
            notes,
            configurations,
            start_timestamp,
            end_timestamp,
            remove_inactive_users_timeout,
        ),
    )
    cur.close()
    get_db_connection().commit()


def update_campaign(
    db_campaign,
    name: str,
    notes: str,
    configurations: str,
    start_timestamp: int,
    end_timestamp: int,
    remove_inactive_users_timeout: int,
) -> None:
    """
    Update an existing campaign.

    Args:
        db_campaign: The campaign record to update.
        name: New campaign name.
        notes: New campaign notes/description.
        configurations: New JSON string of campaign configurations.
        start_timestamp: New start time in milliseconds.
        end_timestamp: New end time in milliseconds.
        remove_inactive_users_timeout: New timeout value.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'update "et"."campaign" set "name" = %s, "notes" = %s, "config_json" = %s, '
        '"start_timestamp" = %s, "end_timestamp" = %s, "remove_inactive_users_timeout" = %s '
        'where "id"=%s;',
        (
            name,
            notes,
            configurations,
            start_timestamp,
            end_timestamp,
            remove_inactive_users_timeout,
            db_campaign["id"],
        ),
    )
    cur.close()
    get_db_connection().commit()


def get_campaign(
    campaign_id: int,
    db_creator_user=None,
) -> Optional[Any]:
    """
    Retrieve a campaign by ID.

    Args:
        campaign_id: The campaign ID.
        db_creator_user: Optional user to filter by creator.

    Returns:
        The campaign record if found, None otherwise.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

    if db_creator_user is None:
        cur.execute('select * from "et"."campaign" where "id"=%s;', (campaign_id,))
    else:
        cur.execute(
            'select * from "et"."campaign" where "id"=%s and "creator_id"=%s;',
            (campaign_id, db_creator_user["id"]),
        )

    row = cur.fetchone()
    cur.close()
    get_db_connection().commit()
    return row


def delete_campaign(db_campaign) -> None:
    """
    Delete a campaign.

    Args:
        db_campaign: The campaign record to delete.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute('delete from "et"."campaign" where id=%s;', (db_campaign["id"],))
    cur.close()
    get_db_connection().commit()


def get_campaigns(db_creator_user=None) -> List[Any]:
    """
    Get all campaigns, optionally filtered by creator.

    Args:
        db_creator_user: Optional user to filter campaigns by creator.

    Returns:
        List of campaign records.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

    if db_creator_user is None:
        cur.execute('select * from "et"."campaign";')
    else:
        cur.execute(
            'select * from "et"."campaign" where "creator_id"=%s;',
            (db_creator_user["id"],),
        )

    rows = cur.fetchall()
    cur.close()
    return rows


def get_campaign_participants_count(db_campaign=None) -> int:
    """
    Get the count of participants in a campaign.

    Args:
        db_campaign: The campaign record, or None for all users.

    Returns:
        Number of participants.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

    if db_campaign is None:
        cur.execute(
            'select count(*) as "participant_count" from "et"."user" where true;'
        )
    else:
        cur.execute(
            'select count(*) as "participant_count" from "et"."user" '
            'where "id" in (select "user_id" from "stats"."campaign_participant_stats" '
            'where "campaign_id"=%s);',
            (db_campaign["id"],),
        )

    participant_count = cur.fetchone()["participant_count"]
    cur.close()
    return participant_count


def get_campaign_participants(db_campaign=None) -> List[Any]:
    """
    Get all participants in a campaign.

    Args:
        db_campaign: The campaign record, or None for all users.

    Returns:
        List of user records for all participants.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

    if db_campaign is None:
        cur.execute('select * from "et"."user" where "id_token" is not null;')
    else:
        cur.execute(
            'select * from "et"."user" '
            'where "id" in (select "user_id" from "stats"."campaign_participant_stats" '
            'where "campaign_id"=%s);',
            (db_campaign["id"],),
        )

    rows = cur.fetchall()
    cur.close()
    return rows


# =============================================================================
# 3. Data Source Management
# =============================================================================


def register_data_source(
    db_creator_user,
    name: str,
    icon_name: str,
) -> int:
    """
    Register a new data source.

    Args:
        db_creator_user: The user creating the data source.
        name: Data source name.
        icon_name: Icon identifier for the data source.

    Returns:
        The ID of the created data source.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'insert into "et"."data_source"("creator_id", "name", "icon_name") '
        'values (%s,%s,%s) returning "id";',
        (db_creator_user["id"], name, icon_name),
    )
    data_source_id = cur.fetchone()["id"]
    cur.close()
    get_db_connection().commit()
    return data_source_id


def get_data_source(
    data_source_name: Optional[str] = None,
    data_source_id: Optional[int] = None,
) -> Optional[Any]:
    """
    Retrieve a data source by name or ID.

    Args:
        data_source_name: The data source name (optional).
        data_source_id: The data source ID (optional).

    Returns:
        The data source record if found, None otherwise.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    row = None

    if data_source_id is not None and data_source_name is not None:
        cur.execute(
            'select * from "et"."data_source" where "id"=%s and "name"=%s;',
            (data_source_id, data_source_name),
        )
        row = cur.fetchone()
    elif data_source_id is not None:
        cur.execute(
            'select * from "et"."data_source" where "id"=%s;', (data_source_id,)
        )
        row = cur.fetchone()
    elif data_source_name is not None:
        cur.execute(
            'select * from "et"."data_source" where "name"=%s;', (data_source_name,)
        )
        row = cur.fetchone()

    cur.close()
    return row


def get_data_source_id(data_source_name: str) -> Optional[int]:
    """
    Get the ID of a data source by name.

    Args:
        data_source_name: The data source name.

    Returns:
        The data source ID if found, None otherwise.
    """
    db_data_source = get_data_source(data_source_name=data_source_name)
    if db_data_source is not None:
        return db_data_source["id"]
    return None


def get_all_data_sources() -> List[Any]:
    """
    Get all data sources.

    Returns:
        List of all data source records.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute('select * from "et"."data_source";')
    rows = cur.fetchall()
    cur.close()
    return rows


def get_campaign_data_sources(db_campaign) -> List[Any]:
    """
    Get all data sources configured for a campaign.

    Args:
        db_campaign: The campaign record.

    Returns:
        List of data source records.
    """
    db_data_sources = []
    config_jsons = json.loads(s=db_campaign["config_json"])

    for config_json in config_jsons:
        db_data_source = get_data_source(data_source_id=config_json["data_source_id"])
        if db_data_source is not None:
            db_data_sources.append(db_data_source)

    return db_data_sources


# =============================================================================
# 4. Data Management
# =============================================================================


def fast_store_data_record(
    cur,
    user_id: int,
    campaign_id: int,
    data_source_id: int,
    timestamp: int,
    value: bytes,
) -> bool:
    """
    Quickly store a data record using an existing cursor.

    Args:
        cur: Database cursor.
        user_id: User ID.
        campaign_id: Campaign ID.
        data_source_id: Data source ID.
        timestamp: Record timestamp.
        value: Binary data value.

    Returns:
        True if the record was inserted, False if it already existed.
    """
    cur.execute(
        f'insert into "data"."{campaign_id}-{user_id}"'
        '("timestamp", "value", "data_source_id") values (%s,%s,%s) '
        'on conflict do nothing returning true;',
        (timestamp, psycopg2.Binary(value), data_source_id),
    )
    return cur.fetchone() is not None


def store_data_record(
    db_user,
    db_campaign,
    db_data_source,
    timestamp: int,
    value: bytes,
) -> None:
    """
    Store a single data record.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.
        db_data_source: The data source record.
        timestamp: Record timestamp.
        value: Binary data value.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    fast_store_data_record(
        cur=cur,
        user_id=db_user["id"],
        campaign_id=db_campaign["id"],
        data_source_id=db_data_source["id"],
        timestamp=timestamp,
        value=value,
    )
    cur.close()
    get_db_connection().commit()


def store_data_records(
    db_user,
    db_campaign,
    timestamp_list: List[int],
    data_source_id_list: List[int],
    value_list: List[bytes],
) -> None:
    """
    Store multiple data records.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.
        timestamp_list: List of record timestamps.
        data_source_id_list: List of data source IDs.
        value_list: List of binary data values.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    data_sources: Dict[int, Tuple[Any, int, int]] = {}

    for timestamp, data_source_id, value in zip(
        timestamp_list, data_source_id_list, value_list
    ):
        if data_source_id not in data_sources:
            tmp_db_data_source = get_data_source(data_source_id=data_source_id)
            if tmp_db_data_source is None:
                continue
            data_sources[data_source_id] = (tmp_db_data_source, 0, timestamp)

        db_data_source, amount, last_timestamp = data_sources[data_source_id]
        if db_data_source is not None:
            fast_store_data_record(
                cur=cur,
                user_id=db_user["id"],
                campaign_id=db_campaign["id"],
                data_source_id=db_data_source["id"],
                timestamp=timestamp,
                value=value,
            )

    cur.close()
    get_db_connection().commit()


def get_next_k_data_records(
    db_user,
    db_campaign,
    from_record_id: int,
    db_data_source,
    k: int,
) -> List[Any]:
    """
    Get the next k data records starting from a record ID.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.
        from_record_id: Starting record ID.
        db_data_source: The data source to filter by.
        k: Maximum number of records to return.

    Returns:
        List of data records.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        f'select * from "data"."{db_campaign["id"]}-{db_user["id"]}" '
        f'where "id">=%s and "data_source_id"=%s order by "id" limit({k});',
        (from_record_id, db_data_source["id"]),
    )
    k_records = cur.fetchall()
    cur.close()
    return k_records


def get_filtered_data_records(
    db_user,
    db_campaign,
    db_data_source,
    from_timestamp: int,
    till_timestamp: int = -1,
) -> List[Any]:
    """
    Get data records with optional timestamp filtering.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.
        db_data_source: The data source to filter by.
        from_timestamp: Minimum timestamp (inclusive).
        till_timestamp: Maximum timestamp (exclusive), -1 for no limit.

    Returns:
        List of data records ordered by timestamp.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    table_name = f'"data"."{db_campaign["id"]}-{db_user["id"]}"'

    if till_timestamp > 0:
        cur.execute(
            f'select * from {table_name} '
            f'where "data_source_id"=%s and "timestamp">=%s and "timestamp"<%s '
            f'order by "timestamp" asc;',
            (db_data_source["id"], from_timestamp, till_timestamp),
        )
    else:
        cur.execute(
            f'select * from {table_name} '
            f'where "data_source_id"=%s and "timestamp">=%s '
            f'order by "timestamp" asc limit 500;',
            (db_data_source["id"], from_timestamp),
        )

    data_records = cur.fetchall()
    cur.close()
    return data_records


def dump_data(db_campaign, db_user) -> str:
    """
    Export data to a binary file.

    Args:
        db_campaign: The campaign record.
        db_user: The user record.

    Returns:
        Path to the created file.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

    file_path = utils.get_download_file_path(
        f'{db_campaign["id"]}-{db_user["id"]}.bin.tmp'
    )
    cur.execute(
        f'copy (select "id", "timestamp", "value", "data_source_id" '
        f'from "data"."{db_campaign["id"]}-{db_user["id"]}") to %s with binary;',
        (file_path,),
    )

    cur.close()
    return file_path


def dump_csv_data(
    db_campaign,
    db_user=None,
    db_data_source=None,
) -> str:
    """
    Export data to a CSV file.

    Args:
        db_campaign: The campaign record.
        db_user: Optional user filter.
        db_data_source: Optional data source filter.

    Returns:
        Path to the created CSV file.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

    if db_user is not None:
        file_path = utils.get_download_file_path(
            f'campaign-{db_campaign["id"]} user-{db_user["id"]}.csv'
        )
        tmp_file_path = utils.get_download_file_path(
            f'{db_campaign["id"]}-{db_user["id"]}.csv'
        )
        cur.execute(
            f'copy (select "data_source_id", "timestamp", "value" '
            f'from "data"."{db_campaign["id"]}-{db_user["id"]}") '
            f"to %s delimiter ',' csv header;",
            (tmp_file_path,),
        )
        _convert_csv_hex_to_text(file_path, tmp_file_path)

    elif db_data_source is not None:
        file_path = utils.get_download_file_path(
            f'campaign-{db_campaign["id"]} data_source-{db_data_source["id"]}.csv'
        )
        participants = get_campaign_participants(db_campaign=db_campaign)

        for index, participant in enumerate(participants):
            sub_file_path = utils.get_download_file_path(
                f'{db_campaign["id"]}-{participant["id"]}.csv'
            )
            cur.execute(
                f'copy (select "timestamp", "value" '
                f'from "data"."{db_campaign["id"]}-{participant["id"]}" '
                f'where "data_source_id"={db_data_source["id"]}) '
                f"to %s delimiter ',' csv header;",
                (sub_file_path,),
            )
            _convert_csv_with_user_id(
                file_path, sub_file_path, participant["id"], is_first=(index == 0)
            )
            os.remove(sub_file_path)

    else:
        file_path = utils.get_download_file_path(f'campaign-{db_campaign["id"]}.csv')
        participants = get_campaign_participants(db_campaign=db_campaign)

        for index, participant in enumerate(participants):
            sub_file_path = utils.get_download_file_path(
                f'{db_campaign["id"]}-{participant["id"]}.csv'
            )
            cur.execute(
                f'copy (select "data_source_id", "timestamp", "value" '
                f'from "data"."{db_campaign["id"]}-{participant["id"]}") '
                f"to %s delimiter ',' csv header;",
                (sub_file_path,),
            )
            _convert_csv_with_user_id(
                file_path, sub_file_path, participant["id"], is_first=(index == 0)
            )
            os.remove(sub_file_path)

    cur.close()
    return file_path


def _convert_csv_hex_to_text(output_path: str, input_path: str) -> None:
    """
    Convert hex-encoded values in CSV to text.

    Args:
        output_path: Output file path.
        input_path: Input file path.
    """
    with open(output_path, "a") as writer, open(input_path, "r") as reader:
        rows = reader.readlines()
        writer.write(rows[0])  # Header
        for line in rows[1:]:
            cells = line[:-1].split(",")
            cells[-1] = str(bytes.fromhex(cells[-1][2:]), encoding="utf8")
            writer.write(f'{",".join(cells)}\n')


def _convert_csv_with_user_id(
    output_path: str,
    input_path: str,
    user_id: int,
    is_first: bool,
) -> None:
    """
    Convert CSV with user ID prepended to each row.

    Args:
        output_path: Output file path.
        input_path: Input file path.
        user_id: User ID to prepend.
        is_first: Whether this is the first file (write header).
    """
    with open(output_path, "a") as writer, open(input_path, "r") as reader:
        rows = reader.readlines()
        if is_first:
            writer.write(f"user_id,{rows[0]}")
        for line in rows[1:]:
            cells = line[:-1].split(",")
            cells[-1] = str(bytes.fromhex(cells[-1][2:]), encoding="utf8")
            writer.write(f'{user_id},{",".join(cells)}\n')


# =============================================================================
# 5. Communication Management
# =============================================================================


def create_direct_message(
    db_source_user,
    db_target_user,
    subject: str,
    content: str,
) -> None:
    """
    Create a direct message between users.

    Args:
        db_source_user: The sender's user record.
        db_target_user: The recipient's user record.
        subject: Message subject.
        content: Message content.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'insert into "et"."direct_message"'
        '("src_user_id", "target_user_id", "timestamp", "subject", "content") '
        'values (%s,%s,%s,%s,%s);',
        (
            db_source_user["id"],
            db_target_user["id"],
            utils.get_timestamp_ms(),
            subject,
            content,
        ),
    )
    cur.close()
    get_db_connection().commit()


def get_unread_direct_messages(db_user) -> List[Any]:
    """
    Get and mark as read all unread direct messages for a user.

    Args:
        db_user: The user record.

    Returns:
        List of unread message records.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select * from "et"."direct_message" '
        'where "target_user_id"=%s and "read"=FALSE;',
        (db_user["id"],),
    )
    rows = cur.fetchall()
    cur.execute(
        'update "et"."direct_message" set "read"=TRUE where trg_user_id=%s;',
        (db_user["id"],),
    )
    cur.close()
    get_db_connection().commit()
    return rows


def create_notification(
    db_target_user,
    db_campaign,
    timestamp: int,
    subject: str,
    content: str,
) -> None:
    """
    Create a notification for a user.

    Args:
        db_target_user: The target user record.
        db_campaign: The campaign record.
        timestamp: Notification timestamp.
        subject: Notification subject.
        content: Notification content.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'insert into "et"."notification"'
        '("target_user_id", "campaign_id", "timestamp", "subject", "content") '
        'values (%s,%s,%s,%s,%s)',
        (db_target_user["id"], db_campaign["id"], timestamp, subject, content),
    )
    cur.close()
    get_db_connection().commit()


def get_unread_notifications(db_user) -> List[Any]:
    """
    Get and mark as read all unread notifications for a user.

    Args:
        db_user: The user record.

    Returns:
        List of unread notification records.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select * from "et"."notification" '
        'where "target_user_id"=%s and "read"=FALSE;',
        (db_user["id"],),
    )
    rows = cur.fetchall()
    cur.execute(
        'update "et"."notification" set "read"=TRUE where "target_user_id"=%s;',
        (db_user["id"],),
    )
    cur.close()
    get_db_connection().commit()
    return rows


# =============================================================================
# 6. Statistics
# =============================================================================


def get_participant_join_timestamp(db_user, db_campaign) -> int:
    """
    Get the timestamp when a participant joined a campaign.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        Join timestamp in milliseconds.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select "join_timestamp" as "join_timestamp" '
        'from "stats"."campaign_participant_stats" '
        'where "user_id"=%s and "campaign_id"=%s;',
        (db_user["id"], db_campaign["id"]),
    )
    join_timestamp = cur.fetchone()["join_timestamp"]
    cur.close()
    return join_timestamp


def get_participant_last_sync_timestamp(db_user, db_campaign) -> int:
    """
    Get the most recent sync timestamp for a participant.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        Last sync timestamp in milliseconds, or 0 if none.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select max("sync_timestamp") as "last_sync_timestamp" '
        'from "stats"."per_data_source_stats" '
        'where "campaign_id"=%s and "user_id"=%s;',
        (db_campaign["id"], db_user["id"]),
    )
    last_sync_timestamp = extract_value(
        row=cur.fetchone(), column_name="last_sync_timestamp", default_value=0
    )
    cur.close()
    return last_sync_timestamp


def get_participant_heartbeat_timestamp(db_user, db_campaign) -> int:
    """
    Get the last heartbeat timestamp for a participant.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        Last heartbeat timestamp in milliseconds.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select "last_heartbeat_timestamp" as "last_heartbeat_timestamp" '
        'from "stats"."campaign_participant_stats" '
        'where "user_id" = %s and "campaign_id" = %s;',
        (db_user["id"], db_campaign["id"]),
    )
    last_heartbeat_timestamp = cur.fetchone()["last_heartbeat_timestamp"]
    cur.close()
    return last_heartbeat_timestamp


def get_participants_amount_of_data(db_user, db_campaign) -> int:
    """
    Get the total amount of data samples for a participant.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        Total number of data samples.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select sum("amount_of_samples") as "amount_of_samples" '
        'from "stats"."per_data_source_stats" '
        'where "campaign_id"=%s and "user_id"=%s;',
        (db_campaign["id"], db_user["id"]),
    )
    amount_of_samples = extract_value(
        row=cur.fetchone(), column_name="amount_of_samples", default_value=0
    )
    cur.close()
    return 0 if amount_of_samples is None else amount_of_samples


def get_participants_per_data_source_stats(
    db_user,
    db_campaign,
) -> List[Tuple[Any, int, int]]:
    """
    Get per-data-source statistics for a participant.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        List of tuples: (data_source, amount_of_samples, sync_timestamp).
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    db_data_sources = get_campaign_data_sources(db_campaign=db_campaign)
    res_stats = []

    for db_data_source in db_data_sources:
        cur.execute(
            'select "amount_of_samples" as "amount_of_samples" '
            'from "stats"."per_data_source_stats" '
            'where "campaign_id"=%s and "user_id"=%s and "data_source_id"=%s;',
            (db_campaign["id"], db_user["id"], db_data_source["id"]),
        )
        amount_of_samples = extract_value(
            row=cur.fetchone(), column_name="amount_of_samples", default_value=0
        )

        cur.execute(
            'select "sync_timestamp" as "sync_timestamp" '
            'from "stats"."per_data_source_stats" '
            'where "campaign_id"=%s and "user_id"=%s and "data_source_id"=%s;',
            (db_campaign["id"], db_user["id"], db_data_source["id"]),
        )
        sync_timestamp = extract_value(
            row=cur.fetchone(), column_name="sync_timestamp", default_value=0
        )

        res_stats.append((db_data_source, amount_of_samples, sync_timestamp))

    cur.close()
    return res_stats


def update_user_heartbeat_timestamp(db_user, db_campaign) -> None:
    """
    Update a participant's heartbeat timestamp to the current time.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'update "stats"."campaign_participant_stats" '
        'set "last_heartbeat_timestamp" = %s '
        'where "user_id" = %s and "campaign_id" = %s;',
        (utils.get_timestamp_ms(), db_user["id"], db_campaign["id"]),
    )
    cur.close()
    get_db_connection().commit()


def remove_participant_from_campaign(db_user, db_campaign) -> None:
    """
    Remove a participant from a campaign.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'delete from "stats"."campaign_participant_stats" '
        'where "user_id" = %s and "campaign_id" = %s;',
        (db_user["id"], db_campaign["id"]),
    )
    cur.close()
    get_db_connection().commit()


def get_participants_data_source_sync_timestamps(
    db_user,
    db_campaign,
    db_data_source,
) -> int:
    """
    Get the sync timestamp for a specific data source.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.
        db_data_source: The data source record.

    Returns:
        Sync timestamp in milliseconds, or 0 if none.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    cur.execute(
        'select "sync_timestamp" as "sync_timestamp" '
        'from "stats"."per_data_source_stats" '
        'where "campaign_id"=%s and "user_id"=%s and "data_source_id"=%s;',
        (db_campaign["id"], db_user["id"], db_data_source["id"]),
    )
    sync_timestamp = extract_value(
        row=cur.fetchone(), column_name="sync_timestamp", default_value=0
    )
    cur.close()
    get_db_connection().commit()
    return 0 if sync_timestamp is None else sync_timestamp


def get_filtered_amount_of_data(
    db_campaign,
    from_timestamp: int,
    till_timestamp: int,
    db_user=None,
    db_data_source=None,
) -> int:
    """
    Get the count of data records with optional filters.

    Args:
        db_campaign: The campaign to count data for.
        from_timestamp: Minimum timestamp (inclusive).
        till_timestamp: Maximum timestamp (exclusive).
        db_user: Optional user filter.
        db_data_source: Optional data source filter.

    Returns:
        Count of matching data records.
    """
    cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
    amount = 0

    if db_user is None:
        # All users
        participants = get_campaign_participants(db_campaign=db_campaign)

        if db_data_source is None:
            # All data sources
            for db_participant_user in participants:
                cur.execute(
                    f'select count(*) as "amount" '
                    f'from "data"."{db_campaign["id"]}-{db_participant_user["id"]}" '
                    f'where "timestamp">=%s and "timestamp"<%s;',
                    (from_timestamp, till_timestamp),
                )
                amount += cur.fetchone()["amount"]
        else:
            # Single data source
            for db_participant_user in participants:
                cur.execute(
                    f'select count(*) as "amount" '
                    f'from "data"."{db_campaign["id"]}-{db_participant_user["id"]}" '
                    f'where "data_source_id"=%s and "timestamp">=%s and "timestamp"<%s;',
                    (db_data_source["id"], from_timestamp, till_timestamp),
                )
                amount += cur.fetchone()["amount"]
    else:
        # Single user
        table_name = f'"data"."{db_campaign["id"]}-{db_user["id"]}"'

        if db_data_source is None:
            # All data sources
            cur.execute(
                f'select count(*) as "amount" from {table_name} '
                f'where "timestamp">=%s and "timestamp"<%s;',
                (from_timestamp, till_timestamp),
            )
            amount += cur.fetchone()["amount"]
        else:
            # Single data source
            cur.execute(
                f'select count(*) as "amount" from {table_name} '
                f'where "data_source_id"=%s and "timestamp">=%s and "timestamp"<%s;',
                (db_data_source["id"], from_timestamp, till_timestamp),
            )
            amount += cur.fetchone()["amount"]

    cur.close()
    get_db_connection().commit()
    return amount
