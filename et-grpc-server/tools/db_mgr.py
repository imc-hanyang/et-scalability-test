"""
Database manager for Cassandra operations.

This module provides functions for interacting with the Cassandra database,
including user management, campaign management, data source management,
data storage, communication, and statistics.
"""
import json
import logging
import os
import time
from os.path import exists
from typing import Any, List, Optional, Tuple

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from dotenv import load_dotenv

from tools import settings, utils

# Load environment variables
load_dotenv()

# Configure module logger
DEFAULT_CONTACT_POINTS = os.getenv("CASSANDRA_HOST", "127.0.0.1").split(",")
logger = logging.getLogger(__name__)


# ==============================================================================
# Database Connection Management
# ==============================================================================


def get_cassandra_session(maintenance: bool = False) -> Optional[Any]:
    """
    Get or create a Cassandra database session.

    Args:
        maintenance: If True, creates a separate maintenance session.
            Defaults to False.

    Returns:
        Cassandra session object, or None if connection fails.
    """
    if settings.cassandra_cluster is None:
        settings.cassandra_cluster = Cluster(
            contact_points=DEFAULT_CONTACT_POINTS,
            executor_threads=2048,
            connect_timeout=1200,
        )
        settings.cassandra_session = settings.cassandra_cluster.connect()
        logger.info("Cassandra session initialized: %s", settings.cassandra_session)
    return settings.cassandra_session


def end() -> None:
    """
    Close the Cassandra database connection.

    Shuts down both the session and the cluster.
    """
    if settings.cassandra_session:
        settings.cassandra_session.shutdown()
    if settings.cassandra_cluster:
        settings.cassandra_cluster.shutdown()
    logger.info("Cassandra connection closed")


def get_next_id(session: Any, table_name: str) -> int:
    """
    Get the next available ID for a table.

    Args:
        session: Cassandra session object.
        table_name: Fully qualified table name (e.g., '"et"."user"').

    Returns:
        Next available ID (max ID + 1, or 0 if table is empty).
    """
    res = session.execute(f'select max("id") from {table_name};')
    last_id = res.one()[0]
    return 0 if last_id is None else last_id + 1


# ==============================================================================
# User Management
# ==============================================================================


def create_user(name: str, email: str, session_key: str) -> Optional[Any]:
    """
    Create a new user in the database.

    Args:
        name: User's display name.
        email: User's email address.
        session_key: User's session key (password hash).

    Returns:
        The created user record, or None if creation failed.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."user"')
    session.execute(
        'insert into "et"."user"("id", "email", "sessionKey", "name") values (%s,%s,%s,%s);',
        (next_id, email, session_key, name),
    )
    return session.execute(
        'select * from "et"."user" where "id"=%s;', (next_id,)
    ).one()


def get_user(
    user_id: Optional[int] = None, email: Optional[str] = None
) -> Optional[Any]:
    """
    Get a user from the database.

    Args:
        user_id: User's ID. Optional.
        email: User's email address. Optional.

    Returns:
        User record if found, None otherwise.
    """
    session = get_cassandra_session()
    db_user = None

    if user_id is not None and email is not None:
        db_user = session.execute(
            'select * from "et"."user" where "id"=%s and "email"=%s allow filtering;',
            (user_id, email),
        ).one()
    elif user_id is not None:
        db_user = session.execute(
            'select * from "et"."user" where "id"=%s allow filtering;', (user_id,)
        ).one()
    elif email is not None:
        db_user = session.execute(
            'select * from "et"."user" where "email"=%s allow filtering;', (email,)
        ).one()

    return db_user


def set_user_tag(db_user: Any, tag: str = "") -> None:
    """
    Set a tag for a user.

    Args:
        db_user: User database object.
        tag: Tag string to set. Defaults to empty string.
    """
    session = get_cassandra_session()
    session.execute(
        'update "et"."user" set "tag"=%s where "id"=%s;', (tag, db_user.id)
    )


def update_session_key(db_user: Any, session_key: str) -> None:
    """
    Update a user's session key.

    Args:
        db_user: User database object.
        session_key: New session key to set.
    """
    session = get_cassandra_session()
    session.execute(
        'update "et"."user" set "sessionKey" = %s where "id" = %s and "email" = %s;',
        (session_key, db_user.id, db_user.email),
    )


def user_is_bound_to_campaign(db_user: Any, db_campaign: Any) -> bool:
    """
    Check if a user is bound to a campaign.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.

    Returns:
        True if the user is bound to the campaign, False otherwise.
    """
    session = get_cassandra_session()
    count = session.execute(
        'select count(*) from "stats"."campaignParticipantStats" '
        'where "campaignId"=%s and "userId"=%s allow filtering;',
        (db_campaign.id, db_user.id),
    ).one()[0]
    return count > 0


def bind_participant_to_campaign(db_user: Any, db_campaign: Any) -> bool:
    """
    Bind a user to a campaign as a participant.

    Creates the participant stats record and data table if this is a new binding.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.

    Returns:
        True if this is a new binding, False if already bound.
    """
    session = get_cassandra_session()
    if not user_is_bound_to_campaign(db_user=db_user, db_campaign=db_campaign):
        session.execute(
            'insert into "stats"."campaignParticipantStats"'
            '("userId", "campaignId", "joinTimestamp") values (%s,%s,%s);',
            (db_user.id, db_campaign.id, utils.get_timestamp_ms()),
        )
        session.execute(
            f'create table if not exists "data"."cmp{db_campaign.id}_usr{db_user.id}"'
            '("dataSourceId" int, "timestamp" bigint, "value" blob, '
            'primary key ("dataSourceId", "timestamp"));'
        )
        return True  # New binding
    return False  # Already bound


def get_campaign_participants(db_campaign: Any) -> List[Any]:
    """
    Get all participants of a campaign.

    Args:
        db_campaign: Campaign database object.

    Returns:
        List of user database objects for all participants.
    """
    session = get_cassandra_session()
    participants = []

    for row in session.execute(
        'select "userId" from "stats"."campaignParticipantStats" '
        'where "campaignId"=%s allow filtering;',
        (db_campaign.id,),
    ).all():
        user = get_user(user_id=row.userId)
        if user is not None:
            participants.append(user)

    return participants


def get_campaign_researchers(db_campaign: Any) -> List[Any]:
    """
    Get all researchers of a campaign.

    Args:
        db_campaign: Campaign database object.

    Returns:
        List of user database objects for all researchers.
    """
    session = get_cassandra_session()
    researchers = []

    for row in session.execute(
        'select "researcherId" from "et"."campaignResearchers" '
        'where "campaignId"=%s allow filtering;',
        (db_campaign.id,),
    ).all():
        user = get_user(user_id=row.researcherId)
        if user is not None:
            researchers.append(user)

    return researchers


def get_campaign_participants_count(db_campaign: Any) -> int:
    """
    Get the number of participants in a campaign.

    Args:
        db_campaign: Campaign database object.

    Returns:
        Number of participants.
    """
    session = get_cassandra_session()
    return len(
        session.execute(
            'select "userId" from "stats"."campaignParticipantStats" '
            'where "campaignId"=%s allow filtering;',
            (db_campaign.id,),
        ).all()
    )


def add_researcher_to_campaign(db_campaign: Any, db_researcher_user: Any) -> None:
    """
    Add a researcher to a campaign.

    Args:
        db_campaign: Campaign database object.
        db_researcher_user: Researcher user database object.
    """
    session = get_cassandra_session()
    session.execute(
        'insert into "et"."campaignResearchers"("campaignId", "researcherId") '
        "values(%s,%s);",
        (db_campaign.id, db_researcher_user.id),
    )


def remove_researcher_from_campaign(db_campaign: Any, db_researcher_user: Any) -> None:
    """
    Remove a researcher from a campaign.

    Args:
        db_campaign: Campaign database object.
        db_researcher_user: Researcher user database object.
    """
    session = get_cassandra_session()
    session.execute(
        'delete from "et"."campaignResearchers" '
        'where "campaignId"=%s and "researcherId"=%s;',
        (db_campaign.id, db_researcher_user.id),
    )


# ==============================================================================
# Campaign Management
# ==============================================================================


def create_or_update_campaign(
    db_creator_user: Any,
    name: str,
    notes: str,
    configurations: str,
    start_timestamp: int,
    end_timestamp: int,
    db_campaign: Optional[Any] = None,
) -> Optional[Any]:
    """
    Create a new campaign or update an existing one.

    Args:
        db_creator_user: Creator user database object.
        name: Campaign name.
        notes: Campaign notes/description.
        configurations: Campaign configuration JSON string.
        start_timestamp: Campaign start timestamp in milliseconds.
        end_timestamp: Campaign end timestamp in milliseconds.
        db_campaign: Existing campaign to update. If None, creates a new campaign.

    Returns:
        The created or updated campaign database object.
    """
    session = get_cassandra_session()

    if db_campaign is None:
        # Create new campaign
        next_id = get_next_id(session=session, table_name='"et"."campaign"')
        session.execute(
            'insert into "et"."campaign"'
            '("id", "creatorId", "name", "notes", "configJson", '
            '"startTimestamp", "endTimestamp") values (%s,%s,%s,%s,%s,%s,%s);',
            (
                next_id,
                db_creator_user.id,
                name,
                notes,
                configurations,
                start_timestamp,
                end_timestamp,
            ),
        )
        return get_campaign(campaign_id=next_id, db_researcher_user=db_creator_user)

    elif db_campaign.creatorId == db_creator_user.id:
        # Update existing campaign
        session.execute(
            'update "et"."campaign" set "name" = %s, "notes" = %s, '
            '"configJson" = %s, "startTimestamp" = %s, "endTimestamp" = %s '
            'where "creatorId"=%s and "id"=%s;',
            (
                name,
                notes,
                configurations,
                start_timestamp,
                end_timestamp,
                db_creator_user.id,
                db_campaign.id,
            ),
        )
        return db_campaign

    return None


def get_campaign(
    campaign_id: int, db_researcher_user: Optional[Any] = None
) -> Optional[Any]:
    """
    Get a campaign from the database.

    Args:
        campaign_id: Campaign ID.
        db_researcher_user: If provided, verifies the user has access. Optional.

    Returns:
        Campaign database object if found, None otherwise.
    """
    session = get_cassandra_session()

    if db_researcher_user is None:
        db_campaign = session.execute(
            'select * from "et"."campaign" where "id"=%s allow filtering;',
            (campaign_id,),
        ).one()
    else:
        db_campaign = session.execute(
            'select * from "et"."campaign" '
            'where "id"=%s and "creatorId"=%s allow filtering;',
            (campaign_id, db_researcher_user.id),
        ).one()

        if db_campaign is None:
            # Check if user is a researcher
            is_researcher = (
                session.execute(
                    'select count(*) from "et"."campaignResearchers" '
                    'where "campaignId"=%s and "researcherId"=%s;',
                    (campaign_id, db_researcher_user.id),
                ).one()[0]
                > 0
            )
            if is_researcher:
                db_campaign = get_campaign(campaign_id=campaign_id)

    return db_campaign


def delete_campaign(db_campaign: Any) -> None:
    """
    Delete a campaign from the database.

    Args:
        db_campaign: Campaign database object to delete.
    """
    session = get_cassandra_session()
    session.execute(
        'delete from "et"."campaign" where "creatorId"=%s and "id"=%s;',
        (db_campaign.creatorId, db_campaign.id),
    )


def get_campaigns(
    db_creator_user: Optional[Any] = None, active_only: bool = False
) -> List[Any]:
    """
    Get campaigns from the database.

    Args:
        db_creator_user: If provided, only returns campaigns by this creator.
        active_only: If True, only returns campaigns that haven't ended.

    Returns:
        List of campaign database objects.
    """
    session = get_cassandra_session()

    if db_creator_user is None:
        db_campaigns = session.execute('select * from "et"."campaign";').all()
    else:
        db_campaigns = session.execute(
            'select * from "et"."campaign" where "creatorId"=%s allow filtering;',
            (db_creator_user.id,),
        ).all()

    if active_only:
        now_ts = int(time.time() * 1000)
        db_campaigns = [c for c in db_campaigns if c.endTimestamp > now_ts]

    return db_campaigns


def get_researcher_campaigns(db_researcher_user: Any) -> List[Any]:
    """
    Get all campaigns where a user is a researcher.

    Args:
        db_researcher_user: Researcher user database object.

    Returns:
        List of campaign database objects.
    """
    session = get_cassandra_session()
    campaigns = []

    for row in session.execute(
        'select "campaignId" from "et"."campaignResearchers" '
        'where "researcherId"=%s allow filtering;',
        (db_researcher_user.id,),
    ).all():
        campaign = get_campaign(campaign_id=row.campaignId)
        if campaign is not None:
            campaigns.append(campaign)

    return campaigns


# ==============================================================================
# Data Source Management
# ==============================================================================


def create_data_source(
    db_creator_user: Any, name: str, icon_name: str
) -> Optional[Any]:
    """
    Create a new data source.

    Args:
        db_creator_user: Creator user database object.
        name: Data source name.
        icon_name: Icon filename for the data source.

    Returns:
        The created data source database object.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."dataSource"')
    session.execute(
        'insert into "et"."dataSource"("id", "creatorId", "name", "iconName") '
        "values (%s,%s,%s,%s);",
        (next_id, db_creator_user.id, name, icon_name),
    )
    return get_data_source(data_source_id=next_id)


def get_data_source(
    data_source_name: Optional[str] = None, data_source_id: Optional[int] = None
) -> Optional[Any]:
    """
    Get a data source from the database.

    Args:
        data_source_name: Data source name. Optional.
        data_source_id: Data source ID. Optional.

    Returns:
        Data source database object if found, None otherwise.
    """
    session = get_cassandra_session()
    db_data_source = None

    if data_source_id is not None and data_source_name is not None:
        db_data_source = session.execute(
            'select * from "et"."dataSource" '
            'where "id"=%s and "name"=%s allow filtering;',
            (data_source_id, data_source_name),
        ).one()
    elif data_source_id is not None:
        db_data_source = session.execute(
            'select * from "et"."dataSource" where "id"=%s allow filtering;',
            (data_source_id,),
        ).one()
    elif data_source_name is not None:
        db_data_source = session.execute(
            'select * from "et"."dataSource" where "name"=%s allow filtering;',
            (data_source_name,),
        ).one()

    return db_data_source


def get_all_data_sources() -> List[Any]:
    """
    Get all data sources from the database.

    Returns:
        List of all data source database objects.
    """
    session = get_cassandra_session()
    return session.execute('select * from "et"."dataSource";').all()


def get_campaign_data_sources(db_campaign: Any) -> List[Any]:
    """
    Get all data sources configured for a campaign.

    Args:
        db_campaign: Campaign database object.

    Returns:
        List of data source database objects.
    """
    data_sources = []
    config_jsons = json.loads(s=db_campaign.configJson)

    for config_json in config_jsons:
        db_data_source = get_data_source(
            data_source_id=config_json["data_source_id"]
        )
        if db_data_source is not None:
            data_sources.append(db_data_source)

    return data_sources


# ==============================================================================
# Data Storage
# ==============================================================================


def store_data_record(
    db_user: Any,
    db_campaign: Any,
    db_data_source: Any,
    timestamp: int,
    value: bytes,
) -> None:
    """
    Store a single data record.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
        db_data_source: Data source database object.
        timestamp: Record timestamp in milliseconds.
        value: Record value as bytes.
    """
    session = get_cassandra_session()
    session.execute(
        f'insert into "data"."cmp{db_campaign.id}_usr{db_user.id}"'
        '("dataSourceId", "timestamp", "value") values (%s,%s,%s);',
        (db_data_source.id, timestamp, value),
    )


def store_data_records(
    db_user: Any,
    db_campaign: Any,
    timestamp_list: List[int],
    data_source_id_list: List[int],
    value_list: List[bytes],
) -> None:
    """
    Store multiple data records.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
        timestamp_list: List of timestamps in milliseconds.
        data_source_id_list: List of data source IDs.
        value_list: List of values as bytes.
    """
    data_sources_cache: dict = {}

    for timestamp, data_source_id, value in zip(
        timestamp_list, data_source_id_list, value_list
    ):
        if data_source_id not in data_sources_cache:
            db_data_source = get_data_source(data_source_id=data_source_id)
            if db_data_source is None:
                continue
            data_sources_cache[data_source_id] = db_data_source

        if data_sources_cache[data_source_id] is not None:
            store_data_record(
                db_user=db_user,
                db_campaign=db_campaign,
                db_data_source=data_sources_cache[data_source_id],
                timestamp=timestamp,
                value=value,
            )


def get_file(db_campaign: Any, db_user: Any, db_data_source: Any) -> Any:
    """
    Get or create a file handle for storing data records.

    Args:
        db_campaign: Campaign database object.
        db_user: User database object.
        db_data_source: Data source database object.

    Returns:
        File handle for appending data.
    """
    root_dir = "/home/kobiljon/Desktop/easytrack/services/data"
    file_path = (
        f"{root_dir}/cmp{db_campaign.id}_usr{db_user.id}_ds{db_data_source.id}.csv"
    )

    if exists(file_path):
        return open(file_path, "a")
    else:
        file_handle = open(file_path, "w")
        file_handle.write("timestamp,value\n")
        return file_handle


def store_data_record_to_file(file: Any, timestamp: int, value: Any) -> None:
    """
    Write a data record to a file.

    Args:
        file: File handle to write to.
        timestamp: Record timestamp.
        value: Record value.
    """
    file.write(f"{timestamp},{value}\n")


def store_data_records_to_file(
    db_user: Any,
    db_campaign: Any,
    timestamp_list: List[int],
    data_source_id_list: List[int],
    value_list: List[Any],
) -> None:
    """
    Store multiple data records to files.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
        timestamp_list: List of timestamps.
        data_source_id_list: List of data source IDs.
        value_list: List of values.
    """
    files: dict = {}

    for timestamp, data_source_id, value in zip(
        timestamp_list, data_source_id_list, value_list
    ):
        if data_source_id not in files:
            db_data_source = get_data_source(data_source_id=data_source_id)
            files[data_source_id] = get_file(
                db_campaign=db_campaign,
                db_user=db_user,
                db_data_source=db_data_source,
            )
        store_data_record_to_file(
            file=files[data_source_id], timestamp=timestamp, value=value
        )


def get_next_k_data_records(
    db_user: Any,
    db_campaign: Any,
    from_timestamp: int,
    db_data_source: Any,
    k: int,
) -> List[Any]:
    """
    Get the next K data records after a timestamp.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
        from_timestamp: Starting timestamp in milliseconds.
        db_data_source: Data source database object.
        k: Maximum number of records to return.

    Returns:
        List of data record objects.
    """
    session = get_cassandra_session()
    return session.execute(
        f'select * from "data"."cmp{db_campaign.id}_usr{db_user.id}" '
        f'where "timestamp">=%s and "dataSourceId"=%s '
        f'order by "timestamp" asc limit {k} allow filtering;',
        (from_timestamp, db_data_source.id),
    ).all()


def get_filtered_data_records(
    db_user: Any,
    db_campaign: Any,
    db_data_source: Any,
    from_timestamp: Optional[int] = None,
    till_timestamp: Optional[int] = None,
) -> List[Any]:
    """
    Get data records filtered by timestamp range.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
        db_data_source: Data source database object.
        from_timestamp: Start timestamp (inclusive). Optional.
        till_timestamp: End timestamp (exclusive). Optional.

    Returns:
        List of data record objects.
    """
    session = get_cassandra_session()
    table_name = f'"data"."cmp{db_campaign.id}_usr{db_user.id}"'

    if from_timestamp is not None and till_timestamp is not None:
        data_records = session.execute(
            f'select * from {table_name} where "dataSourceId"=%s '
            f'and "timestamp">=%s and "timestamp"<%s '
            f'order by "timestamp" allow filtering;',
            (db_data_source.id, from_timestamp, till_timestamp),
        ).all()
    elif from_timestamp is not None:
        data_records = session.execute(
            f'select * from {table_name} where "dataSourceId"=%s '
            f'and "timestamp">=%s order by "timestamp" allow filtering;',
            (db_data_source.id, from_timestamp),
        ).all()
    elif till_timestamp is not None:
        data_records = session.execute(
            f'select * from {table_name} where "dataSourceId"=%s '
            f'and "timestamp"<%s order by "timestamp" allow filtering;',
            (db_data_source.id, till_timestamp),
        ).all()
    else:
        data_records = session.execute(
            f'select * from {table_name} where "dataSourceId"=%s '
            f'order by "timestamp" allow filtering;',
            (db_data_source.id,),
        ).all()

    return data_records


def dump_data(db_campaign: Any, db_user: Any) -> str:
    """
    Dump campaign data for a user to a file.

    Args:
        db_campaign: Campaign database object.
        db_user: User database object.

    Returns:
        Path to the dumped file.
    """
    session = get_cassandra_session()

    file_path = utils.get_download_file_path(
        f"cmp{db_campaign.id}_usr{db_user.id}.bin.tmp"
    )
    session.execute(
        f'copy (select "id", "timestamp", "value", "dataSourceId" '
        f'from "data"."cmp{db_campaign.id}_usr{db_user.id}" allow filtering) '
        f"to %s with binary;",
        (file_path,),
    )

    session.close()
    return file_path


# ==============================================================================
# Communication Management
# ==============================================================================


def create_direct_message(
    db_source_user: Any,
    db_target_user: Any,
    subject: str,
    content: str,
) -> Optional[Any]:
    """
    Create a direct message between users.

    Args:
        db_source_user: Sender user database object.
        db_target_user: Recipient user database object.
        subject: Message subject.
        content: Message content.

    Returns:
        The created direct message database object.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."directMessage"')
    session.execute(
        'insert into "et"."directMessage"'
        '("id", "sourceUserId", "targetUserId", "timestamp", "subject", "content") '
        "values (%s,%s,%s,%s,%s,%s);",
        (
            next_id,
            db_source_user.id,
            db_target_user.id,
            utils.get_timestamp_ms(),
            subject,
            content,
        ),
    )
    return session.execute(
        'select * from "et"."directMessage" where "id"=%s;', (next_id,)
    ).one()


def get_unread_direct_messages(db_user: Any) -> List[Any]:
    """
    Get unread direct messages for a user and mark them as read.

    Args:
        db_user: User database object.

    Returns:
        List of unread direct message database objects.
    """
    session = get_cassandra_session()
    db_direct_messages = session.execute(
        'select * from "et"."directMessage" '
        'where "targetUserId"=%s and "read"=FALSE allow filtering;',
        (db_user.id,),
    ).all()

    for db_direct_message in db_direct_messages:
        session.execute(
            'update "et"."directMessage" set "read"=TRUE '
            'where "targetUserId"=%s and "sourceUserId"=%s and "id"=%s;',
            (db_user.id, db_direct_message.sourceUserId, db_direct_message.id),
        )

    return db_direct_messages


def create_notification(
    db_campaign: Any,
    timestamp: int,
    subject: str,
    content: str,
) -> List[Any]:
    """
    Create a notification for all campaign participants.

    Args:
        db_campaign: Campaign database object.
        timestamp: Notification timestamp.
        subject: Notification subject.
        content: Notification content.

    Returns:
        List of created notification database objects.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."notification"')

    for db_participant in get_campaign_participants(db_campaign=db_campaign):
        session.execute(
            'insert into "et"."notification"'
            '("id", "timestamp", "subject", "content", "read", '
            '"campaignId", "targetUserId") values (%s,%s,%s,%s,%s,%s,%s)',
            (
                next_id,
                timestamp,
                subject,
                content,
                False,
                db_campaign.id,
                db_participant.id,
            ),
        )

    return session.execute(
        'select * from "et"."notification" where "id"=%s allow filtering;', (next_id,)
    ).all()


def get_unread_notifications(db_user: Any) -> List[Any]:
    """
    Get unread notifications for a user and mark them as read.

    Args:
        db_user: User database object.

    Returns:
        List of unread notification database objects.
    """
    session = get_cassandra_session()
    db_notifications = session.execute(
        'select * from "et"."notification" '
        'where "targetUserId"=%s and "read"=FALSE allow filtering;',
        (db_user.id,),
    ).all()

    for db_notification in db_notifications:
        session.execute(
            'update "et"."notification" set "read"=TRUE '
            'where "campaignId"=%s and "targetUserId"=%s and "id"=%s;',
            (db_notification.campaignId, db_user.id, db_notification.id),
        )

    return db_notifications


# ==============================================================================
# Statistics
# ==============================================================================


def get_participant_join_timestamp(db_user: Any, db_campaign: Any) -> Optional[int]:
    """
    Get when a participant joined a campaign.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.

    Returns:
        Join timestamp in milliseconds, or None if not found.
    """
    session = get_cassandra_session()
    res = session.execute(
        'select "joinTimestamp" from "stats"."campaignParticipantStats" '
        'where "userId"=%s and "campaignId"=%s allow filtering;',
        (db_user.id, db_campaign.id),
    ).one()
    return None if res is None else res.joinTimestamp


def get_participant_last_sync_timestamp(db_user: Any, db_campaign: Any) -> int:
    """
    Get the last sync timestamp for a participant.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.

    Returns:
        Last sync timestamp in milliseconds, or 0 if never synced.
    """
    session = get_cassandra_session()
    res = session.execute(
        'select max("syncTimestamp") from "stats"."perDataSourceStats" '
        'where "campaignId"=%s and "userId"=%s allow filtering;',
        (db_campaign.id, db_user.id),
    ).one()[0]
    return 0 if res is None else res


def get_participant_heartbeat_timestamp(db_user: Any, db_campaign: Any) -> int:
    """
    Get the last heartbeat timestamp for a participant.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.

    Returns:
        Last heartbeat timestamp in milliseconds, or 0 if never sent.
    """
    session = get_cassandra_session()
    res = session.execute(
        'select "lastHeartbeatTimestamp" from "stats"."campaignParticipantStats" '
        'where "userId" = %s and "campaignId" = %s allow filtering;',
        (db_user.id, db_campaign.id),
    ).one()
    return 0 if res is None else res.lastHeartbeatTimestamp


def get_participants_amount_of_data(db_user: Any, db_campaign: Any) -> int:
    """
    Get the total amount of data submitted by a participant.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.

    Returns:
        Total number of data samples submitted.
    """
    session = get_cassandra_session()
    amount_of_samples = session.execute(
        'select sum("amountOfSamples") from "stats"."perDataSourceStats" '
        'where "campaignId"=%s and "userId"=%s allow filtering;',
        (db_campaign.id, db_user.id),
    ).one()[0]
    return 0 if amount_of_samples is None else amount_of_samples


def get_participants_per_data_source_stats(
    db_user: Any, db_campaign: Any
) -> List[Tuple[Any, int, int]]:
    """
    Get per-data-source statistics for a participant.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.

    Returns:
        List of tuples (data_source, amount_of_samples, sync_timestamp).
    """
    session = get_cassandra_session()
    db_data_sources = get_campaign_data_sources(db_campaign=db_campaign)
    res_stats = []

    for db_data_source in db_data_sources:
        # Get amount of samples
        res = session.execute(
            'select "amountOfSamples" from "stats"."perDataSourceStats" '
            'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s '
            "allow filtering;",
            (db_campaign.id, db_user.id, db_data_source.id),
        ).one()
        amount_of_samples = (
            0 if res is None or res.amountOfSamples is None else res.amountOfSamples
        )

        # Get sync timestamp
        res = session.execute(
            'select "syncTimestamp" from "stats"."perDataSourceStats" '
            'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s '
            "allow filtering;",
            (db_campaign.id, db_user.id, db_data_source.id),
        ).one()
        sync_timestamp = (
            0 if res is None or res.syncTimestamp is None else res.syncTimestamp
        )

        res_stats.append((db_data_source, amount_of_samples, sync_timestamp))

    return res_stats


def update_user_heartbeat_timestamp(db_user: Any, db_campaign: Any) -> None:
    """
    Update the heartbeat timestamp for a participant.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
    """
    session = get_cassandra_session()
    session.execute(
        'update "stats"."campaignParticipantStats" '
        'set "lastHeartbeatTimestamp" = %s '
        'where "userId" = %s and "campaignId" = %s;',
        (utils.get_timestamp_ms(), db_user.id, db_campaign.id),
    )


def remove_participant_from_campaign(db_user: Any, db_campaign: Any) -> None:
    """
    Remove a participant from a campaign.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
    """
    session = get_cassandra_session()
    session.execute(
        'delete from "stats"."campaignParticipantStats" '
        'where "userId" = %s and "campaignId" = %s;',
        (db_user.id, db_campaign.id),
    )


def get_participants_data_source_sync_timestamps(
    db_user: Any, db_campaign: Any, db_data_source: Any
) -> int:
    """
    Get the sync timestamp for a specific data source.

    Args:
        db_user: User database object.
        db_campaign: Campaign database object.
        db_data_source: Data source database object.

    Returns:
        Sync timestamp in milliseconds, or 0 if never synced.
    """
    session = get_cassandra_session()
    res = session.execute(
        'select "syncTimestamp" from "stats"."perDataSourceStats" '
        'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s allow filtering;',
        (db_campaign.id, db_user.id, db_data_source.id),
    )
    return 0 if res is None else res.syncTimestamp


def get_filtered_amount_of_data(
    db_campaign: Any,
    from_timestamp: int = 0,
    till_timestamp: int = 9999999999999,
    db_user: Optional[Any] = None,
    db_data_source: Optional[Any] = None,
) -> int:
    """
    Get the amount of data matching filter criteria.

    Args:
        db_campaign: Campaign database object.
        from_timestamp: Start timestamp (inclusive). Defaults to 0.
        till_timestamp: End timestamp (exclusive). Defaults to far future.
        db_user: If provided, only count data from this user.
        db_data_source: If provided, only count data from this source.

    Returns:
        Number of matching data records.
    """
    session = get_cassandra_session()
    amount = 0

    if db_user is None:
        # All users
        participants = get_campaign_participants(db_campaign=db_campaign)

        if db_data_source is None:
            # All data sources
            for db_participant_user in participants:
                amount += session.execute(
                    f'select count(*) from "data"."{db_campaign.id}-{db_participant_user.id}" '
                    f'where "timestamp">=%s and "timestamp"<%s allow filtering;',
                    (from_timestamp, till_timestamp),
                    timeout=60000,
                ).one()[0]
        else:
            # Single data source
            for db_participant_user in participants:
                amount += session.execute(
                    f'select count(*) from "data"."{db_campaign.id}-{db_participant_user.id}" '
                    f'where "dataSourceId"=%s and "timestamp">=%s and "timestamp"<%s '
                    f"allow filtering;",
                    (db_data_source.id, from_timestamp, till_timestamp),
                    timeout=60000,
                ).one()[0]
    else:
        # Single user
        table_name = f'"data"."cmp{db_campaign.id}_usr{db_user.id}"'

        if db_data_source is None:
            # All data sources
            amount += session.execute(
                f"select count(*) from {table_name} "
                f'where "timestamp">=%s and "timestamp"<%s;',
                (from_timestamp, till_timestamp),
                timeout=60000,
            ).one()[0]
        else:
            # Single data source
            amount += session.execute(
                f"select count(*) from {table_name} "
                f'where "dataSourceId"=%s and "timestamp">=%s and "timestamp"<%s '
                f"allow filtering;",
                (db_data_source.id, from_timestamp, till_timestamp),
                timeout=60000,
            ).one()[0]

    return amount
