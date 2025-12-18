"""
Cassandra Database Manager for the EasyTrack Dashboard.

This module provides functions for interacting with Apache Cassandra database,
including user management, campaign management, data source management,
data storage and retrieval, and statistics tracking.
"""

import json
import logging
import subprocess
from subprocess import PIPE
from typing import Any, Dict, List, Optional, Tuple

from cassandra.cluster import Cluster

from tools import settings, utils

# Configure logging
logger = logging.getLogger(__name__)

# Constants
DEFAULT_CONTACT_POINTS = ["localhost"]
EXECUTOR_THREADS = 2048
CONNECT_TIMEOUT = 1200


# =============================================================================
# Common Database Operations
# =============================================================================


def get_cassandra_session():
    """
    Get or create a Cassandra database session.

    Initializes the Cassandra cluster connection if not already established.

    Returns:
        The active Cassandra session object.
    """
    if settings.cassandra_session is None:
        settings.cassandra_cluster = Cluster(
            contact_points=DEFAULT_CONTACT_POINTS,
            executor_threads=EXECUTOR_THREADS,
            connect_timeout=CONNECT_TIMEOUT,
        )
        settings.cassandra_session = settings.cassandra_cluster.connect()
        logger.info("Cassandra session initialized: %s", settings.cassandra_session)
    return settings.cassandra_session


def end() -> None:
    """
    Clean up and close the Cassandra database connections.

    Shuts down both the session and cluster connections.
    """
    settings.cassandra_session.shutdown()
    settings.cassandra_cluster.shutdown()


def get_next_id(session, table_name: str) -> int:
    """
    Get the next available ID for a table.

    Args:
        session: The Cassandra session.
        table_name: The fully qualified table name.

    Returns:
        The next available ID (max ID + 1, or 0 if table is empty).
    """
    res = session.execute(f'select max("id") from {table_name};')
    last_id = res.one()[0]
    return 0 if last_id is None else last_id + 1


# =============================================================================
# 1. User Management
# =============================================================================


def create_user(name: str, email: str, session_key: str) -> Optional[Any]:
    """
    Create a new user in the database.

    Args:
        name: User's display name.
        email: User's email address.
        session_key: Authentication session key.

    Returns:
        The created user record, or None if creation failed.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."user"')
    session.execute(
        'insert into "et"."user"("id", "email", "sessionKey", "name") values (%s,%s,%s,%s);',
        (next_id, email, session_key, name),
    )
    return session.execute('select * from "et"."user" where "id"=%s;', (next_id,)).one()


def get_user(user_id: Optional[int] = None, email: Optional[str] = None) -> Optional[Any]:
    """
    Retrieve a user from the database.

    Args:
        user_id: The user's ID (optional).
        email: The user's email address (optional).

    Returns:
        The user record if found, None otherwise.

    Note:
        At least one of user_id or email must be provided.
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


def update_session_key(db_user, session_key: str) -> None:
    """
    Update a user's session key.

    Args:
        db_user: The user record to update.
        session_key: The new session key.
    """
    session = get_cassandra_session()
    session.execute(
        'update "et"."user" set "sessionKey" = %s where "id" = %s and "email" = %s;',
        (session_key, db_user.id, db_user.email),
    )


def user_is_bound_to_campaign(db_user, db_campaign) -> bool:
    """
    Check if a user is bound to a campaign as a participant.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.

    Returns:
        True if the user is a participant in the campaign, False otherwise.
    """
    session = get_cassandra_session()
    count = session.execute(
        'select count(*) from "stats"."campaignParticipantStats" '
        'where "campaignId"=%s and "userId"=%s allow filtering;',
        (db_campaign.id, db_user.id),
    ).one()[0]
    return count > 0


def bind_participant_to_campaign(db_user, db_campaign) -> bool:
    """
    Bind a user to a campaign as a participant.

    Creates the participant stats record and data table if this is a new binding.

    Args:
        db_user: The user record.
        db_campaign: The campaign record.

    Returns:
        True if this was a new binding, False if already bound.
    """
    session = get_cassandra_session()

    if user_is_bound_to_campaign(db_user=db_user, db_campaign=db_campaign):
        return False  # Already bound

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


def get_campaign_participants(db_campaign) -> List[Any]:
    """
    Get all participants in a campaign.

    Args:
        db_campaign: The campaign record.

    Returns:
        List of user records for all participants.
    """
    session = get_cassandra_session()
    db_participants = []

    rows = session.execute(
        'select "userId" from "stats"."campaignParticipantStats" '
        'where "campaignId"=%s allow filtering;',
        (db_campaign.id,),
    ).all()

    for row in rows:
        user = get_user(user_id=row.userId)
        if user:
            db_participants.append(user)
        else:
            # Clean up orphaned participant stats
            session.execute(
                'delete from "stats"."campaignParticipantStats" '
                'where "userId" = %s and "campaignId" = %s;',
                (row.userId, db_campaign.id),
            )

    return db_participants


def get_campaign_researchers(db_campaign) -> List[Any]:
    """
    Get all researchers associated with a campaign.

    Args:
        db_campaign: The campaign record.

    Returns:
        List of user records for all researchers.
    """
    session = get_cassandra_session()
    db_researchers = []

    rows = session.execute(
        'select "researcherId" from "et"."campaignResearchers" '
        'where "campaignId"=%s allow filtering;',
        (db_campaign.id,),
    ).all()

    for row in rows:
        db_researchers.append(get_user(user_id=row.researcherId))

    return db_researchers


def get_campaign_participants_count(db_campaign) -> int:
    """
    Get the count of participants in a campaign.

    Args:
        db_campaign: The campaign record.

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


def add_researcher_to_campaign(db_campaign, db_researcher_user) -> None:
    """
    Add a researcher to a campaign.

    Args:
        db_campaign: The campaign record.
        db_researcher_user: The researcher's user record.
    """
    session = get_cassandra_session()
    session.execute(
        'insert into "et"."campaignResearchers"("campaignId", "researcherId") '
        'values(%s,%s);',
        (db_campaign.id, db_researcher_user.id),
    )


def remove_researcher_from_campaign(db_campaign, db_researcher_user) -> None:
    """
    Remove a researcher from a campaign.

    Args:
        db_campaign: The campaign record.
        db_researcher_user: The researcher's user record.
    """
    session = get_cassandra_session()
    session.execute(
        'delete from "et"."campaignResearchers" '
        'where "campaignId"=%s and "researcherId"=%s;',
        (db_campaign.id, db_researcher_user.id),
    )


# =============================================================================
# 2. Campaign Management
# =============================================================================


def create_or_update_campaign(
    db_creator_user,
    name: str,
    notes: str,
    configurations: str,
    start_timestamp: int,
    end_timestamp: int,
    db_campaign=None,
) -> Optional[Any]:
    """
    Create a new campaign or update an existing one.

    Args:
        db_creator_user: The user creating/updating the campaign.
        name: Campaign name.
        notes: Campaign notes/description.
        configurations: JSON string of campaign configurations.
        start_timestamp: Campaign start time in milliseconds.
        end_timestamp: Campaign end time in milliseconds.
        db_campaign: Existing campaign to update (None to create new).

    Returns:
        The created/updated campaign record.
    """
    session = get_cassandra_session()

    if db_campaign is None:
        # Create new campaign
        next_id = get_next_id(session=session, table_name='"et"."campaign"')
        session.execute(
            'insert into "et"."campaign"'
            '("id", "creatorId", "name", "notes", "configJson", "startTimestamp", "endTimestamp") '
            'values (%s,%s,%s,%s,%s,%s,%s);',
            (next_id, db_creator_user.id, name, notes, configurations, start_timestamp, end_timestamp),
        )
        return get_campaign(campaign_id=next_id, db_researcher_user=db_creator_user)

    elif db_campaign.creatorId == db_creator_user.id:
        # Update existing campaign
        session.execute(
            'update "et"."campaign" set "name" = %s, "notes" = %s, "configJson" = %s, '
            '"startTimestamp" = %s, "endTimestamp" = %s where "creatorId"=%s and "id"=%s;',
            (name, notes, configurations, start_timestamp, end_timestamp,
             db_creator_user.id, db_campaign.id),
        )
        return db_campaign

    return None


def get_campaign(campaign_id: int, db_researcher_user=None) -> Optional[Any]:
    """
    Retrieve a campaign by ID.

    Args:
        campaign_id: The campaign ID.
        db_researcher_user: Optional user to filter by (must be creator or researcher).

    Returns:
        The campaign record if found and accessible, None otherwise.
    """
    session = get_cassandra_session()

    if db_researcher_user is None:
        db_campaign = session.execute(
            'select * from "et"."campaign" where "id"=%s allow filtering;',
            (campaign_id,),
        ).one()
    else:
        db_campaign = session.execute(
            'select * from "et"."campaign" where "id"=%s and "creatorId"=%s allow filtering;',
            (campaign_id, db_researcher_user.id),
        ).one()

        if db_campaign is None:
            # Check if user is a researcher for this campaign
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


def delete_campaign(db_campaign) -> None:
    """
    Delete a campaign.

    Args:
        db_campaign: The campaign record to delete.
    """
    session = get_cassandra_session()
    session.execute(
        'delete from "et"."campaign" where "creatorId"=%s and "id"=%s;',
        (db_campaign.creatorId, db_campaign.id),
    )


def get_campaigns(db_creator_user=None) -> List[Any]:
    """
    Get all campaigns, optionally filtered by creator.

    Args:
        db_creator_user: Optional user to filter campaigns by creator.

    Returns:
        List of campaign records.
    """
    session = get_cassandra_session()

    if db_creator_user is None:
        db_campaigns = session.execute('select * from "et"."campaign";').all()
    else:
        db_campaigns = session.execute(
            'select * from "et"."campaign" where "creatorId"=%s allow filtering;',
            (db_creator_user.id,),
        ).all()

    return db_campaigns


def get_researcher_campaigns(db_researcher_user) -> List[Any]:
    """
    Get all campaigns where the user is a researcher.

    Args:
        db_researcher_user: The researcher's user record.

    Returns:
        List of campaign records.
    """
    session = get_cassandra_session()
    db_campaigns = []

    rows = session.execute(
        'select "campaignId" from "et"."campaignResearchers" '
        'where "researcherId"=%s allow filtering;',
        (db_researcher_user.id,),
    ).all()

    for row in rows:
        db_campaigns.append(get_campaign(campaign_id=row.campaignId))

    return db_campaigns


# =============================================================================
# 3. Data Source Management
# =============================================================================


def create_data_source(db_creator_user, name: str, icon_name: str) -> Any:
    """
    Create a new data source.

    Args:
        db_creator_user: The user creating the data source.
        name: Data source name.
        icon_name: Icon identifier for the data source.

    Returns:
        The created data source record.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."dataSource"')
    session.execute(
        'insert into "et"."dataSource"("id", "creatorId", "name", "iconName") '
        'values (%s,%s,%s,%s);',
        (next_id, db_creator_user.id, name, icon_name),
    )
    return get_data_source(data_source_id=next_id)


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
    session = get_cassandra_session()
    db_data_source = None

    if data_source_id is not None and data_source_name is not None:
        db_data_source = session.execute(
            'select * from "et"."dataSource" where "id"=%s and "name"=%s allow filtering;',
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
    Get all data sources.

    Returns:
        List of all data source records.
    """
    session = get_cassandra_session()
    return session.execute('select * from "et"."dataSource";').all()


def get_campaign_data_sources(db_campaign) -> List[Any]:
    """
    Get all data sources configured for a campaign.

    Args:
        db_campaign: The campaign record.

    Returns:
        List of data source records.
    """
    db_data_sources = []
    config_jsons = json.loads(s=db_campaign.configJson)

    for config_json in config_jsons:
        db_data_source = get_data_source(data_source_id=config_json["data_source_id"])
        if db_data_source is not None:
            db_data_sources.append(db_data_source)

    return db_data_sources


# =============================================================================
# 4. Data Management
# =============================================================================


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
        db_user: The user the data belongs to.
        db_campaign: The campaign the data belongs to.
        db_data_source: The data source type.
        timestamp: Record timestamp in milliseconds.
        value: Binary data value.
    """
    session = get_cassandra_session()
    session.execute(
        f'insert into "data"."cmp{db_campaign.id}_usr{db_user.id}"'
        '("dataSourceId", "timestamp", "value") values (%s,%s,%s);',
        (db_data_source.id, timestamp, value),
    )


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
        db_user: The user the data belongs to.
        db_campaign: The campaign the data belongs to.
        timestamp_list: List of record timestamps.
        data_source_id_list: List of data source IDs.
        value_list: List of binary data values.
    """
    data_sources: Dict[int, Any] = {}

    for timestamp, data_source_id, value in zip(timestamp_list, data_source_id_list, value_list):
        if data_source_id not in data_sources:
            db_data_source = get_data_source(data_source_id=data_source_id)
            if db_data_source is None:
                continue
            data_sources[data_source_id] = db_data_source

        if data_sources[data_source_id] is not None:
            store_data_record(
                db_user=db_user,
                db_campaign=db_campaign,
                db_data_source=data_sources[data_source_id],
                timestamp=timestamp,
                value=value,
            )


def get_next_k_data_records(
    db_user,
    db_campaign,
    from_timestamp: int,
    db_data_source,
    k: int,
) -> List[Any]:
    """
    Get the next k data records starting from a timestamp.

    Args:
        db_user: The user to get data for.
        db_campaign: The campaign to get data from.
        from_timestamp: Starting timestamp (inclusive).
        db_data_source: The data source to filter by.
        k: Maximum number of records to return.

    Returns:
        List of data records.
    """
    session = get_cassandra_session()
    k_records = session.execute(
        f'select * from "data"."cmp{db_campaign.id}_usr{db_user.id}" '
        f'where "timestamp">=%s and "dataSourceId"=%s '
        f'order by "timestamp" asc limit {k} allow filtering;',
        (from_timestamp, db_data_source.id),
    ).all()
    return k_records


def get_filtered_data_records(
    db_user,
    db_campaign,
    db_data_source,
    from_timestamp: Optional[int] = None,
    till_timestamp: Optional[int] = None,
) -> List[Any]:
    """
    Get data records with optional timestamp filtering.

    Args:
        db_user: The user to get data for.
        db_campaign: The campaign to get data from.
        db_data_source: The data source to filter by.
        from_timestamp: Minimum timestamp (inclusive, optional).
        till_timestamp: Maximum timestamp (exclusive, optional).

    Returns:
        List of data records ordered by timestamp.
    """
    session = get_cassandra_session()
    table_name = f'"data"."cmp{db_campaign.id}_usr{db_user.id}"'

    if from_timestamp is not None and till_timestamp is not None:
        data_records = session.execute(
            f'select * from {table_name} where "dataSourceId"=%s '
            f'and "timestamp">=%s and "timestamp"<%s order by "timestamp" allow filtering;',
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


def dump_data(db_campaign, db_user, db_data_source=None) -> str:
    """
    Export data to a CSV file.

    Args:
        db_campaign: The campaign to export data from.
        db_user: The user to export data for.
        db_data_source: Optional data source filter (currently unused).

    Returns:
        Path to the created CSV file.
    """
    file_path = utils.get_download_file_path(f"cmp{db_campaign.id}_usr{db_user.id}.bin.csv")

    # Run cqlsh COPY command
    command = [
        settings.cqlsh_path,
        "-e",
        f"copy data.cmp{db_campaign.id}_usr{db_user.id} to '{file_path}' with header = true;",
    ]
    subprocess.run(command, stdout=PIPE, stderr=PIPE, shell=True)

    return file_path


# =============================================================================
# 5. Communication Management
# =============================================================================


def create_direct_message(
    db_source_user,
    db_target_user,
    subject: str,
    content: str,
) -> Any:
    """
    Create a direct message between users.

    Args:
        db_source_user: The sender's user record.
        db_target_user: The recipient's user record.
        subject: Message subject.
        content: Message content.

    Returns:
        The created message record.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."directMessage"')
    session.execute(
        'insert into "et"."directMessage"'
        '("id", "sourceUserId", "targetUserId", "timestamp", "subject", "content") '
        'values (%s,%s,%s,%s,%s);',
        (next_id, db_source_user.id, db_target_user.id, utils.get_timestamp_ms(), subject, content),
    )
    return session.execute(
        'select * from "et"."directMessage" where "id"=%s;', (next_id,)
    ).one()


def get_unread_direct_messages(db_user) -> List[Any]:
    """
    Get and mark as read all unread direct messages for a user.

    Args:
        db_user: The user to get messages for.

    Returns:
        List of unread message records.
    """
    session = get_cassandra_session()
    db_direct_messages = session.execute(
        'select * from "et"."directMessage" '
        'where "targetUserId"=%s and "read"=FALSE allow filtering;',
        (db_user.id,),
    ).all()
    session.execute(
        'update "et"."directMessage" set "read"=TRUE where targetUserId=%s;',
        (db_user.id,),
    )
    return db_direct_messages


def create_notification(
    db_campaign,
    timestamp: int,
    subject: str,
    content: str,
) -> List[Any]:
    """
    Create a notification for all campaign participants.

    Args:
        db_campaign: The campaign to notify.
        timestamp: Notification timestamp.
        subject: Notification subject.
        content: Notification content.

    Returns:
        List of created notification records.
    """
    session = get_cassandra_session()
    next_id = get_next_id(session=session, table_name='"et"."notification"')

    for db_participant in get_campaign_participants(db_campaign=db_campaign):
        session.execute(
            'insert into "et"."notification"'
            '("id", "timestamp", "subject", "content", "read", "campaignId", "targetUserId") '
            'values (%s,%s,%s,%s,%s,%s,%s)',
            (next_id, timestamp, subject, content, False, db_campaign.id, db_participant.id),
        )

    return session.execute(
        'select * from "et"."notification" where "id"=%s allow filtering;', (next_id,)
    ).all()


def get_unread_notifications(db_user) -> List[Any]:
    """
    Get and mark as read all unread notifications for a user.

    Args:
        db_user: The user to get notifications for.

    Returns:
        List of unread notification records.
    """
    session = get_cassandra_session()
    db_notifications = session.execute(
        'select * from "et"."notification" '
        'where "targetUserId"=%s and "read"=FALSE allow filtering;',
        (db_user.id,),
    ).all()
    session.execute(
        'update "et"."notification" set "read"=TRUE where "targetUserId"=%s;',
        (db_user.id,),
    )
    return db_notifications


# =============================================================================
# 6. Statistics
# =============================================================================


def get_participant_join_timestamp(db_user, db_campaign) -> Optional[int]:
    """
    Get the timestamp when a participant joined a campaign.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

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


def get_participant_last_sync_timestamp(db_user, db_campaign) -> int:
    """
    Get the most recent sync timestamp for a participant.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        Last sync timestamp in milliseconds, or 0 if none.
    """
    session = get_cassandra_session()
    res = session.execute(
        'select max("syncTimestamp") from "stats"."perDataSourceStats" '
        'where "campaignId"=%s and "userId"=%s allow filtering;',
        (db_campaign.id, db_user.id),
    ).one()[0]
    return 0 if res is None else res


def get_participant_heartbeat_timestamp(db_user, db_campaign) -> int:
    """
    Get the last heartbeat timestamp for a participant.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        Last heartbeat timestamp in milliseconds, or 0 if none.
    """
    session = get_cassandra_session()
    res = session.execute(
        'select "lastHeartbeatTimestamp" from "stats"."campaignParticipantStats" '
        'where "userId" = %s and "campaignId" = %s allow filtering;',
        (db_user.id, db_campaign.id),
    ).one()
    return 0 if res is None else res.lastHeartbeatTimestamp


def get_participants_amount_of_data(db_user, db_campaign) -> int:
    """
    Get the total amount of data samples for a participant.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.

    Returns:
        Total number of data samples.
    """
    session = get_cassandra_session()
    amount_of_samples = session.execute(
        'select sum("amountOfSamples") from "stats"."perDataSourceStats" '
        'where "campaignId"=%s and "userId"=%s allow filtering;',
        (db_campaign.id, db_user.id),
    ).one()[0]
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
    session = get_cassandra_session()
    db_data_sources = get_campaign_data_sources(db_campaign=db_campaign)
    res_stats = []

    for db_data_source in db_data_sources:
        res = session.execute(
            'select "amountOfSamples" from "stats"."perDataSourceStats" '
            'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s allow filtering;',
            (db_campaign.id, db_user.id, db_data_source.id),
        ).one()
        amount_of_samples = 0 if res is None or res.amountOfSamples is None else res.amountOfSamples

        res = session.execute(
            'select "syncTimestamp" from "stats"."perDataSourceStats" '
            'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s allow filtering;',
            (db_campaign.id, db_user.id, db_data_source.id),
        ).one()
        sync_timestamp = 0 if res is None or res.syncTimestamp is None else res.syncTimestamp

        res_stats.append((db_data_source, amount_of_samples, sync_timestamp))

    return res_stats


def update_user_heartbeat_timestamp(db_user, db_campaign) -> None:
    """
    Update a participant's heartbeat timestamp to the current time.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.
    """
    session = get_cassandra_session()
    session.execute(
        'update "stats"."campaignParticipantStats" '
        'set "lastHeartbeatTimestamp" = %s where "userId" = %s and "campaignId" = %s;',
        (utils.get_timestamp_ms(), db_user.id, db_campaign.id),
    )


def remove_participant_from_campaign(db_user, db_campaign) -> None:
    """
    Remove a participant from a campaign.

    Args:
        db_user: The participant's user record.
        db_campaign: The campaign record.
    """
    session = get_cassandra_session()
    session.execute(
        'delete from "stats"."campaignParticipantStats" '
        'where "userId" = %s and "campaignId" = %s;',
        (db_user.id, db_campaign.id),
    )


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
    session = get_cassandra_session()
    res = session.execute(
        'select "syncTimestamp" from "stats"."perDataSourceStats" '
        'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s allow filtering;',
        (db_campaign.id, db_user.id, db_data_source.id),
    )
    return 0 if res is None else res.syncTimestamp


def get_filtered_amount_of_data(
    db_campaign,
    from_timestamp: int = 0,
    till_timestamp: int = 9999999999999,
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
                ).one()[0]
        else:
            # Single data source
            for db_participant_user in participants:
                amount += session.execute(
                    f'select count(*) from "data"."{db_campaign.id}-{db_participant_user.id}" '
                    f'where "dataSourceId"=%s and "timestamp">=%s and "timestamp"<%s allow filtering;',
                    (db_data_source.id, from_timestamp, till_timestamp),
                ).one()[0]
    else:
        # Single user
        table_name = f'"data"."cmp{db_campaign.id}_usr{db_user.id}"'

        if db_data_source is None:
            # All data sources
            amount += session.execute(
                f'select count(*) from {table_name} '
                f'where "timestamp">=%s and "timestamp"<%s;',
                (from_timestamp, till_timestamp),
            ).one()[0]
        else:
            # Single data source
            amount += session.execute(
                f'select count(*) from {table_name} '
                f'where "dataSourceId"=%s and "timestamp">=%s and "timestamp"<%s allow filtering;',
                (db_data_source.id, from_timestamp, till_timestamp),
            ).one()[0]

    return amount
