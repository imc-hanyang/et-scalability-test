"""
Data Quality Statistics Module.

This module provides background jobs for monitoring and updating
data quality statistics for campaigns.
"""
import json
import logging
import os
import subprocess
import threading
import time
from typing import Any, Dict, List

from tools import db_mgr as db
from tools import utils

# Configure module logger
logger = logging.getLogger(__name__)

# Thread tracking dictionaries
campaign_stats_threads: Dict[int, threading.Thread] = {}
campaign_backup_threads: Dict[int, threading.Thread] = {}

# Constants
STATS_CHECK_INTERVAL_SECONDS: int = 10
BACKUP_INTERVAL_SECONDS: int = 2 * 60
QUERY_TIMEOUT_SECONDS: float = 300.0


def start_background_jobs() -> None:
    """
    Start all background data quality monitoring jobs.

    Starts threads for:
    - Amount of data routine (checks data submission stats)
    """
    threading.Thread(target=amounts_of_data_routine, daemon=True).start()
    logger.info("Background data quality jobs started")


def amounts_of_data_routine() -> None:
    """
    Background routine to update data amount statistics.

    Continuously checks active campaigns and updates per-data-source
    statistics for all participants. Runs every STATS_CHECK_INTERVAL_SECONDS.
    """
    logger.info("Starting amounts_of_data_routine...")

    def _update_campaign_stats(
        db_campaign: Any,
        db_participants: List[Any],
        db_data_sources: List[Any],
    ) -> None:
        """Update statistics for a single campaign."""
        session = db.get_cassandra_session(maintenance=True)

        for db_user in db_participants:
            if db_user is None:
                continue

            for db_data_source in db_data_sources:
                if db_data_source is None:
                    continue

                # Check if stats record exists
                exists = (
                    session.execute(
                        'select count(*) from "stats"."perDataSourceStats" '
                        'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s '
                        "allow filtering;",
                        (db_campaign.id, db_user.id, db_data_source.id),
                        timeout=QUERY_TIMEOUT_SECONDS,
                    ).one()[0]
                    > 0
                )

                # Create stats record if needed
                if not exists:
                    session.execute(
                        'insert into "stats"."perDataSourceStats"'
                        '("campaignId", "userId", "dataSourceId") '
                        "values (%s,%s,%s);",
                        (db_campaign.id, db_user.id, db_data_source.id),
                    )

                # Get all timestamps for this data source
                timestamps = session.execute(
                    f'select "timestamp" from "data"."cmp{db_campaign.id}_usr{db_user.id}" '
                    'where "dataSourceId"=%s allow filtering;',
                    (db_data_source.id,),
                    timeout=QUERY_TIMEOUT_SECONDS,
                ).all()

                total_amount_of_samples = len(timestamps)
                last_sync_timestamp = (
                    0
                    if total_amount_of_samples == 0
                    else max(timestamps, key=lambda x: x.timestamp).timestamp
                )

                # Update stats
                session.execute(
                    'update "stats"."perDataSourceStats" '
                    'set "syncTimestamp" = %s, "amountOfSamples" = %s '
                    'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s;',
                    (
                        last_sync_timestamp,
                        total_amount_of_samples,
                        db_campaign.id,
                        db_user.id,
                        db_data_source.id,
                    ),
                )

        logger.info(
            "%s: DQ - Campaign %s amount of data check completed successfully",
            utils.get_timestamp_ms(),
            db_campaign.id,
        )

    while True:
        db_campaigns = db.get_campaigns(active_only=True)

        # Pre-fetch participants and data sources for all campaigns
        all_db_participants = {
            db_campaign.id: db.get_campaign_participants(db_campaign=db_campaign)
            for db_campaign in db_campaigns
        }
        all_db_data_sources = {
            db_campaign.id: [
                db.get_data_source(data_source_id=config_json["data_source_id"])
                for config_json in json.loads(s=db_campaign.configJson)
            ]
            for db_campaign in db_campaigns
        }

        # Process each campaign
        for db_campaign in db_campaigns:
            _update_campaign_stats(
                db_campaign=db_campaign,
                db_participants=all_db_participants[db_campaign.id],
                db_data_sources=all_db_data_sources[db_campaign.id],
            )

        campaign_ids = ", ".join(str(c.id) for c in db_campaigns)
        logger.info(
            "%s: DQ - Checking amounts of data for campaigns: %s",
            utils.get_timestamp_ms(),
            campaign_ids,
        )
        time.sleep(STATS_CHECK_INTERVAL_SECONDS)


def backup_routine() -> None:
    """
    Background routine to backup campaign data.

    Continuously backs up data for active campaigns by exporting
    tables to CSV files. Runs every BACKUP_INTERVAL_SECONDS.
    """
    logger.info("Starting backup_routine...")

    def _backup_campaign(db_campaign: Any, db_participants: List[Any]) -> None:
        """Backup data for a single campaign."""
        for db_user in db_participants:
            if db_user is None:
                continue

            logger.info(
                "DQ: Backing up campaign %s participant %s...",
                db_campaign.id,
                db_user.id,
            )

            table = f"cmp{db_campaign.id}_usr{db_user.id}"
            filepath = f"/root/EasyTrack_Platform/data/{table}.csv"
            query = f'copy "data"."{table}" to \'{filepath}\';'
            query_path = "/root/EasyTrack_Platform/data/query.cqlsh"

            with open(query_path, "w") as query_file:
                query_file.write(query)

            with open(os.devnull, "wb") as devnull:
                subprocess.check_call(
                    ["cqlsh", "-f", query_path],
                    stdout=devnull,
                    stderr=subprocess.STDOUT,
                )

            logger.info(
                "DQ: Campaign %s participant %s backup completed successfully",
                db_campaign.id,
                db_user.id,
            )

        logger.info(
            "%s: DQ - Campaign %s backup completed successfully",
            utils.get_timestamp_ms(),
            db_campaign.id,
        )

    while True:
        db_campaigns = db.get_campaigns(active_only=True)

        all_db_participants = {
            db_campaign.id: db.get_campaign_participants(db_campaign=db_campaign)
            for db_campaign in db_campaigns
        }

        for db_campaign in db_campaigns:
            db_participants = all_db_participants[db_campaign.id]

            # Skip if backup thread is already running
            if (
                db_campaign.id in campaign_backup_threads
                and campaign_backup_threads[db_campaign.id].is_alive()
            ):
                continue

            # Start new backup thread
            campaign_backup_threads[db_campaign.id] = threading.Thread(
                target=_backup_campaign, args=(db_campaign, db_participants)
            )
            campaign_backup_threads[db_campaign.id].start()

        campaign_ids = ", ".join(str(c.id) for c in db_campaigns)
        logger.info(
            "%s: DQ - Backing up campaigns: %s",
            utils.get_timestamp_ms(),
            campaign_ids,
        )
        time.sleep(BACKUP_INTERVAL_SECONDS)
