import json
import os
import subprocess
import threading
import time

from tools import db_mgr as db
from tools import utils

campaign_stats_threads = {}
campaign_backup_threads = {}


def start_background_jobs():
    threading.Thread(target=amounts_of_data_routine).start()
    # threading.Thread(target=track_missing_ema_routine, args=([
    #     db.get_data_source(data_source_name='SURVEY_EMA'),
    #     db.get_data_source(data_source_name='SELF_STRESS_REPORT'),
    # ],)).start()


def amounts_of_data_routine():
    print("starting amounts_of_data_routine() ...")

    def campaign_stats(db_campaign, db_participants, db_data_sources):
        session = db.get_cassandra_session(maintenance=True)
        for db_user in db_participants:
            if db_user is not None:
                for db_data_source in db_data_sources:
                    if db_data_source is not None:
                        exists = (
                            session.execute(
                                'select count(*) from "stats"."perDataSourceStats" where "campaignId"=%s and '
                                '"userId"=%s and "dataSourceId"=%s allow filtering;',
                                (db_campaign.id, db_user.id, db_data_source.id),
                                timeout=300.0,
                            ).one()[0]
                            > 0
                        )
                        if not exists:
                            session.execute(
                                'insert into "stats"."perDataSourceStats"("campaignId", "userId", "dataSourceId") '
                                "values (%s,%s,%s);",
                                (db_campaign.id, db_user.id, db_data_source.id),
                            )
                        timestamps = session.execute(
                            f'select "timestamp" from "data"."cmp{db_campaign.id}_usr{db_user.id}"'
                            'where "dataSourceId"=%s allow filtering;',
                            (db_data_source.id,),
                            timeout=300.0,
                        ).all()
                        total_amount_of_samples = len(timestamps)
                        last_sync_timestamp = (
                            0
                            if total_amount_of_samples == 0
                            else max(timestamps, key=lambda x: x.timestamp).timestamp
                        )
                        session.execute(
                            'update "stats"."perDataSourceStats" set "syncTimestamp" = %s, "amountOfSamples" = %s '
                            'where "campaignId"=%s and "userId"=%s and "dataSourceId"=%s;',
                            (
                                last_sync_timestamp,
                                total_amount_of_samples,
                                db_campaign.id,
                                db_user.id,
                                db_data_source.id,
                            ),
                        )
        print(
            utils.get_timestamp_ms(),
            f"DQ : campaign {db_campaign.id} amount of data check --> success",
        )

    while True:
        db_campaigns = db.get_campaigns(active_only=True)
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

        for db_campaign in db_campaigns:
            db_participants = all_db_participants[db_campaign.id]
            db_data_sources = all_db_data_sources[db_campaign.id]

            campaign_stats(
                db_campaign=db_campaign,
                db_participants=db_participants,
                db_data_sources=db_data_sources,
            )

        print(
            utils.get_timestamp_ms(),
            f'DQ : checking amounts of data for campaigns {", ".join([str(x.id) for x in db_campaigns])}',
        )
        time.sleep(10)


def backup_routine():
    print("starting backup_routine() ...")

    def campaign_backup(db_campaign, db_participants):
        for db_user in db_participants:
            if db_user is not None:
                print(
                    f"DQ : backing up campaign {db_campaign.id} participant {db_user.id}..."
                )
                table = f"cmp{db_campaign.id}_usr{db_user.id}"
                filepath = f"/root/EasyTrack_Platform/data/{table}.csv"
                query = f'copy "data"."{table}" to \'{filepath}\';'
                query_path = f"/root/EasyTrack_Platform/data/query.cqlsh"
                with open(query_path, "w") as w:
                    w.write(query)
                with open(os.devnull, "wb") as devnull:
                    subprocess.check_call(
                        ["cqlsh", "-f", query_path],
                        stdout=devnull,
                        stderr=subprocess.STDOUT,
                    )
                print(
                    f"DQ : campaign {db_campaign.id} participant {db_user.id} backup done --> success"
                )

        print(
            utils.get_timestamp_ms(),
            f"DQ : campaign {db_campaign.id} backup --> success",
        )

    while True:
        db_campaigns = db.get_campaigns(active_only=True)
        all_db_participants = {
            db_campaign.id: db.get_campaign_participants(db_campaign=db_campaign)
            for db_campaign in db_campaigns
        }

        for db_campaign in db_campaigns:
            db_participants = all_db_participants[db_campaign.id]

            if (
                db_campaign.id in campaign_backup_threads
                and campaign_backup_threads[db_campaign.id].is_alive()
            ):
                continue
            else:
                campaign_backup_threads[db_campaign.id] = threading.Thread(
                    target=campaign_backup, args=(db_campaign, db_participants)
                )
                campaign_backup_threads[db_campaign.id].start()

        print(
            utils.get_timestamp_ms(),
            f'DQ : backing up campaigns {", ".join([str(x.id) for x in db_campaigns])}',
        )
        time.sleep(2 * 60)
