import os
import re
import time
from concurrent import futures

import grpc
from cassandra import UnresolvableContactPoints
from cassandra.cluster import NoHostAvailable

from et_grpcs import et_service_pb2, et_service_pb2_grpc
from tools import db_mgr as db
from tools import settings, utils


class ETServiceServicer(et_service_pb2_grpc.ETServiceServicer):
    # region User management module
    def register(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: register()")

        grpc_response = et_service_pb2.Register.Response()
        grpc_response.success = False

        # verify that request.username is minimum 4 characters long
        if len(request.username) < 3:
            print(f"{timestamp}: register(); invalid username")
            grpc_response.message = (
                "Username must be minimum 4 characters long"
            )
            return grpc_response
        # verify that password is minimum 4 characters long
        if len(request.password) < 3:
            print(f"{timestamp}: register(); invalid password")
            grpc_response.message = "Password must be minimum 4 characters long"
            return grpc_response

        db_user = db.get_user(email=f"{request.username}@easytrack.com")
        if db_user is None:
            # new user
            print("new user : ", end="")
            new_email = f"{request.username}@easytrack.com"
            db.create_user(
                name=request.name,
                email=new_email,
                session_key=request.password,
            )
            db_user = db.get_user(email=new_email)
            if db_user is not None:
                grpc_response.success = True
            else:
                grpc_response.message = (
                    "Failed to create user (contact backend developer)"
                )
                print(f"{timestamp}: register(); failed to create user")
        else:
            # already exists
            grpc_response.message = "Username already exists"
            print(f"{timestamp}: register(); username already exists")

        print(f"{timestamp}: register(); success = {grpc_response.success}")
        return grpc_response

    def login(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: login()")

        grpc_response = et_service_pb2.Login.Response()
        grpc_response.success = False

        # verify that request.username is minimum 3 characters long
        if len(request.username) < 3:
            print(f"{timestamp}: register(); invalid username")
            grpc_response.message = (
                "Username must be minimum 4 characters long"
            )
            return grpc_response
        # verify that password is minimum 3 characters long
        if len(request.password) < 3:
            print(f"{timestamp}: register(); invalid password")
            grpc_response.message = "Password must be minimum 4 characters long"
            return grpc_response

        email = f"{request.username}@easytrack.com"
        db_user = db.get_user(email=email)
        if db_user is not None and db_user.sessionKey == request.password:
            grpc_response.success = True
            grpc_response.userId = db_user.id
            grpc_response.name = db_user.name
            grpc_response.sessionKey = db_user.sessionKey

        print(f"{timestamp}: login(); success = {grpc_response.success}")
        return grpc_response

    def loginWithGoogle(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: loginWithGoogleId()")

        grpc_response = et_service_pb2.LoginWithGoogle.Response()
        grpc_response.success = False

        google_profile = utils.load_google_profile(id_token=request.idToken)
        session_key = utils.md5(value=f'{google_profile["email"]}{utils.now_us()}')
        db_user = (
            db.get_user(email=google_profile["email"])
            if google_profile is not None
            else None
        )

        if db_user is None:
            print("new user : ", end="")
            db.create_user(
                name=google_profile["name"],
                email=google_profile["email"],
                session_key=session_key,
            )
            db_user = db.get_user(email=google_profile["email"])
            if db_user is not None:
                grpc_response.userId = db_user.id
                grpc_response.sessionKey = session_key
                grpc_response.success = True
        else:
            db.update_session_key(db_user=db_user, session_key=session_key)
            grpc_response.userId = db_user.id
            grpc_response.sessionKey = session_key
            grpc_response.success = True

        print(f"{timestamp}: loginWithGoogleId(); success = {grpc_response.success}")
        return grpc_response

    def setTag(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: setTag()")

        grpc_response = et_service_pb2.BindUserToCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        if db_user is not None:
            db.set_user_tag(db_user=db_user, tag=request.tag)
            grpc_response.success = True

        print(f"{timestamp}: setTag(); success = {grpc_response.success}")
        return grpc_response

    def getTag(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: getTag()")

        grpc_response = et_service_pb2.BindUserToCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        if db_user is not None:
            grpc_response.tag = db_user.tag
            grpc_response.success = True

        print(f"{timestamp}: getTag(); success = {grpc_response.success}")
        return grpc_response

    def bindUserToCampaign(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: bindUserToCampaign()")

        grpc_response = et_service_pb2.BindUserToCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)

        if None not in [db_user, db_campaign]:
            grpc_response.isFirstTimeBinding = db.bind_participant_to_campaign(
                db_user=db_user, db_campaign=db_campaign
            )
            grpc_response.campaignStartTimestamp = db_campaign.startTimestamp
            grpc_response.success = True

        print(
            f"{timestamp}: bindUserToCampaign(newBinding={grpc_response.isFirstTimeBinding}); success = {grpc_response.success}"
        )
        return grpc_response

    def retrieveParticipants(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveParticipants()")

        grpc_response = et_service_pb2.RetrieveParticipants.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(
            campaign_id=request.campaignId, db_researcher_user=db_user
        )

        if (
            None not in [db_user, db_campaign]
            and db_user.sessionKey == request.sessionKey
        ):
            for row in db.get_campaign_participants(db_campaign=db_campaign):
                grpc_response.userId.extend([row.id])
                grpc_response.name.extend([row.name])
                grpc_response.email.extend([row.email])
            grpc_response.success = True

        print(f"{timestamp}: retrieveParticipants(); success = {grpc_response.success}")
        return grpc_response

    # endregion

    # region Campaign management module
    def registerCampaign(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: registerCampaign()")

        grpc_response = et_service_pb2.RegisterCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)

        if (
            db_user is not None
            and db_user.sessionKey == request.sessionKey
            and (db_campaign is None or db_campaign.creatorId == db_user.id)
        ):
            db.create_or_update_campaign(
                db_creator_user=db_user,
                db_campaign=db_campaign,
                name=request.name,
                notes=request.notes,
                configurations=request.configJson,
                start_timestamp=request.startTimestamp,
                end_timestamp=request.endTimestamp,
            )
            grpc_response.success = True

        print(f"{timestamp}: registerCampaign(); success = {grpc_response.success}")
        return grpc_response

    def deleteCampaign(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: deleteCampaign()")

        grpc_response = et_service_pb2.DeleteCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(
            campaign_id=request.campaignId, db_researcher_user=db_user
        )

        if (
            None not in [db_user, db_campaign]
            and db_user.sessionKey == request.sessionKey
        ):
            db.delete_campaign(db_campaign=db_campaign)
            grpc_response.success = True

        print(f"{timestamp}: deleteCampaign(); success = {grpc_response.success}")
        return grpc_response

    def retrieveCampaigns(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveCampaigns()")

        grpc_response = et_service_pb2.RetrieveCampaigns.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)

        if db_user is not None and db_user.sessionKey == request.sessionKey:
            db_campaigns = (
                db.get_campaigns(db_creator_user=db_user)
                if request.myCampaignsOnly
                else db.get_campaigns()
            )
            for db_campaign in db_campaigns:
                grpc_response.campaignId.extend([db_campaign.id])
                grpc_response.creatorEmail.extend([db_user.email])
                grpc_response.name.extend([db_campaign.name])
                grpc_response.notes.extend([db_campaign.notes])
                grpc_response.configJson.extend([db_campaign.configJson])
                grpc_response.startTimestamp.extend([db_campaign.startTimestamp])
                grpc_response.endTimestamp.extend([db_campaign.endTimestamp])
                grpc_response.participantCount.extend(
                    [db.get_campaign_participants_count(db_campaign=db_campaign)]
                )
            grpc_response.success = True

        print(f"{timestamp}: retrieveCampaigns(); success = {grpc_response.success}")
        return grpc_response

    def retrieveCampaign(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveCampaign()")

        grpc_response = et_service_pb2.RetrieveCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)

        is_not_none = None not in [db_user, db_campaign]
        session_key_valid = db_user.sessionKey == request.sessionKey
        user_matches = db_user.id == db_campaign.creatorId or db.user_is_bound_to_campaign(
            db_user=db_user, db_campaign=db_campaign
        )
        if is_not_none and session_key_valid and user_matches:
            grpc_response.name = db_campaign.name
            grpc_response.notes = db_campaign.notes
            grpc_response.creatorEmail = (
                db.get_user(user_id=db_campaign.creatorId).email
                if db_campaign.creatorId is not None
                else "N/A"
            )
            grpc_response.startTimestamp = db_campaign.startTimestamp
            grpc_response.endTimestamp = db_campaign.endTimestamp
            grpc_response.configJson = db_campaign.configJson
            grpc_response.participantCount = db.get_campaign_participants_count(
                db_campaign=db_campaign
            )
            grpc_response.success = True

        print(f"{timestamp}: retrieveCampaign(); success = {grpc_response.success}")
        return grpc_response

    # endregion

    # region Data source management module
    def bindDataSource(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: bindDataSource()")

        grpc_response = et_service_pb2.BindDataSource.Response()
        grpc_response.success = True

        db_user = db.get_user(user_id=request.userId)

        if db_user is not None and db_user.sessionKey == request.sessionKey:
            db_data_source = db.get_data_source(data_source_name=request.name)
            if db_data_source is None:
                print("new data source : ", end="")
                db.create_data_source(
                    db_creator_user=db_user,
                    name=request.name,
                    icon_name=request.iconName,
                )
                db_data_source = db.get_data_source(data_source_name=request.name)
            grpc_response.dataSourceId = db_data_source.id
            grpc_response.iconName = db_data_source.iconName
            grpc_response.success = True

        print(f"{timestamp}: bindDataSource(); success = {grpc_response.success}")
        return grpc_response

    def retrieveDataSources(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveDataSources()")

        grpc_response = et_service_pb2.RetrieveDataSources.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)

        if db_user is not None and db_user.sessionKey == request.sessionKey:
            for data_source in db.get_all_data_sources():
                grpc_response.dataSourceId.extend([data_source.id])
                grpc_response.creatorEmail.extend(
                    [
                        (
                            db.get_user(user_id=data_source.creatorId).email
                            if data_source.creatorId is not None
                            else "N/A"
                        )
                    ]
                )
                grpc_response.name.extend([data_source.name])
                grpc_response.iconName.extend([data_source.iconName])
            grpc_response.success = True

        print(f"{timestamp}: retrieveDataSources(); success = {grpc_response.success}")
        return grpc_response

    # endregion

    # region Data management module
    def submitDataRecord(self, request, context):
        # timestamp = int(time.time() * 1000)
        # print(f'{timestamp}: submitDataRecord()')

        grpc_response = et_service_pb2.SubmitDataRecord.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)
        db_data_source = db.get_data_source(data_source_id=request.dataSource)

        if None not in [
            db_user,
            db_campaign,
            db_data_source,
        ] and db.user_is_bound_to_campaign(db_user=db_user, db_campaign=db_campaign):
            f = db.get_file(
                db_campaign=db_campaign,
                db_user=db_user,
                db_data_source=db_data_source,
            )
            db.store_data_record_to_file(
                file=f,
                timestamp=request.timestamp,
                value=request.value,
            )
            f.close()
            grpc_response.success = True

        # print(f'{timestamp}: submitDataRecord(); success = {grpc_response.success}')
        return grpc_response

    def submitDataRecords(self, request, context):
        # timestamp = int(time.time() * 1000)
        # print(f'{timestamp}: submitDataRecords()')

        grpc_response = et_service_pb2.SubmitDataRecords.Response()
        grpc_response.success = False

        # db_user = db.get_user(user_id=request.userId)
        # db_campaign = db.get_campaign(campaign_id=request.campaignId)

        # if None not in [db_user, db_campaign] and db.user_is_bound_to_campaign(db_user=db_user, db_campaign=db_campaign) and len(request.timestamp) > 0:
        #     # db.store_data_records(db_user=db_user, db_campaign=db_campaign, timestamp_list=request.timestamp, data_source_id_list=request.dataSource, value_list=request.value)
        #     db.store_data_records_to_file(
        #         db_user=db_user,
        #         db_campaign=db_campaign,
        #         timestamp_list=request.timestamp,
        #         data_source_id_list=request.dataSource,
        #         value_list=request.value,
        #     )
        #     grpc_response.success = True

        # print(f'{timestamp} submitDataRecords(); success = {grpc_response.success}')
        grpc_response.success = True
        return grpc_response

    def retrieveKNextDataRecords(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveKNextDataRecords()")

        grpc_response = et_service_pb2.RetrieveKNextDataRecords.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_target_user = db.get_user(email=request.targetEmail)
        db_target_campaign = db.get_campaign(campaign_id=request.targetCampaignId)
        db_data_source = db.get_data_source(data_source_id=request.targetDataSourceId)

        if (
            None not in [db_user, db_target_user, db_target_campaign, db_data_source]
            and db_user.sessionKey == request.sessionKey
            and request.k <= 500
            and (
                db_user.id == db_target_campaign.creatorId
                or db.user_is_bound_to_campaign(
                    db_user=db_user, db_campaign=db_target_campaign
                )
            )
            and (
                db_target_user["id"] == db_target_campaign.creatorId
                or db.user_is_bound_to_campaign(
                    db_user=db_target_user, db_campaign=db_target_campaign
                )
            )
        ):
            data_records = db.get_next_k_data_records(
                db_campaign=db_target_campaign,
                db_user=db_target_user,
                db_data_source=db_data_source,
                from_timestamp=request.fromTimestamp,
                k=request.k,
            )
            for data_record in data_records:
                grpc_response.timestamp.extend([data_record.timestamp])
                grpc_response.value.extend([data_record.value])
            grpc_response.success = True

        print(
            f"{timestamp} retrieveKNextDataRecords(); success = {grpc_response.success}"
        )
        return grpc_response

    def retrieveFilteredDataRecords(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveFilteredDataRecords()")

        grpc_response = et_service_pb2.RetrieveFilteredDataRecords.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_target_user = db.get_user(email=request.targetEmail)
        db_target_campaign = db.get_campaign(campaign_id=request.targetCampaignId)
        db_data_source = db.get_data_source(data_source_id=request.targetDataSourceId)
        from_timestamp = request.fromTimestamp
        till_timestamp = request.tillTimestamp

        if None not in [
            db_user,
            db_target_user,
            db_target_campaign,
            db_data_source,
        ] and db.user_is_bound_to_campaign(
            db_user=db_target_user, db_campaign=db_target_campaign
        ):
            data_records = db.get_filtered_data_records(
                db_user=db_target_user,
                db_campaign=db_target_campaign,
                db_data_source=db_data_source,
                from_timestamp=from_timestamp,
                till_timestamp=till_timestamp,
            )
            for data_record in data_records:
                grpc_response.dataSource.extend([data_record.dataSourceId])
                grpc_response.timestamp.extend([data_record.timestamp])
                value = data_record.value
                if request.simplifyIfTooLarge and len(value) > 500:
                    value = bytes(f"[{len(value)}] bytes", "utf8")
                grpc_response.value.extend([value])
            grpc_response.success = True

        print(
            f"{timestamp} retrieveFilteredDataRecords(); success = {grpc_response.success}"
        )
        return grpc_response

    def downloadDumpfile(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: downloadDumpfile()")

        grpc_response = et_service_pb2.DownloadDumpfile.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)
        db_target_user = db.get_user(email=request.targetEmail)

        if (
            None not in [db_user, db_campaign, db_target_user]
            and db_user.sessionKey == request.sessionKey
            and db_campaign.creatorId == db_user.id
            and db.user_is_bound_to_campaign(
                db_user=db_target_user, db_campaign=db_campaign
            )
        ):
            file_path = db.dump_data(db_campaign=db_campaign, db_user=db_target_user)
            with open(file_path, "rb") as r:
                grpc_response.dump = bytes(r.read())
            os.remove(file_path)
            grpc_response.success = True

        print(f"{timestamp}: downloadDumpfile(); success = {grpc_response.success}")
        return grpc_response

    # endregion

    # region Statistics module
    def submitHeartbeat(self, request, context):
        # timestamp = int(time.time() * 1000)
        # print(f'{timestamp}: submitHeartbeat()')

        grpc_response = et_service_pb2.SubmitHeartbeat.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)

        if None not in [db_user, db_campaign] and db.user_is_bound_to_campaign(
            db_user=db_user, db_campaign=db_campaign
        ):
            db.update_user_heartbeat_timestamp(db_user=db_user, db_campaign=db_campaign)
            grpc_response.success = True

        # print(f'{timestamp}: submitHeartbeat(); success = {grpc_response.success}')
        return grpc_response

    def retrieveParticipantStats(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveParticipantStatistics()")

        grpc_response = et_service_pb2.RetrieveParticipantStats.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_target_campaign = db.get_campaign(campaign_id=request.targetCampaignId)
        db_target_user = db.get_user(email=request.targetEmail)

        if (
            None not in [db_user, db_target_user, db_target_campaign]
            and db_user.sessionKey == request.sessionKey
            and db.user_is_bound_to_campaign(
                db_user=db_target_user, db_campaign=db_target_campaign
            )
        ):
            grpc_response.campaignJoinTimestamp = db.get_participant_join_timestamp(
                db_user=db_target_user, db_campaign=db_target_campaign
            )
            grpc_response.lastSyncTimestamp = db.get_participant_last_sync_timestamp(
                db_user=db_target_user, db_campaign=db_target_campaign
            )
            grpc_response.lastHeartbeatTimestamp = (
                db.get_participant_heartbeat_timestamp(
                    db_user=db_target_user, db_campaign=db_target_campaign
                )
            )
            grpc_response.amountOfSubmittedDataSamples = (
                db.get_participants_amount_of_data(
                    db_user=db_target_user, db_campaign=db_target_campaign
                )
            )
            for (
                data_source,
                amount_of_data,
                last_sync_time,
            ) in db.get_participants_per_data_source_stats(
                db_user=db_target_user, db_campaign=db_target_campaign
            ):
                grpc_response.dataSourceId.extend([data_source.id])
                grpc_response.perDataSourceAmountOfData.extend([amount_of_data])
                grpc_response.perDataSourceLastSyncTimestamp.extend([last_sync_time])
            grpc_response.success = True

        print(
            f"{timestamp}: retrieveParticipantStatistics(); success = {grpc_response.success}"
        )
        return grpc_response

    # endregion

    # region Communication management module
    def submitDirectMessage(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: submitDirectMessage()")

        grpc_response = et_service_pb2.SubmitDirectMessage.Response()
        grpc_response.success = False

        db_source_user = db.get_user(user_id=request.userId)
        db_target_user = db.get_user(email=request.targetEmail)

        if (
            None not in [db_source_user, db_target_user]
            and db_source_user.sessionKey == request.sessionKey
        ):
            db_direct_message = db.create_direct_message(
                db_source_user=db_source_user,
                db_target_user=db_target_user,
                subject=request.subject,
                content=request.content,
            )
            grpc_response.success = True
            grpc_response.id = db_direct_message.id

        print(f"{timestamp}: submitDirectMessage(); success = {grpc_response.success}")
        return grpc_response

    def retrieveUnreadDirectMessages(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveUnreadDirectMessages()")

        grpc_response = et_service_pb2.RetrieveUnreadDirectMessages.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)

        if db_user is not None and db_user.sessionKey == request.sessionKey:
            for db_direct_message in db.get_unread_direct_messages(db_user=db_user):
                grpc_response.id.extend([db_direct_message.id])
                grpc_response.sourceEmail.extend(
                    [db.get_user(user_id=db_direct_message.sourceUserId).email]
                )
                grpc_response.timestamp.extend([db_direct_message.timestamp])
                grpc_response.subject.extend([db_direct_message.subject])
                grpc_response.content.extend([db_direct_message.content])
            grpc_response.success = True

        print(
            f"{timestamp}: retrieveUnreadDirectMessages(); success = {grpc_response.success}"
        )
        return grpc_response

    def submitNotification(self, request, context):
        pass

    def retrieveUnreadNotifications(self, request, context):
        timestamp = int(time.time() * 1000)
        print(f"{timestamp}: retrieveUnreadNotifications()")

        grpc_response = et_service_pb2.RetrieveUnreadNotifications.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)

        if db_user is not None and db_user.sessionKey == request.sessionKey:
            for notification in db.get_unread_notifications(db_user=db_user):
                grpc_response.id.extend([notification.id])
                grpc_response.campaignId.extend([notification.campaignId])
                grpc_response.timestamp.extend([notification.timestamp])
                grpc_response.subject.extend([notification.subject])
                grpc_response.content.extend([notification.content])
            grpc_response.success = True

        print(
            f"{timestamp} retrieveUnreadNotifications(); success = {grpc_response.success}"
        )
        return grpc_response

    # endregion


def main():
    wait_db = True
    while wait_db:
        try:
            session = db.get_cassandra_session()
            if session is None:
                time.sleep(5)
                print("Waiting for DB to boot up...")
                continue

            wait_db = False
            res = session.execute(
                "select count(*) from system_schema.keyspaces where keyspace_name='et';"
            )
            init_necessary = res.one()[0] == 0
            if init_necessary:
                print("DB initialization necessary, initializing now...")
                with open("assets/schema.cql") as r:
                    for line in r:
                        session.execute(line[:-1])
        except (UnresolvableContactPoints, NoHostAvailable):
            time.sleep(5)
            print("Waiting for DB to boot up...")
    print("DB is ready! booting server now...")

    # !note!
    if not os.path.exists(settings.download_dir):
        os.mkdir(settings.download_dir)
        os.chmod(settings.download_dir, 0o777)
    print(
        f'! please give "full control" permissions of the path "{settings.download_dir}" to "Everyone" !'
    )

    # create a gRPC server
    max_message_length = 2147483647
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1000),
        options=[
            ("grpc.max_send_message_length", max_message_length),
            ("grpc.max_receive_message_length", max_message_length),
            ("grpc.keepalive_time_ms", 900000),
            # send keepalive ping every 15 minutes, default is 2 hours
            ("grpc.keepalive_timeout_ms", 5000),
            # keepalive ping time out after 5 seconds, default is 20 seconds
            ("grpc.keepalive_permit_without_calls", True),
            # allow keepalive pings when there's no gRPC calls
            ("grpc.http2.max_pings_without_data", 0),
            # allow unlimited amount of keepalive pings without data
            ("grpc.http2.min_time_between_pings_ms", 900000),
            # allow grpc pings from client every 15 minutes
            ("grpc.http2.min_ping_interval_without_data_ms", 5000),
            # allow grpc pings from client without data every 5 seconds
        ],
    )

    # use the generated function `add_ETServiceServicer_to_server` to add the defined class to the server
    et_service_pb2_grpc.add_ETServiceServicer_to_server(ETServiceServicer(), server)

    # listen on port 5432 [ server.add_insecure_port('[::]:5432') ]
    print("Starting gRPC server on port 5432.")
    server.add_insecure_port("0.0.0.0:5432")
    server.start()

    # since server.start() will not block, a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
        db.end()
        print("Server has stopped.")


if __name__ == "__main__":
    main()
