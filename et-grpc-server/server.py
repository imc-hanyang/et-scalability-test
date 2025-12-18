"""
EasyTrack gRPC Server.

This module implements the main gRPC server for the EasyTrack platform,
providing services for user management, campaign management, data source
management, data submission/retrieval, statistics, and communication.
"""
import logging
import os
import time
from concurrent import futures
from typing import Any

import grpc
from cassandra import UnresolvableContactPoints
from cassandra.cluster import NoHostAvailable

from et_grpcs import et_service_pb2, et_service_pb2_grpc
from tools import db_mgr as db
from tools import settings, utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ETServiceServicer(et_service_pb2_grpc.ETServiceServicer):
    """
    gRPC service implementation for EasyTrack.

    Provides RPC methods for user management, campaign management,
    data source management, data submission/retrieval, statistics,
    and communication between users.
    """

    # ==========================================================================
    # User Management Module
    # ==========================================================================

    def register(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.Register.Response:
        """
        Register a new user.

        Args:
            request: Register request containing username, name, and password.
            context: gRPC context.

        Returns:
            Register response indicating success or failure.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: register()", timestamp)

        grpc_response = et_service_pb2.Register.Response()
        grpc_response.success = False

        # Validate username length
        if len(request.username) < settings.MIN_USERNAME_LENGTH:
            logger.warning("%s: register() - invalid username", timestamp)
            grpc_response.message = (
                f"Username must be minimum {settings.MIN_USERNAME_LENGTH} characters long"
            )
            return grpc_response

        # Validate password length
        if len(request.password) < settings.MIN_PASSWORD_LENGTH:
            logger.warning("%s: register() - invalid password", timestamp)
            grpc_response.message = (
                f"Password must be minimum {settings.MIN_PASSWORD_LENGTH} characters long"
            )
            return grpc_response

        db_user = db.get_user(email=f"{request.username}@easytrack.com")
        if db_user is None:
            # New user
            logger.info("Creating new user: %s", request.username)
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
                logger.error("%s: register() - failed to create user", timestamp)
        else:
            # User already exists
            grpc_response.message = "Username already exists"
            logger.warning("%s: register() - username already exists", timestamp)

        logger.info("%s: register() - success = %s", timestamp, grpc_response.success)
        return grpc_response

    def login(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.Login.Response:
        """
        Log in an existing user.

        Args:
            request: Login request containing username and password.
            context: gRPC context.

        Returns:
            Login response with user info if successful.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: login()", timestamp)

        grpc_response = et_service_pb2.Login.Response()
        grpc_response.success = False

        # Validate username length
        if len(request.username) < settings.MIN_USERNAME_LENGTH:
            logger.warning("%s: login() - invalid username", timestamp)
            grpc_response.message = (
                f"Username must be minimum {settings.MIN_USERNAME_LENGTH} characters long"
            )
            return grpc_response

        # Validate password length
        if len(request.password) < settings.MIN_PASSWORD_LENGTH:
            logger.warning("%s: login() - invalid password", timestamp)
            grpc_response.message = (
                f"Password must be minimum {settings.MIN_PASSWORD_LENGTH} characters long"
            )
            return grpc_response

        email = f"{request.username}@easytrack.com"
        db_user = db.get_user(email=email)
        if db_user is not None and db_user.sessionKey == request.password:
            grpc_response.success = True
            grpc_response.userId = db_user.id
            grpc_response.name = db_user.name
            grpc_response.sessionKey = db_user.sessionKey

        logger.info("%s: login() - success = %s", timestamp, grpc_response.success)
        return grpc_response

    def loginWithGoogle(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.LoginWithGoogle.Response:
        """
        Log in or register a user using Google OAuth.

        Args:
            request: LoginWithGoogle request containing Google ID token.
            context: gRPC context.

        Returns:
            LoginWithGoogle response with user info if successful.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: loginWithGoogle()", timestamp)

        grpc_response = et_service_pb2.LoginWithGoogle.Response()
        grpc_response.success = False

        google_profile = utils.load_google_profile(id_token=request.idToken)
        if google_profile is None:
            return grpc_response

        session_key = utils.md5(value=f'{google_profile["email"]}{utils.now_us()}')
        db_user = db.get_user(email=google_profile["email"])

        if db_user is None:
            # New user
            logger.info("Creating new Google user: %s", google_profile["email"])
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
            # Existing user - update session key
            db.update_session_key(db_user=db_user, session_key=session_key)
            grpc_response.userId = db_user.id
            grpc_response.sessionKey = session_key
            grpc_response.success = True

        logger.info(
            "%s: loginWithGoogle() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    def setTag(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.BindUserToCampaign.Response:
        """
        Set a tag for a user.

        Args:
            request: SetTag request containing userId and tag.
            context: gRPC context.

        Returns:
            Response indicating success or failure.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: setTag()", timestamp)

        grpc_response = et_service_pb2.BindUserToCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        if db_user is not None:
            db.set_user_tag(db_user=db_user, tag=request.tag)
            grpc_response.success = True

        logger.info("%s: setTag() - success = %s", timestamp, grpc_response.success)
        return grpc_response

    def getTag(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.BindUserToCampaign.Response:
        """
        Get a user's tag.

        Args:
            request: GetTag request containing userId.
            context: gRPC context.

        Returns:
            Response containing the user's tag.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: getTag()", timestamp)

        grpc_response = et_service_pb2.BindUserToCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        if db_user is not None:
            grpc_response.tag = db_user.tag
            grpc_response.success = True

        logger.info("%s: getTag() - success = %s", timestamp, grpc_response.success)
        return grpc_response

    def bindUserToCampaign(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.BindUserToCampaign.Response:
        """
        Bind a user to a campaign as a participant.

        Args:
            request: BindUserToCampaign request with userId and campaignId.
            context: gRPC context.

        Returns:
            Response with binding status and campaign start timestamp.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: bindUserToCampaign()", timestamp)

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

        logger.info(
            "%s: bindUserToCampaign(newBinding=%s) - success = %s",
            timestamp,
            grpc_response.isFirstTimeBinding,
            grpc_response.success,
        )
        return grpc_response

    def retrieveParticipants(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveParticipants.Response:
        """
        Retrieve all participants of a campaign.

        Args:
            request: RetrieveParticipants request with campaign info.
            context: gRPC context.

        Returns:
            Response containing participant information.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveParticipants()", timestamp)

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

        logger.info(
            "%s: retrieveParticipants() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    # ==========================================================================
    # Campaign Management Module
    # ==========================================================================

    def registerCampaign(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RegisterCampaign.Response:
        """
        Register a new campaign or update an existing one.

        Args:
            request: RegisterCampaign request with campaign details.
            context: gRPC context.

        Returns:
            Response indicating success or failure.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: registerCampaign()", timestamp)

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

        logger.info(
            "%s: registerCampaign() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    def deleteCampaign(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.DeleteCampaign.Response:
        """
        Delete a campaign.

        Args:
            request: DeleteCampaign request with campaign info.
            context: gRPC context.

        Returns:
            Response indicating success or failure.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: deleteCampaign()", timestamp)

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

        logger.info(
            "%s: deleteCampaign() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    def retrieveCampaigns(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveCampaigns.Response:
        """
        Retrieve campaigns for a user.

        Args:
            request: RetrieveCampaigns request.
            context: gRPC context.

        Returns:
            Response containing campaign information.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveCampaigns()", timestamp)

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

        logger.info(
            "%s: retrieveCampaigns() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    def retrieveCampaign(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveCampaign.Response:
        """
        Retrieve a single campaign.

        Args:
            request: RetrieveCampaign request with campaignId.
            context: gRPC context.

        Returns:
            Response containing campaign details.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveCampaign()", timestamp)

        grpc_response = et_service_pb2.RetrieveCampaign.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)

        if db_user is None or db_campaign is None:
            return grpc_response

        session_key_valid = db_user.sessionKey == request.sessionKey
        user_matches = db_user.id == db_campaign.creatorId or db.user_is_bound_to_campaign(
            db_user=db_user, db_campaign=db_campaign
        )

        if session_key_valid and user_matches:
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

        logger.info(
            "%s: retrieveCampaign() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    # ==========================================================================
    # Data Source Management Module
    # ==========================================================================

    def createDataSource(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.CreateDataSource.Response:
        """
        Create a new data source.

        Args:
            request: CreateDataSource request with data source details.
            context: gRPC context.

        Returns:
            Response with created data source ID.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: createDataSource()", timestamp)

        grpc_response = et_service_pb2.CreateDataSource.Response()
        grpc_response.success = True

        db_user = db.get_user(user_id=request.userId)

        if db_user is not None and db_user.sessionKey == request.sessionKey:
            db_data_source = db.get_data_source(data_source_name=request.name)
            if db_data_source is None:
                logger.info("Creating new data source: %s", request.name)
                db.create_data_source(
                    db_creator_user=db_user,
                    name=request.name,
                    icon_name=request.iconName,
                )
                db_data_source = db.get_data_source(data_source_name=request.name)
            grpc_response.dataSourceId = db_data_source.id
            grpc_response.success = True

        logger.info(
            "%s: createDataSource() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    def retrieveDataSources(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveDataSources.Response:
        """
        Retrieve all data sources.

        Args:
            request: RetrieveDataSources request.
            context: gRPC context.

        Returns:
            Response containing data source information.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveDataSources()", timestamp)

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

        logger.info(
            "%s: retrieveDataSources() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    # ==========================================================================
    # Data Management Module
    # ==========================================================================

    def submitDataRecord(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.SubmitDataRecord.Response:
        """
        Submit a single data record.

        Args:
            request: SubmitDataRecord request with data record.
            context: gRPC context.

        Returns:
            Response indicating success or failure.
        """
        grpc_response = et_service_pb2.SubmitDataRecord.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)
        db_data_source = db.get_data_source(data_source_id=request.dataSource)

        if (
            None not in [db_user, db_campaign, db_data_source]
            and db.user_is_bound_to_campaign(db_user=db_user, db_campaign=db_campaign)
        ):
            db.store_data_record(
                db_user=db_user,
                db_campaign=db_campaign,
                db_data_source=db_data_source,
                timestamp=request.timestamp,
                value=request.value,
            )
            grpc_response.success = True

        return grpc_response

    def submitDataRecords(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.SubmitDataRecords.Response:
        """
        Submit multiple data records in batch.

        Args:
            request: SubmitDataRecords request with data records.
            context: gRPC context.

        Returns:
            Response indicating success or failure.
        """
        grpc_response = et_service_pb2.SubmitDataRecords.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)

        if (
            None not in [db_user, db_campaign]
            and db.user_is_bound_to_campaign(db_user=db_user, db_campaign=db_campaign)
            and len(request.timestamp) > 0
        ):
            db.store_data_records(
                db_user=db_user,
                db_campaign=db_campaign,
                timestamp_list=request.timestamp,
                data_source_id_list=request.dataSource,
                value_list=request.value,
            )
            grpc_response.success = True

        return grpc_response

    def retrieveKNextDataRecords(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveKNextDataRecords.Response:
        """
        Retrieve the next K data records after a timestamp.

        Args:
            request: RetrieveKNextDataRecords request.
            context: gRPC context.

        Returns:
            Response containing data records.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveKNextDataRecords()", timestamp)

        grpc_response = et_service_pb2.RetrieveKNextDataRecords.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_target_user = db.get_user(email=request.targetEmail)
        db_target_campaign = db.get_campaign(campaign_id=request.targetCampaignId)
        db_data_source = db.get_data_source(data_source_id=request.targetDataSourceId)

        if (
            None not in [db_user, db_target_user, db_target_campaign, db_data_source]
            and db_user.sessionKey == request.sessionKey
            and request.k <= settings.MAX_K_RECORDS
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

        logger.info(
            "%s: retrieveKNextDataRecords() - success = %s",
            timestamp,
            grpc_response.success,
        )
        return grpc_response

    def retrieveFilteredDataRecords(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveFilteredDataRecords.Response:
        """
        Retrieve data records filtered by timestamp range.

        Args:
            request: RetrieveFilteredDataRecords request.
            context: gRPC context.

        Returns:
            Response containing filtered data records.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveFilteredDataRecords()", timestamp)

        grpc_response = et_service_pb2.RetrieveFilteredDataRecords.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_target_user = db.get_user(email=request.targetEmail)
        db_target_campaign = db.get_campaign(campaign_id=request.targetCampaignId)
        db_data_source = db.get_data_source(data_source_id=request.targetDataSourceId)
        from_timestamp = request.fromTimestamp
        till_timestamp = request.tillTimestamp

        if (
            None not in [db_user, db_target_user, db_target_campaign, db_data_source]
            and db.user_is_bound_to_campaign(
                db_user=db_target_user, db_campaign=db_target_campaign
            )
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

        logger.info(
            "%s: retrieveFilteredDataRecords() - success = %s",
            timestamp,
            grpc_response.success,
        )
        return grpc_response

    def downloadDumpfile(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.DownloadDumpfile.Response:
        """
        Download a data dump file for a user.

        Args:
            request: DownloadDumpfile request.
            context: gRPC context.

        Returns:
            Response containing the dump file data.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: downloadDumpfile()", timestamp)

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
            with open(file_path, "rb") as dump_file:
                grpc_response.dump = bytes(dump_file.read())
            os.remove(file_path)
            grpc_response.success = True

        logger.info(
            "%s: downloadDumpfile() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    # ==========================================================================
    # Statistics Module
    # ==========================================================================

    def submitHeartbeat(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.SubmitHeartbeat.Response:
        """
        Submit a heartbeat from a participant.

        Args:
            request: SubmitHeartbeat request.
            context: gRPC context.

        Returns:
            Response indicating success or failure.
        """
        grpc_response = et_service_pb2.SubmitHeartbeat.Response()
        grpc_response.success = False

        db_user = db.get_user(user_id=request.userId)
        db_campaign = db.get_campaign(campaign_id=request.campaignId)

        if (
            None not in [db_user, db_campaign]
            and db.user_is_bound_to_campaign(db_user=db_user, db_campaign=db_campaign)
        ):
            db.update_user_heartbeat_timestamp(db_user=db_user, db_campaign=db_campaign)
            grpc_response.success = True

        return grpc_response

    def retrieveParticipantStats(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveParticipantStats.Response:
        """
        Retrieve statistics for a participant.

        Args:
            request: RetrieveParticipantStats request.
            context: gRPC context.

        Returns:
            Response containing participant statistics.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveParticipantStats()", timestamp)

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

        logger.info(
            "%s: retrieveParticipantStats() - success = %s",
            timestamp,
            grpc_response.success,
        )
        return grpc_response

    # ==========================================================================
    # Communication Management Module
    # ==========================================================================

    def submitDirectMessage(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.SubmitDirectMessage.Response:
        """
        Submit a direct message to another user.

        Args:
            request: SubmitDirectMessage request.
            context: gRPC context.

        Returns:
            Response with message ID if successful.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: submitDirectMessage()", timestamp)

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

        logger.info(
            "%s: submitDirectMessage() - success = %s", timestamp, grpc_response.success
        )
        return grpc_response

    def retrieveUnreadDirectMessages(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveUnreadDirectMessages.Response:
        """
        Retrieve unread direct messages for a user.

        Args:
            request: RetrieveUnreadDirectMessages request.
            context: gRPC context.

        Returns:
            Response containing unread messages.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveUnreadDirectMessages()", timestamp)

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

        logger.info(
            "%s: retrieveUnreadDirectMessages() - success = %s",
            timestamp,
            grpc_response.success,
        )
        return grpc_response

    def submitNotification(
        self, request: Any, context: grpc.ServicerContext
    ) -> None:
        """
        Submit a notification to campaign participants.

        Args:
            request: SubmitNotification request.
            context: gRPC context.

        Returns:
            None (not implemented).

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError("submitNotification is not yet implemented")

    def retrieveUnreadNotifications(
        self, request: Any, context: grpc.ServicerContext
    ) -> et_service_pb2.RetrieveUnreadNotifications.Response:
        """
        Retrieve unread notifications for a user.

        Args:
            request: RetrieveUnreadNotifications request.
            context: gRPC context.

        Returns:
            Response containing unread notifications.
        """
        timestamp = int(time.time() * 1000)
        logger.info("%s: retrieveUnreadNotifications()", timestamp)

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

        logger.info(
            "%s: retrieveUnreadNotifications() - success = %s",
            timestamp,
            grpc_response.success,
        )
        return grpc_response


def main() -> None:
    """
    Main entry point for the gRPC server.

    Initializes the database connection, creates the gRPC server,
    and starts listening for requests.
    """
    # Wait for database to be ready
    wait_db = True
    while wait_db:
        try:
            session = db.get_cassandra_session()
            if session is None:
                time.sleep(5)
                logger.info("Waiting for DB to boot up...")
                continue

            wait_db = False
            res = session.execute(
                "select count(*) from system_schema.keyspaces where keyspace_name='et';"
            )
            init_necessary = res.one()[0] == 0

            if init_necessary:
                logger.info("DB initialization necessary, initializing now...")
                with open("assets/schema.cql", "r", encoding="utf-8") as schema_file:
                    cql = schema_file.read()

                for stmt in cql.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        session.execute(stmt)

        except (UnresolvableContactPoints, NoHostAvailable):
            time.sleep(5)
            logger.info("Waiting for DB to boot up...")

    logger.info("DB is ready! Booting server now...")

    # Ensure download directory exists
    if not os.path.exists(settings.download_dir):
        os.mkdir(settings.download_dir)
        os.chmod(settings.download_dir, 0o777)

    logger.info(
        'Please give "full control" permissions of the path "%s" to "Everyone"',
        settings.download_dir,
    )

    # Configure gRPC server options
    grpc_options = [
        ("grpc.max_send_message_length", settings.MAX_MESSAGE_LENGTH),
        ("grpc.max_receive_message_length", settings.MAX_MESSAGE_LENGTH),
        ("grpc.keepalive_time_ms", settings.GRPC_KEEPALIVE_TIME_MS),
        ("grpc.keepalive_timeout_ms", settings.GRPC_KEEPALIVE_TIMEOUT_MS),
        ("grpc.keepalive_permit_without_calls", True),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.http2.min_time_between_pings_ms", settings.GRPC_KEEPALIVE_TIME_MS),
        (
            "grpc.http2.min_ping_interval_without_data_ms",
            settings.GRPC_MIN_PING_INTERVAL_MS,
        ),
    ]

    # Create gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=settings.MAX_GRPC_WORKERS),
        options=grpc_options,
    )

    # Add service to server
    et_service_pb2_grpc.add_ETServiceServicer_to_server(ETServiceServicer(), server)

    # Start server
    server_port = 50051
    logger.info("Starting gRPC server on port %d.", server_port)
    server.add_insecure_port(f"0.0.0.0:{server_port}")
    server.start()

    # Keep server running
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
        db.end()
        logger.info("Server has stopped.")


if __name__ == "__main__":
    main()
