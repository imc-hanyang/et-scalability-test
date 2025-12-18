"""
Scalability Test Initialization Script.

This script initializes the EasyTrack gRPC server for scalability testing by:
1. Creating a developer user
2. Setting up data sources
3. Creating a test campaign
4. Registering and binding multiple participants
"""
import json
import logging
import time
from typing import Any

import grpc
from tqdm import tqdm

from et_grpcs import et_service_pb2, et_service_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration constants
SERVER_HOST: str = "localhost"
SERVER_PORT: int = 50051
CAMPAIGN_DURATION_DAYS: int = 30
NUM_PARTICIPANTS: int = 2048
DEV_USERNAME: str = "dev"
DEV_EMAIL: str = f"{DEV_USERNAME}@easytrack.com"


def create_grpc_channel(host: str, port: int) -> grpc.Channel:
    """
    Create a gRPC channel to the server.

    Args:
        host: Server hostname.
        port: Server port.

    Returns:
        gRPC channel object.
    """
    return grpc.insecure_channel(f"{host}:{port}")


def register_dev_user(stub: Any) -> None:
    """
    Register the developer user.

    Args:
        stub: gRPC service stub.
    """
    req = et_service_pb2.Register.Request(
        username=DEV_USERNAME,
        name="EasyTrack Developer",
        password=DEV_EMAIL,
    )
    res = stub.register(request=req)
    if res.success:
        logger.info("Dev user registered (username=%s)", DEV_EMAIL)


def login_dev_user(stub: Any) -> tuple:
    """
    Log in as the developer user.

    Args:
        stub: gRPC service stub.

    Returns:
        Tuple of (user_id, session_key).

    Raises:
        AssertionError: If login fails or user ID is not 0.
    """
    req = et_service_pb2.Login.Request(
        username=DEV_USERNAME,
        password=DEV_EMAIL,
    )
    res = stub.login(request=req)
    assert res.success, "Dev user login failed"
    assert res.userId == 0, f"Expected userId 0, got {res.userId}"

    logger.info("Dev user logged in (userId=%d)", res.userId)
    return res.userId, res.sessionKey


def create_data_sources(stub: Any, user_id: int, session_key: str) -> list:
    """
    Create data sources for the test campaign.

    Args:
        stub: gRPC service stub.
        user_id: Developer user ID.
        session_key: Developer session key.

    Returns:
        List of data source configurations for the campaign.
    """
    config_json = []
    data_source_names = ["DATA", "LOG"]

    for data_source_name in data_source_names:
        req = et_service_pb2.CreateDataSource.Request(
            userId=user_id,
            sessionKey=session_key,
            name=data_source_name,
            iconName="miscellaneous-data-sources.png",
        )
        res = stub.createDataSource(request=req)
        assert res.success, f"Failed to create data source '{data_source_name}'"

        logger.info(
            "Created data source '%s' (dataSourceId=%d)",
            data_source_name,
            res.dataSourceId,
        )

        config_json.append({
            "name": data_source_name,
            "icon_name": "miscellaneous-data-sources.png",
            "config_json": {},
            "data_source_id": res.dataSourceId,
        })

    return config_json


def create_campaign(
    stub: Any,
    user_id: int,
    session_key: str,
    config_json: list,
) -> int:
    """
    Create the scalability test campaign.

    Args:
        stub: gRPC service stub.
        user_id: Developer user ID.
        session_key: Developer session key.
        config_json: Data source configuration list.

    Returns:
        Campaign ID.
    """
    campaign_id = 0
    current_time_ms = int(time.time() * 1000)
    end_time_ms = current_time_ms + (CAMPAIGN_DURATION_DAYS * 24 * 60 * 60 * 1000)

    req = et_service_pb2.RegisterCampaign.Request(
        userId=user_id,
        sessionKey=session_key,
        campaignId=campaign_id,
        name="Scalability Test",
        notes="A campaign for testing the scalability of the system.",
        configJson=json.dumps(config_json),
        startTimestamp=current_time_ms,
        endTimestamp=end_time_ms,
    )
    res = stub.registerCampaign(request=req)
    if res.success:
        logger.info("Created new campaign (campaignId=%d)", campaign_id)

    # Verify campaign was created
    req = et_service_pb2.RetrieveCampaign.Request(
        userId=user_id,
        sessionKey=session_key,
        campaignId=campaign_id,
    )
    res = stub.retrieveCampaign(request=req)
    assert res.success, "Failed to retrieve campaign"
    logger.info("Retrieved campaign (campaignId=%d)", campaign_id)

    return campaign_id


def create_and_bind_participants(stub: Any, campaign_id: int) -> None:
    """
    Create participant users and bind them to the campaign.

    Args:
        stub: gRPC service stub.
        campaign_id: Campaign ID to bind participants to.
    """
    for i in tqdm(range(1, NUM_PARTICIPANTS + 1), desc="Creating participants", leave=False):
        username = f"participant{i}"

        # Register participant
        req = et_service_pb2.Register.Request(
            username=username,
            name=f"Participant {i}",
            password=username,
        )
        stub.register(request=req)

        # Login participant
        req = et_service_pb2.Login.Request(
            username=username,
            password=username,
        )
        res = stub.login(request=req)
        assert res.success, f"Failed to login participant {i}"

        # Bind participant to campaign
        req = et_service_pb2.BindUserToCampaign.Request(
            userId=res.userId,
            sessionKey=res.sessionKey,
            campaignId=campaign_id,
        )
        res = stub.bindUserToCampaign(request=req)
        assert res.success, f"Failed to bind participant {i} to campaign"

    logger.info("Created and bound %d participants to the campaign", NUM_PARTICIPANTS)


def main() -> None:
    """
    Main entry point for the scalability test initialization.
    """
    channel = create_grpc_channel(SERVER_HOST, SERVER_PORT)
    stub = et_service_pb2_grpc.ETServiceStub(channel)

    try:
        # Step 1: Register dev user
        register_dev_user(stub)

        # Step 2: Login as dev user
        user_id, session_key = login_dev_user(stub)

        # Step 3: Create data sources
        config_json = create_data_sources(stub, user_id, session_key)

        # Step 4: Create campaign
        campaign_id = create_campaign(stub, user_id, session_key, config_json)

        # Step 5: Create and bind participants
        create_and_bind_participants(stub, campaign_id)

    finally:
        channel.close()


if __name__ == "__main__":
    main()
