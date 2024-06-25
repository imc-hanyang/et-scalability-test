import json
import time

import grpc
from tqdm import tqdm

from et_grpcs import et_service_pb2, et_service_pb2_grpc

ip_address = "localhost"
port = 5432


def main():
    channel = grpc.insecure_channel(f"{ip_address}:{port}")
    stub = et_service_pb2_grpc.ETServiceStub(channel)

    # Try to register new dev user
    email = "dev@easytrack.com"
    req = et_service_pb2.Register.Request(
        username=email[: email.index("@")],
        name="EasyTrack Developer",
        password=email,
    )
    res = stub.register(request=req)
    if res.success:
        print(f"Dev user registered (username={email})")

    # Log in as dev user (ID = 0)
    req = et_service_pb2.Login.Request(
        username=email[: email.index("@")],
        password=email,
    )
    res = stub.login(request=req)
    assert res.success
    assert res.userId == 0
    user_id = res.userId
    session_key = res.sessionKey
    print(f"Dev user logged in (userId={res.userId})")

    # Create data sources
    config_json = []
    for data_source_name in ["DATA", "LOG"]:
        # Create data source
        req = et_service_pb2.CreateDataSource.Request(
            userId=user_id,
            sessionKey=session_key,
            name=data_source_name,
            iconName="miscellaneous-data-sources.png",
        )
        res = stub.createDataSource(request=req)
        assert res.success
        print(f"Created data source '{data_source_name}' (dataSourceId={res.dataSourceId})")

        # Append to config_json
        config_json.append(
            {
                "name": data_source_name,
                "icon_name": "miscellaneous-data-sources.png",
                "config_json": {},
                "data_source_id": res.dataSourceId,
            }
        )

    # Try to register a new campaign
    campaign_id = 0
    req = et_service_pb2.RegisterCampaign.Request(
        userId=user_id,
        sessionKey=session_key,
        campaignId=campaign_id,
        name="Scalability Test",
        notes="A campaign for testing the scalability of the system.",
        configJson=json.dumps(config_json),
        startTimestamp=int(time.time() * 1000),
        endTimestamp=int(time.time() * 1000 + 30 * 24 * 60 * 60 * 1000),
    )
    res = stub.registerCampaign(request=req)
    if res.success:
        print(f"Created new campaign (campaignId={campaign_id})")

    # Get campaign (ID = 0)
    req = et_service_pb2.RetrieveCampaign.Request(
        userId=user_id,
        sessionKey=session_key,
        campaignId=campaign_id,
    )
    res = stub.retrieveCampaign(request=req)
    assert res.success
    print(f"Retrieved campaign (campaignId={campaign_id})")

    # Create and bind 2048 users as campaign participants (id=1, 2, ..., 1024)
    for i in tqdm(range(1, 2049), desc="Creating participants", leave=False):
        # Try to register new participant
        email = (
            f"participant{i}"  # e.g. participant1, participant2, ..., participant1024
        )
        req = et_service_pb2.Register.Request(
            username=email,
            name=f"Participant {i}",
            password=email,
        )
        stub.register(request=req)

        # Login participant
        req = et_service_pb2.Login.Request(
            username=email,
            password=email,
        )
        res = stub.login(request=req)
        assert res.success

        # Bind participant to campaign
        req = et_service_pb2.BindUserToCampaign.Request(
            userId=res.userId,
            sessionKey=res.sessionKey,
            campaignId=campaign_id,
        )
        res = stub.bindUserToCampaign(request=req)
        assert res.success
    print("Created and bound 2048 participants to the campaign")

    channel.close()


if __name__ == "__main__":
    main()
