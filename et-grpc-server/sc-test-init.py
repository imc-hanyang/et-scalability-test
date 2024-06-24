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
    username = "dev@easytrack.com"
    req = et_service_pb2.Register.Request(
        username=username,
        name="EasyTrack Developer",
        password=username,
    )
    res = stub.register(request=req)
    if res.success:
        print(f"Dev user registered (username={username})")

    # Log in as dev user (ID = 0)
    req = et_service_pb2.Login.Request(
        username=username,
        password=username,
    )
    res = stub.login(request=req)
    assert res.success
    assert res.userId == 0
    user_id = res.userId
    session_key = res.sessionKey
    print(f"Dev user logged in (userId={res.userId})")

    # Try to register a new campaign
    campaign_id = 0
    req = et_service_pb2.RegisterCampaign.Request(
        userId=user_id,
        sessionKey=session_key,
        campaignId=campaign_id,
        name="Scalability Test",
        notes="A campaign for testing the scalability of the system.",
        configJson="{}",
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

    # Bind DATA and LOG data sources
    req = et_service_pb2.BindDataSource.Request(
        userId=user_id,
        sessionKey=session_key,
        name="DATA",
        iconName="miscellaneous-data-sources.png",
    )
    res = stub.bindDataSource(request=req)
    assert res.success
    print(f"Bound DATA data source (dataSourceId={res.dataSourceId})")

    req = et_service_pb2.BindDataSource.Request(
        userId=user_id,
        sessionKey=session_key,
        name="LOG",
        iconName="miscellaneous-data-sources.png",
    )
    res = stub.bindDataSource(request=req)
    assert res.success
    print(f"Bound LOG data source (dataSourceId={res.dataSourceId})")

    # Create and bind 2048 users as campaign participants (id=1, 2, ..., 1024)
    for i in tqdm(range(1, 2049), desc="Creating participants", leave=False):
        # Try to register new participant
        username = (
            f"participant{i}"  # e.g. participant1, participant2, ..., participant1024
        )
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
