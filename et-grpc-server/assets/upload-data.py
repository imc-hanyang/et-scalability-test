import time

import grpc

from et_grpcs import et_service_pb2, et_service_pb2_grpc


def bind(ip_address):
    channel = grpc.insecure_channel(f"{ip_address}:50051")
    stub = et_service_pb2_grpc.ETServiceStub(channel)

    req = et_service_pb2.BindUserToCampaign.Request(
        userId=66,
        email="nslabinha@gmail.com",
        campaignId=9,
    )
    res = stub.bindUserToCampaign(request=req)
    print(f"success = {res.success}")

    channel.close()


def submit(ip_address):
    channel = grpc.insecure_channel(f"{ip_address}:50051")
    stub = et_service_pb2_grpc.ETServiceStub(channel)

    req = et_service_pb2.SubmitDataRecords.Request(
        userId=66,
        email="nslabinha@gmail.com",
        campaignId=9,
    )
    req.timestamp.extend([1610124788000, 1610114788001])
    req.dataSource.extend([3, 3])
    req.accuracy.extend([1.0, 1.0])
    with open("C:/Users/Kevin/Downloads/data.png", "rb") as r:
        data1 = r.read()
        data2 = data1
        req.value.extend([data1, data2])

    res = stub.submitDataRecords(request=req)
    print(f"success = {res.success}")

    channel.close()


def fetch(ip_address):
    channel = grpc.insecure_channel(f"{ip_address}:50051")
    stub = et_service_pb2_grpc.ETServiceStub(channel)

    req = et_service_pb2.RetrieveKNextDataRecords.Request(
        userId=66,
        email="nslabinha@gmail.com",
        targetEmail="nslabinha@gmail.com",
        targetCampaignId=9,
        targetDataSourceId=3,
        k=1,
    )
    res = stub.retrieveKNextDataRecords(request=req)
    print(f"success = {res.success}")

    if res.success:
        with open("C:/Users/Kevin/Downloads/data_downloaded.png", "wb") as w:
            w.write(res.value[0])
        print("file saved!")

    channel.close()


def upload_speed_test(ip_address):
    def prepare_request(request_size_bytes=2048):
        with open("C:/Users/Kevin/Downloads/data.png", "rb") as r:
            image = r.read()
        req = et_service_pb2.SubmitDataRecords.Request(
            userId=66,
            email="nslabinha@gmail.com",
            campaignId=8,
        )
        size = 4 + len(req.email) + 4
        while size < request_size_bytes:
            req.timestamp.extend([0])
            req.dataSource.extend([3])
            req.accuracy.extend([1.0])
            req.value.extend([image])
            size += 12 + len(image)

        return req, size

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = et_service_pb2_grpc.ETServiceStub(channel)

    start_time = time.time()
    timestamp_ms = int(start_time * 1000)
    req, bulk_size = prepare_request(request_size_bytes=1024 * 1024)

    cumulative_size = 0
    delta_time = time.time() - start_time
    while delta_time < 10:
        for i in range(0, len(req.dataSource)):
            req.timestamp[i] = timestamp_ms + i
        timestamp_ms += len(req.dataSource)

        res = stub.submitDataRecords(request=req)
        if res.success:
            cumulative_size += bulk_size
            print(f"{delta_time:.1f} seconds - {cumulative_size:,} bytes")
        delta_time = time.time() - start_time

    print(f"{delta_time} seconds - {cumulative_size} bytes [finished]")
    channel.close()


def upload_tsv_file(ip_address, filename):
    channel = grpc.insecure_channel(f"{ip_address}:50051")
    stub = et_service_pb2_grpc.ETServiceStub(channel)

    with open(filename, "r", encoding="utf8") as r:
        req = et_service_pb2.SubmitDataRecords.Request(
            userId=85,
            email="kjrnet23@gmail.com",
            campaignId=4,
        )
        for line in r:
            _, _data_source_id, _timestamp, _accuracy, _value = line[:-1].split("\t")
            req.timestamp.extend([int(_timestamp)])
            req.dataSource.extend([int(_data_source_id)])
            req.accuracy.extend([float(_accuracy)])
            req.value.extend([bytes(_value, encoding="utf8")])

        res = stub.submitDataRecords(request=req)
        print(f"success = {res.success}")

    channel.close()


def main():
    ip_address = "165.246.42.173"
    # bind(ip_address=ip_address)
    # submit(ip_address=ip_address)
    # fetch(ip_address=ip_address)
    # upload_speed_test(ip_address=ip_address)
    upload_tsv_file(ip_address=ip_address, filename="C://Users/Kevin/Downloads/85.tsv")


if __name__ == "__main__":
    main()
