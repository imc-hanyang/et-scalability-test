import grpc

from et_grpcs import et_service_pb2, et_service_pb2_grpc

channel = grpc.insecure_channel("165.246.21.202:50051")
stub = et_service_pb2_grpc.ETServiceStub(channel)
req = et_service_pb2.RetrieveParticipantStats.Request(
    userId=435,
    sessionKey="1234",
    targetCampaignId=8,
    targetEmail="participant@example.com",
)
res = stub.retrieveParticipantStats(req)
print(res)

# req = et_service_pb2.BindUserToCampaign.Request(
#     userId=435,
#     sessionKey='1234',
#     campaignId=8
# )
# res = stub.bindUserToCampaign(req)
# print(res)
#
# with open('C:\\Users\\Kevin\\Downloads\\snu.csv') as r:
#     for line in r:
#         cells = line[:-1].split(',')
#         ts, val = int(cells[0]), ','.join(cells[1:])
#         req = et_service_pb2.SubmitDataRecord.Request(
#             userId=435,
#             sessionKey='1234',
#             campaignId=8,
#             timestamp=ts,
#             dataSource=49,
#             accuracy=0.0,
#             value=val.encode('utf8')
#         )
#         res = stub.submitDataRecord(req)
#         print(res.success)

channel.close()
