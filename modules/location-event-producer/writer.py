import grpc
import location_pb2
import location_pb2_grpc

print("Sending sample payload...")

channel = grpc.insecure_channel("localhost:5005")

stub = location_pb2_grpc.locationServiceStub(channel)

# Update this with desired payload
location = location_pb2.LocationMessage(
    userId=1,
    latitude=-100,
    longitude=30
)

location2 = location_pb2.LocationMessage(
    userId=5,
    latitude=-100,
    longitude=30
)
response1 = stub.Create(location)
response2 = stub.Create(location2)
print("Location sent....")
print(location, location2)