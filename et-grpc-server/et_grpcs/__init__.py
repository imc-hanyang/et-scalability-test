"""
ET gRPC package.

This package provides the generated gRPC service definitions
for the EasyTrack platform.

Modules:
    et_service_pb2: Protocol buffer message definitions.
    et_service_pb2_grpc: gRPC service definitions.
"""
from . import et_service_pb2, et_service_pb2_grpc

__all__ = ["et_service_pb2", "et_service_pb2_grpc"]
