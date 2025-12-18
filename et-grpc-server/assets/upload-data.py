"""
Data Upload Utility Script.

This script provides utility functions for testing data upload functionality
to the EasyTrack gRPC server, including binding users to campaigns,
submitting data records, and performing upload speed tests.
"""
import logging
import time
from typing import Any

import grpc

from et_grpcs import et_service_pb2, et_service_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_PORT: int = 50051
DEFAULT_USER_ID: int = 66
DEFAULT_CAMPAIGN_ID: int = 9
DEFAULT_DATA_SOURCE_ID: int = 3


def create_stub(ip_address: str, port: int = DEFAULT_PORT) -> tuple:
    """
    Create a gRPC channel and stub.

    Args:
        ip_address: Server IP address.
        port: Server port. Defaults to DEFAULT_PORT.

    Returns:
        Tuple of (channel, stub).
    """
    channel = grpc.insecure_channel(f"{ip_address}:{port}")
    stub = et_service_pb2_grpc.ETServiceStub(channel)
    return channel, stub


def bind(ip_address: str) -> None:
    """
    Bind a user to a campaign.

    Args:
        ip_address: Server IP address.
    """
    channel, stub = create_stub(ip_address)

    try:
        req = et_service_pb2.BindUserToCampaign.Request(
            userId=DEFAULT_USER_ID,
            email="nslabinha@gmail.com",
            campaignId=DEFAULT_CAMPAIGN_ID,
        )
        res = stub.bindUserToCampaign(request=req)
        logger.info("Bind result: success = %s", res.success)
    finally:
        channel.close()


def submit(ip_address: str, image_path: str) -> None:
    """
    Submit data records to the server.

    Args:
        ip_address: Server IP address.
        image_path: Path to image file to upload.
    """
    channel, stub = create_stub(ip_address)

    try:
        req = et_service_pb2.SubmitDataRecords.Request(
            userId=DEFAULT_USER_ID,
            email="nslabinha@gmail.com",
            campaignId=DEFAULT_CAMPAIGN_ID,
        )
        req.timestamp.extend([1610124788000, 1610114788001])
        req.dataSource.extend([DEFAULT_DATA_SOURCE_ID, DEFAULT_DATA_SOURCE_ID])
        req.accuracy.extend([1.0, 1.0])

        with open(image_path, "rb") as image_file:
            data = image_file.read()
            req.value.extend([data, data])

        res = stub.submitDataRecords(request=req)
        logger.info("Submit result: success = %s", res.success)
    finally:
        channel.close()


def fetch(ip_address: str, output_path: str) -> None:
    """
    Fetch data records from the server.

    Args:
        ip_address: Server IP address.
        output_path: Path to save the fetched data.
    """
    channel, stub = create_stub(ip_address)

    try:
        req = et_service_pb2.RetrieveKNextDataRecords.Request(
            userId=DEFAULT_USER_ID,
            email="nslabinha@gmail.com",
            targetEmail="nslabinha@gmail.com",
            targetCampaignId=DEFAULT_CAMPAIGN_ID,
            targetDataSourceId=DEFAULT_DATA_SOURCE_ID,
            k=1,
        )
        res = stub.retrieveKNextDataRecords(request=req)
        logger.info("Fetch result: success = %s", res.success)

        if res.success and res.value:
            with open(output_path, "wb") as output_file:
                output_file.write(res.value[0])
            logger.info("File saved to: %s", output_path)
    finally:
        channel.close()


def upload_speed_test(
    ip_address: str,
    image_path: str,
    request_size_bytes: int = 1024 * 1024,
    duration_seconds: int = 10,
) -> None:
    """
    Perform an upload speed test.

    Args:
        ip_address: Server IP address.
        image_path: Path to image file to use for testing.
        request_size_bytes: Target size for each request in bytes.
        duration_seconds: How long to run the test in seconds.
    """

    def prepare_request(target_size: int) -> tuple:
        """Prepare a request of approximately the target size."""
        with open(image_path, "rb") as image_file:
            image = image_file.read()

        req = et_service_pb2.SubmitDataRecords.Request(
            userId=DEFAULT_USER_ID,
            email="nslabinha@gmail.com",
            campaignId=8,  # Different campaign for testing
        )

        size = 4 + len(req.email) + 4  # Base size
        while size < target_size:
            req.timestamp.extend([0])
            req.dataSource.extend([DEFAULT_DATA_SOURCE_ID])
            req.accuracy.extend([1.0])
            req.value.extend([image])
            size += 12 + len(image)

        return req, size

    channel, stub = create_stub("127.0.0.1")  # Use localhost for speed test

    try:
        start_time = time.time()
        timestamp_ms = int(start_time * 1000)
        req, bulk_size = prepare_request(request_size_bytes)

        cumulative_size = 0
        delta_time = time.time() - start_time

        while delta_time < duration_seconds:
            # Update timestamps
            for i in range(len(req.dataSource)):
                req.timestamp[i] = timestamp_ms + i
            timestamp_ms += len(req.dataSource)

            res = stub.submitDataRecords(request=req)
            if res.success:
                cumulative_size += bulk_size
                logger.info("%.1f seconds - %s bytes", delta_time, f"{cumulative_size:,}")

            delta_time = time.time() - start_time

        logger.info(
            "%.1f seconds - %d bytes [finished]",
            delta_time,
            cumulative_size,
        )
    finally:
        channel.close()


def upload_tsv_file(ip_address: str, filename: str) -> None:
    """
    Upload data from a TSV file.

    Args:
        ip_address: Server IP address.
        filename: Path to the TSV file to upload.
    """
    channel, stub = create_stub(ip_address)

    try:
        with open(filename, "r", encoding="utf8") as tsv_file:
            req = et_service_pb2.SubmitDataRecords.Request(
                userId=85,
                email="kjrnet23@gmail.com",
                campaignId=4,
            )

            for line in tsv_file:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 5:
                    _, data_source_id, timestamp, accuracy, value = parts[:5]
                    req.timestamp.extend([int(timestamp)])
                    req.dataSource.extend([int(data_source_id)])
                    req.accuracy.extend([float(accuracy)])
                    req.value.extend([bytes(value, encoding="utf8")])

            res = stub.submitDataRecords(request=req)
            logger.info("Upload TSV result: success = %s", res.success)
    finally:
        channel.close()


def main() -> None:
    """
    Main entry point for the upload utility.

    Uncomment the desired function call to run.
    """
    ip_address = "165.246.42.173"

    # Example usage - uncomment as needed:
    # bind(ip_address=ip_address)
    # submit(ip_address=ip_address, image_path="path/to/image.png")
    # fetch(ip_address=ip_address, output_path="path/to/output.png")
    # upload_speed_test(ip_address=ip_address, image_path="path/to/image.png")
    # upload_tsv_file(ip_address=ip_address, filename="path/to/data.tsv")

    logger.info(
        "Upload utility ready. Uncomment desired function calls in main() to run."
    )


if __name__ == "__main__":
    main()
