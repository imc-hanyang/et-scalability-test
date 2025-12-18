"""
Application settings and configuration constants.

This module contains global configuration settings, directory paths,
and shared state for the EasyTrack gRPC server.
"""
import os
import tempfile
from typing import Any, Optional, Set, List

# ==============================================================================
# Directory Settings
# ==============================================================================

#: Directory for temporary download files
download_dir: str = os.path.join(tempfile.gettempdir(), "easytrack_grpc_server")

#: Directory containing this settings file
settings_dir: str = os.path.dirname(__file__)

#: Root directory of the project
PROJECT_ROOT: str = os.path.abspath(os.path.dirname(settings_dir))

#: Directory for static files
STATIC_DIR: str = os.path.join(PROJECT_ROOT, "static/")

# ==============================================================================
# Database Settings
# ==============================================================================

#: Cassandra cluster connection object
cassandra_cluster: Optional[Any] = None

#: Cassandra session object
cassandra_session: Optional[Any] = None

# ==============================================================================
# Server Settings
# ==============================================================================

#: Minimum length for username validation
MIN_USERNAME_LENGTH: int = 4

#: Minimum length for password validation
MIN_PASSWORD_LENGTH: int = 4

#: Maximum number of gRPC server worker threads
MAX_GRPC_WORKERS: int = 1000

#: Maximum message size for gRPC (2GB - 1 byte)
MAX_MESSAGE_LENGTH: int = 2147483647

#: Maximum number of data records to retrieve in a single request
MAX_K_RECORDS: int = 500

#: gRPC keepalive time in milliseconds (15 minutes)
GRPC_KEEPALIVE_TIME_MS: int = 900000

#: gRPC keepalive timeout in milliseconds (5 seconds)
GRPC_KEEPALIVE_TIMEOUT_MS: int = 5000

#: Minimum time between gRPC pings in milliseconds
GRPC_MIN_PING_INTERVAL_MS: int = 5000

# ==============================================================================
# Threading Settings
# ==============================================================================

#: Number of insert worker threads
num_of_insert_threads: int = 5

#: Job queues for each insert thread
thr_jobs: List[Set[Any]] = [set() for _ in range(num_of_insert_threads)]

# ==============================================================================
# Legacy Database Connection (deprecated)
# ==============================================================================

#: Legacy database connection object (deprecated, use cassandra_session instead)
db_conn: Optional[Any] = None
