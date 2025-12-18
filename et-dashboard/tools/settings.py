"""
Configuration settings for the EasyTrack Dashboard tools package.

This module provides global configuration variables and constants used
throughout the application for database connections and file paths.
"""

import os
import tempfile
from typing import Optional

# Cassandra configuration
CQLSH_PATH: str = "/root/apache-cassandra-4.0-rc1/bin/cqlsh"
DOWNLOAD_DIR: str = os.path.join(tempfile.gettempdir(), "easytrack_dashboard")

# Project paths
SETTINGS_DIR: str = os.path.dirname(__file__)
PROJECT_ROOT: str = os.path.abspath(os.path.dirname(SETTINGS_DIR))
STATIC_DIR: str = os.path.join(PROJECT_ROOT, "static/")

# Backward compatibility aliases (deprecated - use UPPER_CASE versions)
cqlsh_path = CQLSH_PATH
download_dir = DOWNLOAD_DIR
settings_dir = SETTINGS_DIR

# Database connection singletons (initialized at runtime)
db_conn: Optional[object] = None
cassandra_cluster: Optional[object] = None
cassandra_session: Optional[object] = None

