from __future__ import annotations

import os

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

cassandra_cluster: Cluster | None = None
cassandra_session: Session | None = None


def init_connection():
    # if already initialized, return
    global cassandra_cluster, cassandra_session
    if cassandra_cluster is not None:
        return

    # resolve contact points (IP addresses of cassandra nodes / seeds)
    contact_points = os.environ["CASSANDRA_IP_ADDRESSES"].split(",")
    i = 0
    while i < len(contact_points):
        contact_points[i] = contact_points[i].strip()
        if not contact_points[i]:
            del contact_points[i]
        else:
            i += 1

    # prepare ssl context
    # ssl_context = ssl.create_default_context()
    # ssl_context.check_hostname = False
    # ssl_context.verify_mode = ssl.CERT_NONE

    # initialize cassandra session
    cassandra_cluster = Cluster(
        contact_points=contact_points,
        executor_threads=2048,
        connect_timeout=1200,
        # ssl_context=ssl_context,
        auth_provider=PlainTextAuthProvider(
            username=os.environ["CASSANDRA_ADMIN_USER"],
            password=os.environ["CASSANDRA_ADMIN_PASSWORD"],
        ),
    )
    cassandra_session = cassandra_cluster.connect()
    cassandra_session.default_timeout = 1200


def generate_users(n: int):
    # create keyspace (if not exists)
    global cassandra_session
    cassandra_session.execute(
        "CREATE KEYSPACE IF NOT EXISTS et WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )

    # create 'user' table (if not exists)
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS et.user (id int PRIMARY KEY)")

    # insert 'n' users (+ their data tables)
    for i in range(1, n + 1):
        # create user record
        cassandra_session.execute(f"INSERT INTO et.user (id) VALUES (%s)", (i,))
        # create user data table
        cassandra_session.execute(
            f"CREATE TABLE IF NOT EXISTS et.user_{i}_data (timestamp bigint PRIMARY KEY, data blob)"
        )

    print(f"Generated {n} users with their data tables")


def save_data(
    user_id: int,
    timestamp: int,
    data: bytes,
):
    global cassandra_session
    cassandra_session.execute(
        f"INSERT INTO et.user_{user_id}_data (timestamp, data) VALUES (%s, %s)",
        (timestamp, data),
    )
