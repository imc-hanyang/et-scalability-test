from __future__ import annotations

import os

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile,
                               Session)
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement

cassandra_contact_points: [str] = []


def parse_envs():
    # cassandra contact points
    global cassandra_contact_points
    tmp = os.environ["CASSANDRA_IP_ADDRESSES"].split(",")
    i = 0
    while i < len(tmp):
        tmp[i] = tmp[i].strip()
        if not tmp[i]:
            del tmp[i]
        else:
            i += 1
    cassandra_contact_points.clear()
    cassandra_contact_points.extend(tmp)
    print(f"Cassandra contact points: {tmp}")


def get_cassandra_session() -> Session:
    # prepare ssl context
    # ssl_context = ssl.create_default_context()
    # ssl_context.check_hostname = False
    # ssl_context.verify_mode = ssl.CERT_NONE

    # execution profile (fast writes, ignore read)
    execution_profile = ExecutionProfile(
        request_timeout=600,  # seconds
        consistency_level=ConsistencyLevel.ONE,  # write consistency level: only 1 node needs to acknowledge
        load_balancing_policy=RoundRobinPolicy(),
    )

    # initialize a connection to cassandra cluster
    cluster = Cluster(
        contact_points=cassandra_contact_points,
        executor_threads=4,  # number of threads to handle requests
        connect_timeout=10,  # seconds
        # ssl_context=ssl_context,
        execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
        auth_provider=PlainTextAuthProvider(
            username=os.environ["CASSANDRA_ADMIN_USER"],
            password=os.environ["CASSANDRA_ADMIN_PASSWORD"],
        ),
    )

    return cluster.connect()


def save_data_cassandra(
    cassandra_session: Session,
    user_id: int,
    timestamps_arr: [int],
    values_arr: [bytes],
):
    insert_stmt = cassandra_session.prepare(
        f"INSERT INTO et.data (user_id, timestamp, value) VALUES (?, ?, ?)"
    )
    batch_stmt = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    for timestamp, value in zip(timestamps_arr, values_arr):
        batch_stmt.add(insert_stmt, (user_id, timestamp, value))
    cassandra_session.execute(batch_stmt)
