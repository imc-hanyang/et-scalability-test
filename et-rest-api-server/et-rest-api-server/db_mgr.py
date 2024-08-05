from __future__ import annotations

import os

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement

cassandra_contact_points: [str] = []
cassandra_cluster: Cluster | None = None
cassandra_session: Session | None = None


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


def init_connection():
    global cassandra_cluster, cassandra_session

    # prepare ssl context
    # ssl_context = ssl.create_default_context()
    # ssl_context.check_hostname = False
    # ssl_context.verify_mode = ssl.CERT_NONE

    # execution profile (fast writes, ignore read)
    execution_profile = ExecutionProfile(
        request_timeout=600,  # seconds
        load_balancing_policy=RoundRobinPolicy(),
    )

    # initialize a connection to cassandra cluster
    cassandra_cluster = Cluster(
        contact_points=cassandra_contact_points,
        executor_threads=256,  # number of threads to handle requests
        connect_timeout=10,  # seconds
        # ssl_context=ssl_context,
        execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
        auth_provider=PlainTextAuthProvider(
            username=os.environ["CASSANDRA_ADMIN_USER"],
            password=os.environ["CASSANDRA_ADMIN_PASSWORD"],
        ),
    )

    cassandra_session = cassandra_cluster.connect()
    # cassandra_session.default_timeout = 600  # seconds


def save_data(
    user_id: int,
    values: [bytes],
):
    global cassandra_session

    # prepare insert statement
    insert_stmt = cassandra_session.prepare(
        f"INSERT INTO et.data (user_id, value) VALUES (?, ?)"
    )

    # save data points in batch (3 data points per batch)
    batch_stmt = BatchStatement(consistency_level=ConsistencyLevel.ANY)
    for i in range(len(values)):
        batch_stmt.add(insert_stmt, (user_id, values[i]))
        if i % 3 == 0 or i == len(values) - 1:
            cassandra_session.execute(batch_stmt)
            batch_stmt.clear()
