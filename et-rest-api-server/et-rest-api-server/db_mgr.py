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
    # execution_profile = ExecutionProfile(
    #     request_timeout=600,  # seconds
    #     consistency_level=ConsistencyLevel.ONE,  # write consistency level: only 1 node needs to acknowledge
    #     load_balancing_policy=RoundRobinPolicy(),
    # )

    # initialize a connection to cassandra cluster
    cassandra_cluster = Cluster(
        contact_points=cassandra_contact_points,
        executor_threads=256,  # number of threads to handle requests
        connect_timeout=10,  # seconds
        # ssl_context=ssl_context,
        # execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
        auth_provider=PlainTextAuthProvider(
            username=os.environ["CASSANDRA_ADMIN_USER"],
            password=os.environ["CASSANDRA_ADMIN_PASSWORD"],
        ),
    )

    cassandra_session = cassandra_cluster.connect()
    cassandra_session.default_timeout = 600  # seconds


def save_data_cassandra(
    user_id: int,
    timestamps_arr: [int],
    values_arr: [bytes],
):
    global cassandra_session
    insert_stmt = cassandra_session.prepare(
        f"INSERT INTO et.data (user_id, timestamp, value) VALUES (?, ?, ?)"
    )
    cur_size = 0
    cur_batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    for timestamp, value in zip(timestamps_arr, values_arr):
        cur_batch.add(insert_stmt, (user_id, timestamp, value))
        cur_size += len(value)
        if cur_size > 25 * 1024 * 1024:
            cassandra_session.execute(cur_batch)
            print(f"Saved {len(cur_batch)} batch data points to Cassandra")
            cur_batch.clear()
            cur_size = 0
    if cur_size > 0:
        cassandra_session.execute(cur_batch)
        print(f"Saved {len(cur_batch)} batch data points to Cassandra (last batch)")
