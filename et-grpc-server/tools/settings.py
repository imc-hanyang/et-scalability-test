import os
import tempfile

download_dir = os.path.join(tempfile.gettempdir(), "easytrack_grpc_server")
settings_dir = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.dirname(settings_dir))
STATIC_DIR = os.path.join(PROJECT_ROOT, "static/")
db_conn = None
cassandra_cluster = None
cassandra_session = None
num_of_insert_threads = 5
thr_jobs = [set() for _ in range(num_of_insert_threads)]
