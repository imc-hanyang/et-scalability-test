import time

from django.core.files.uploadedfile import TemporaryUploadedFile
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from dotenv import load_dotenv

from . import db_mgr

load_dotenv()
db_mgr.parse_envs()


@csrf_exempt
@require_http_methods(["POST"])
def upload_file(request):
    # parse request data
    t0 = int(time.time() * 1000)
    user_id = int(request.POST["user_id"])
    timestamp = int(request.POST["timestamp"])
    files: [TemporaryUploadedFile] = []
    for filename in request.FILES.keys():
        files.append(request.FILES[filename])

    # read data from files
    timestamps, values_arr = [], []
    values_arr = []
    for i, file in enumerate(files):
        timestamps.append(timestamp + i)
        values_arr.append(file.read())
    t1 = int(time.time() * 1000)

    # save data to database: Cassandra
    cassandra_session = db_mgr.get_cassandra_session()
    db_mgr.save_data_cassandra(
        cassandra_session=cassandra_session,
        user_id=user_id,
        timestamps_arr=timestamps,
        values_arr=values_arr,
    )
    t2 = int(time.time() * 1000)

    # return response
    return JsonResponse(
        {
            "status": "success",
            "request_parsing_time": f"{t1 - t0:,} ms",
            "cassandra_write_time": f"{t2 - t1:,} ms",
            "total_time": f"{t2 - t0:,} ms",
        }
    )
