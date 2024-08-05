import json
import time

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from dotenv import load_dotenv

from . import db_mgr

load_dotenv()
db_mgr.parse_envs()
db_mgr.init_connection()


@csrf_exempt
@require_http_methods(["POST"])
def upload_file(request):
    # parse request
    user_id = int(request.POST["user_id"])
    values = []
    for file in request.FILES.keys():
        values.append(request.FILES[file].read())

    # save data in database
    t0 = int(time.time() * 1000)
    db_mgr.save_data(
        user_id=user_id,
        values=values,
    )
    t1 = int(time.time() * 1000)

    # return response
    return JsonResponse(
        {
            "status": "success",
            "db_write_time": f"{t1 - t0:,} ms",
        }
    )
