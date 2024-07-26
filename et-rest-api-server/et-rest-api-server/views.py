import time

from django.core.files.uploadedfile import TemporaryUploadedFile
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from . import db_mgr

db_mgr.init_connection()


@csrf_exempt
@require_http_methods(["POST"])
def upload_file(request):
    # parse request data
    user_id = int(request.POST["user_id"])
    timestamp = int(request.POST["timestamp"])
    files: [TemporaryUploadedFile] = []
    for filename in request.FILES.keys():
        files.append(request.FILES[filename])

    # save files to database
    for file in files:
        file_data = file.read()
        db_mgr.save_data(
            user_id=user_id,
            timestamp=timestamp,
            data=file_data,
        )
        del file_data

    # return response
    return JsonResponse({"status": "success"})
