import datetime
import json
import mimetypes
import os
import re
import zipfile
from json import JSONDecodeError
from wsgiref.util import FileWrapper

import plotly
import plotly.graph_objects as go
from django.contrib.auth import authenticate as dj_authenticate
from django.contrib.auth import login as dj_login
from django.contrib.auth import logout as dj_logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User as dj_User
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.shortcuts import redirect, render
from django.views.decorators.csrf import csrf_exempt
# Django
from django.views.decorators.http import require_http_methods

# EasyTrack
from ET_Dashboard.models import EnhancedDataSource
from tools import db_mgr as db
from tools import settings, utils


def handle_google_verification(request):
    return render(request=request, template_name="google43e44b3701ba10c8.html")


@require_http_methods(["GET", "POST"])
def handle_login_api(request):
    if request.user.is_authenticated:
        db_user = db.get_user(email=request.user.email)
        if db_user is None:
            print("new user : ", end="")
            session_key = utils.md5(value=f"{request.user.email}{utils.now_us()}")
            db_user = db.create_user(
                name=request.user.get_full_name(),
                email=request.user.email,
                session_key=session_key,
            )
            if db_user is None:
                dj_logout(request=request)
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    return render(
        request=request,
        template_name="page_authentication.html",
        context={"title": "Authentication"},
    )


@require_http_methods(["GET"])
def handle_development_login_api(request):
    dev_email = "dev@easytrack.com"
    db_user = db.get_user(
        email=request.user.email if request.user.is_authenticated else dev_email
    )
    if db_user is None:
        print("new user : ", end="")
        session_key = utils.md5(value=f"{dev_email}{utils.now_us()}")
        db_user = db.create_user(
            name=dev_email, email=dev_email, session_key=session_key
        )
        if db_user is None:
            dj_logout(request=request)
        else:
            if dj_User.objects.filter(email=dev_email).exists():
                dj_user = dj_User.objects.get(email=dev_email)
            else:
                dj_user = dj_User.objects.create_user(
                    username=dev_email, email=dev_email, password=dev_email
                )
            if dj_authenticate(username=dev_email, password=dev_email):
                dj_login(
                    request=request,
                    user=dj_user,
                    backend="django.contrib.auth.backends.ModelBackend",
                )
                return redirect(to="campaigns-list")
            else:
                return redirect(to="login")
    else:
        if dj_User.objects.filter(email=dev_email).exists():
            dj_user = dj_User.objects.get(email=dev_email)
        else:
            dj_User.objects.create_user()
            dj_user = dj_User.objects.create_user(
                username=dev_email, email=dev_email, password=dev_email
            )
            dj_user.first_name = "ET"
            dj_user.last_name = "Development"
            dj_user.save()
        if dj_authenticate(username=dev_email, password=dev_email):
            dj_login(
                request=request,
                user=dj_user,
                backend="django.contrib.auth.backends.ModelBackend",
            )
            return redirect(to="campaigns-list")
        else:
            return redirect(to="login")


@login_required
@require_http_methods(["GET", "POST"])
def handle_logout_api(request):
    dj_logout(request=request)
    return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_campaigns_list(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        my_campaigns = []
        for db_campaign in db.get_campaigns(db_creator_user=db_user):
            my_campaigns += [
                {
                    "id": db_campaign.id,
                    "name": db_campaign.name,
                    "notes": db_campaign.notes,
                    "created_by_me": True,
                    "participants": db.get_campaign_participants_count(
                        db_campaign=db_campaign
                    ),
                }
            ]
        for db_campaign in db.get_researcher_campaigns(db_researcher_user=db_user):
            my_campaigns += [
                {
                    "id": db_campaign.id,
                    "name": db_campaign.name,
                    "notes": db_campaign.notes,
                    "created_by_me": False,
                    "participants": db.get_campaign_participants_count(
                        db_campaign=db_campaign
                    ),
                }
            ]
        print("%s opened the main page" % request.user.email)
        my_campaigns.sort(key=lambda x: x["id"])
        return render(
            request=request,
            template_name="page_campaigns.html",
            context={
                "title": "%s's campaigns" % request.user.email,
                "my_campaigns": my_campaigns,
            },
        )
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_participants_list(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "id" in request.GET and str(request.GET["id"]).isdigit():
            db_campaign = db.get_campaign(
                campaign_id=int(request.GET["id"]), db_researcher_user=db_user
            )
            if db_campaign is not None:
                # campaign dashboard page
                participants = []
                for participant in db.get_campaign_participants(
                    db_campaign=db_campaign
                ):
                    participants += [
                        {
                            "id": participant.id,
                            "name": participant.name,
                            "email": participant.email,
                            "day_no": utils.calculate_day_number(
                                join_timestamp=db.get_participant_join_timestamp(
                                    db_user=participant, db_campaign=db_campaign
                                )
                            ),
                            "amount_of_data": db.get_participants_amount_of_data(
                                db_user=participant, db_campaign=db_campaign
                            ),
                            "last_heartbeat_time": utils.timestamp_to_readable_string(
                                timestamp_ms=db.get_participant_heartbeat_timestamp(
                                    db_user=participant, db_campaign=db_campaign
                                )
                            ),
                            "last_sync_time": utils.timestamp_to_readable_string(
                                timestamp_ms=db.get_participant_last_sync_timestamp(
                                    db_user=participant, db_campaign=db_campaign
                                )
                            ),
                        }
                    ]
                participants.sort(key=lambda x: x["id"])
                return render(
                    request=request,
                    template_name="page_campaign_participants.html",
                    context={
                        "title": "%s's participants" % db_campaign.name,
                        "campaign": db_campaign,
                        "participants": participants,
                    },
                )
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_researchers_list(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and str(request.GET["campaign_id"]).isdigit():
            db_campaign = db.get_campaign(
                campaign_id=int(request.GET["campaign_id"]), db_researcher_user=db_user
            )
            if db_campaign is not None:
                if (
                    "targetEmail" in request.GET
                    and "action" in request.GET
                    and request.GET["action"] in ["add", "remove"]
                ):
                    db_researcher_user = db.get_user(email=request.GET["targetEmail"])
                    if db_researcher_user is not None:
                        if request.GET["action"] == "add":
                            db.add_researcher_to_campaign(
                                db_campaign=db_campaign,
                                db_researcher_user=db_researcher_user,
                            )
                        elif request.GET["action"] == "remove":
                            db.remove_researcher_from_campaign(
                                db_campaign=db_campaign,
                                db_researcher_user=db_researcher_user,
                            )
                        else:
                            return redirect(to="campaigns-list")
                    else:
                        return redirect(to="campaigns-list")

                # list of researchers
                researchers = []
                for db_researcher_user in db.get_campaign_researchers(
                    db_campaign=db_campaign
                ):
                    researchers += [
                        {
                            "name": db_researcher_user.name,
                            "email": db_researcher_user.email,
                        }
                    ]
                researchers.sort(key=lambda x: x["name"])
                return render(
                    request=request,
                    template_name="page_campaign_researchers.html",
                    context={
                        "title": "%s's researchers" % db_campaign.name,
                        "campaign": db_campaign,
                        "researchers": researchers,
                    },
                )
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_participants_data_list(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and str(request.GET["campaign_id"]).isdigit():
            db_campaign = db.get_campaign(
                campaign_id=int(request.GET["campaign_id"]), db_researcher_user=db_user
            )
            if db_campaign is not None:
                campaign_data_source_configs = {}
                for data_source in json.loads(s=db_campaign.configJson):
                    campaign_data_source_configs[data_source["data_source_id"]] = (
                        data_source["config_json"]
                    )
                if "participant_id" in request.GET and utils.is_numeric(
                    request.GET["participant_id"]
                ):
                    db_participant_user = db.get_user(
                        user_id=int(request.GET["participant_id"])
                    )
                    if (
                        db_participant_user is not None
                        and db.user_is_bound_to_campaign(
                            db_user=db_participant_user, db_campaign=db_campaign
                        )
                    ):
                        data_sources = []
                        for (
                            db_data_source,
                            amount_of_data,
                            last_sync_time,
                        ) in db.get_participants_per_data_source_stats(
                            db_user=db_participant_user, db_campaign=db_campaign
                        ):
                            data_sources += [
                                {
                                    "id": db_data_source.id,
                                    "name": db_data_source.name,
                                    "icon_name": db_data_source.iconName,
                                    "config_json": campaign_data_source_configs[
                                        db_data_source.id
                                    ],
                                    "amount_of_data": amount_of_data,
                                    "last_sync_time": utils.timestamp_to_readable_string(
                                        timestamp_ms=last_sync_time
                                    ),
                                }
                            ]
                        data_sources.sort(key=lambda x: x["name"])
                        return render(
                            request=request,
                            template_name="page_participant_data_sources_stats.html",
                            context={
                                "title": f"Data submitted by {db_participant_user.name}, {db_participant_user.email} (ID = {db_participant_user.id})",
                                "campaign": db_campaign,
                                "participant": db_participant_user,
                                "data_sources": data_sources,
                            },
                        )
                    else:
                        return redirect(to="campaigns-list")
                else:
                    return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_raw_samples_list(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and str(request.GET["campaign_id"]).isdigit():
            db_campaign = db.get_campaign(
                campaign_id=int(request.GET["campaign_id"]), db_researcher_user=db_user
            )
            if db_campaign is not None:
                if "email" in request.GET:
                    db_participant_user = db.get_user(email=request.GET["email"])
                    if (
                        db_participant_user is not None
                        and db.user_is_bound_to_campaign(
                            db_user=db_participant_user, db_campaign=db_campaign
                        )
                    ):
                        if (
                            "from_timestamp" in request.GET
                            and "data_source_id" in request.GET
                            and utils.is_numeric(request.GET["from_timestamp"])
                            and utils.is_numeric(request.GET["data_source_id"])
                        ):
                            from_timestamp = int(request.GET["from_timestamp"])
                            db_data_source = db.get_data_source(
                                data_source_id=int(request.GET["data_source_id"])
                            )
                            if db_data_source is not None:
                                records = []
                                for row, record in enumerate(
                                    db.get_next_k_data_records(
                                        db_user=db_participant_user,
                                        db_campaign=db_campaign,
                                        db_data_source=db_data_source,
                                        from_timestamp=from_timestamp,
                                        k=500,
                                    )
                                ):
                                    value_len = len(record.value)
                                    if value_len > 5 * 1024:  # 5KB (e.g., binary files)
                                        value = f"[ {value_len:,} byte data record ]"
                                    else:
                                        try:
                                            value = str(record.value, encoding="utf-8")
                                        except UnicodeDecodeError:
                                            value = (
                                                f"[ {value_len:,} byte data record ]"
                                            )
                                    records += [
                                        {
                                            "row": row + 1,
                                            "timestamp": utils.timestamp_to_readable_string(
                                                timestamp_ms=record.timestamp
                                            ),
                                            "value": value,
                                        }
                                    ]
                                    from_timestamp = record.timestamp
                                return render(
                                    request=request,
                                    template_name="page_raw_data_view.html",
                                    context={
                                        "title": db_data_source.name,
                                        "records": records,
                                        "from_timestamp": from_timestamp,
                                    },
                                )
                            else:
                                return redirect(to="campaigns-list")
                        else:
                            return redirect(to="campaigns-list")
                    else:
                        return redirect(to="campaigns-list")
                else:
                    return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET", "POST"])
def handle_campaign_editor(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if request.method == "GET":
            # request to open the campaign editor
            db_data_sources = db.get_all_data_sources()
            if (
                "edit" in request.GET
                and "campaign_id" in request.GET
                and str(request.GET["campaign_id"]).isdigit()
            ):
                # edit an existing campaign
                db_campaign = db.get_campaign(
                    campaign_id=int(request.GET["campaign_id"]),
                    db_researcher_user=db_user,
                )
                if db_campaign is not None:
                    campaign_db_data_sources = []
                    campaign_data_source_configs = {}
                    for campaign_data_source in json.loads(s=db_campaign.configJson):
                        db_data_source = db.get_data_source(
                            data_source_id=campaign_data_source["data_source_id"]
                        )
                        campaign_db_data_sources += [db_data_source]
                        campaign_data_source_configs[db_data_source.id] = (
                            campaign_data_source["config_json"]
                        )
                    campaign_data_sources = []
                    for db_data_source in db_data_sources:
                        selected = db_data_source in campaign_db_data_sources
                        campaign_data_sources += [
                            {
                                "name": db_data_source.name,
                                "icon_name": db_data_source.iconName,
                                "selected": selected,
                                "config_json": (
                                    campaign_data_source_configs[db_data_source.id]
                                    if selected
                                    else None
                                ),
                            }
                        ]
                    campaign_data_sources.sort(key=lambda key: key["name"])
                    return render(
                        request=request,
                        template_name="page_campaign_editor.html",
                        context={
                            "edit_mode": True,
                            "title": '"%s" Campaign Editor' % db_campaign.name,
                            "campaign": db_campaign,
                            "campaign_start_time": utils.timestamp_to_web_string(
                                timestamp_ms=db_campaign.startTimestamp
                            ),
                            "campaign_end_time": utils.timestamp_to_web_string(
                                timestamp_ms=db_campaign.endTimestamp
                            ),
                            "data_sources": campaign_data_sources,
                        },
                    )
                else:
                    return redirect(to="campaigns-list")
            else:
                # edit for a new campaign
                campaign_data_sources = []
                for db_data_source in db_data_sources:
                    campaign_data_sources += [
                        {
                            "name": db_data_source.name,
                            "icon_name": db_data_source.iconName,
                        }
                    ]
                campaign_data_sources.sort(key=lambda key: key["name"])
                return render(
                    request=request,
                    template_name="page_campaign_editor.html",
                    context={
                        "title": "New campaign",
                        "data_sources": campaign_data_sources,
                    },
                )
        elif request.method == "POST":

            def prepare_campaign_params():
                def is_date_time(string):
                    return (
                        re.search(
                            pattern=r"^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}$",
                            string=string,
                        )
                        is not None
                    )

                def get_campaign_data_sources_as_list():
                    # parse data sources from POST request
                    _campaign_data_sources = []
                    for _data_source in request.POST:
                        if str(_data_source).startswith("config_json_"):
                            _data_source_name = str(_data_source)[12:]
                            if (
                                "icon_name_%s" % _data_source_name in request.POST
                                and "config_json_%s" % _data_source_name in request.POST
                            ):
                                _data_source_icon_name = request.POST[
                                    "icon_name_%s" % _data_source_name
                                ]
                                if len(_data_source_icon_name) > 0:
                                    try:  # validate JSON format
                                        _data_source_config_json = json.loads(
                                            s=request.POST[
                                                "config_json_%s" % _data_source_name
                                            ]
                                        )
                                        _campaign_data_sources += [
                                            {
                                                "name": _data_source_name,
                                                "icon_name": _data_source_icon_name,
                                                "config_json": _data_source_config_json,
                                            }
                                        ]
                                    except JSONDecodeError:
                                        return None

                    # bind the data sources and attach data source ids
                    for _index, _data_source in enumerate(_campaign_data_sources):
                        _db_data_source = db.get_data_source(
                            data_source_name=_data_source["name"]
                        )
                        if _db_data_source is None:  # create a new data source
                            _db_data_source = db.create_data_source(
                                db_creator_user=db_user,
                                name=_data_source["name"],
                                icon_name=_data_source["icon_name"],
                            )
                        _campaign_data_sources[_index][
                            "data_source_id"
                        ] = _db_data_source.id
                    return _campaign_data_sources

                if (
                    "name" in request.POST
                    and "notes" in request.POST
                    and "startTime" in request.POST
                    and "endTime"
                    and "remove_inactive_users_timeout" in request.POST
                ):
                    _campaign_name = str(request.POST["name"])
                    if (
                        utils.is_numeric(request.POST["remove_inactive_users_timeout"])
                        and is_date_time(request.POST["startTime"])
                        and is_date_time(request.POST["endTime"])
                    ):
                        return {
                            "name": _campaign_name,
                            "notes": str(request.POST["notes"]),
                            "configurations": json.dumps(
                                get_campaign_data_sources_as_list()
                            ),
                            "start_timestamp": utils.datetime_to_timestamp_ms(
                                value=datetime.datetime.strptime(
                                    request.POST["startTime"], "%Y-%m-%dT%H:%M"
                                )
                            ),
                            "end_timestamp": utils.datetime_to_timestamp_ms(
                                value=datetime.datetime.strptime(
                                    request.POST["endTime"], "%Y-%m-%dT%H:%M"
                                )
                            ),
                            "remove_inactive_users_timeout": (
                                int(request.POST["remove_inactive_users_timeout"])
                                if int(request.POST["remove_inactive_users_timeout"])
                                > 0
                                else -1
                            ),
                        }
                    else:
                        return None
                else:
                    return None

            if "campaign_id" in request.POST and utils.is_numeric(
                request.POST["campaign_id"]
            ):
                db_campaign = db.get_campaign(
                    campaign_id=int(request.POST["campaign_id"])
                )
                if db_campaign is None:
                    # request to create a new campaign
                    campaign_params = prepare_campaign_params()
                    if campaign_params is not None:
                        db.create_or_update_campaign(
                            db_creator_user=db_user,
                            name=campaign_params["name"],
                            notes=campaign_params["notes"],
                            configurations=campaign_params["configurations"],
                            start_timestamp=campaign_params["start_timestamp"],
                            end_timestamp=campaign_params["end_timestamp"],
                        )
                        return redirect(to="campaigns-list")
                    else:
                        return redirect(to="campaigns-list")
                elif db_campaign.creatorId == db_user.id:
                    # request to edit an existing campaign
                    campaign_params = prepare_campaign_params()
                    if campaign_params is not None:
                        db.create_or_update_campaign(
                            db_creator_user=db_user,
                            name=campaign_params["name"],
                            notes=campaign_params["notes"],
                            configurations=campaign_params["configurations"],
                            start_timestamp=campaign_params["start_timestamp"],
                            end_timestamp=campaign_params["end_timestamp"],
                            db_campaign=db_campaign,
                        )
                        return redirect(to="campaigns-list")
                    else:
                        return redirect(to="campaigns-list")
                else:
                    return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_easytrack_monitor(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and utils.is_numeric(
            request.GET["campaign_id"]
        ):
            db_campaign = db.get_campaign(campaign_id=int(request.GET["campaign_id"]))
            db_campaign_data_sources = db.get_campaign_data_sources(
                db_campaign=db_campaign
            )
            db_campaign_participant_users = db.get_campaign_participants(
                db_campaign=db_campaign
            )
            if db_campaign is not None:
                from_datetime = datetime.datetime.now().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                till_datetime = from_datetime + datetime.timedelta(hours=24)

                if "plot_date" in request.GET:
                    plot_date_str = str(request.GET["plot_date"])
                    if re.search(r"\d{4}-\d{1,2}-\d{1,2}", plot_date_str) is not None:
                        year, month, day = plot_date_str.split("-")
                        from_datetime = datetime.datetime(
                            year=int(year),
                            month=int(month),
                            day=int(day),
                            hour=0,
                            minute=0,
                            second=0,
                            microsecond=0,
                        )
                        till_datetime = from_datetime + datetime.timedelta(hours=24)

                till_timestamp = utils.datetime_to_timestamp_ms(till_datetime)
                from_timestamp = utils.datetime_to_timestamp_ms(from_datetime)
                window = 3600000  # 1 hour jump

                if "participant_id" in request.GET and utils.is_numeric(
                    request.GET["participant_id"]
                ):
                    db_participant_user = db.get_user(
                        user_id=int(request.GET["participant_id"])
                    )
                    if (
                        db_participant_user is not None
                        and db.user_is_bound_to_campaign(
                            db_user=db_participant_user, db_campaign=db_campaign
                        )
                    ):
                        plot_participant = db_participant_user
                    else:
                        plot_participant = None
                else:
                    plot_participant = None

                if "data_source_name" in request.GET:
                    data_source_name = request.GET["data_source_name"]
                    db_data_source = db.get_data_source(
                        data_source_name=data_source_name
                    )
                    if data_source_name == "all":
                        hourly_stats = {}
                        # region compute hourly stats
                        for db_participant_user in (
                            db_campaign_participant_users
                            if plot_participant is None
                            else [plot_participant]
                        ):
                            for db_data_source in db_campaign_data_sources:
                                _from_timestamp = from_timestamp
                                _till_timestamp = _from_timestamp + window
                                while _from_timestamp < till_timestamp:
                                    hour = utils.get_timestamp_hour(
                                        timestamp_ms=_from_timestamp
                                    )
                                    amount = db.get_filtered_amount_of_data(
                                        db_campaign=db_campaign,
                                        db_user=db_participant_user,
                                        from_timestamp=_from_timestamp,
                                        till_timestamp=_till_timestamp,
                                        db_data_source=db_data_source,
                                    )
                                    if hour in hourly_stats:
                                        hourly_stats[hour] += amount
                                    else:
                                        hourly_stats[hour] = amount
                                    _from_timestamp += window
                                    _till_timestamp += window
                        # endregion

                        plot_data_source = {
                            "name": "all campaign data sources combined"
                        }
                        # region plot hourly stats
                        x = []
                        y = []
                        max_amount = 10
                        hours = list(hourly_stats.keys())
                        hours.sort()
                        for hour in hours:
                            amount = hourly_stats[hour]
                            if hour < 13:
                                hour = f'{hour} {"pm" if hour == 12 else "am"}'
                            else:
                                hour = f"{hour % 12} pm"
                            x += [hour]
                            y += [amount]
                            max_amount = max(max_amount, amount)
                        fig = go.Figure([go.Bar(x=x, y=y)])
                        fig.update_yaxes(range=[0, max_amount])
                        plot_str = plotly.offline.plot(
                            fig, auto_open=False, output_type="div"
                        )
                        plot_data_source["plot"] = plot_str
                        # endregion

                        return render(
                            request=request,
                            template_name="easytrack_monitor.html",
                            context={
                                "title": "EasyTracker",
                                "campaign": db_campaign,
                                "plot_date": f"{from_datetime.year}-{from_datetime.month:02}-{from_datetime.day:02}",
                                "participants": db_campaign_participant_users,
                                "plot_participant": plot_participant,
                                "all_data_sources": db_campaign_data_sources,
                                "plot_data_source": plot_data_source,
                            },
                        )
                    elif db_data_source is not None:
                        hourly_stats = {}
                        # region compute hourly stats
                        for db_participant_user in (
                            db_campaign_participant_users
                            if plot_participant.id == "all"
                            else [plot_participant]
                        ):
                            _from_timestamp = from_timestamp
                            _till_timestamp = _from_timestamp + window
                            while _from_timestamp < till_timestamp:
                                hour = utils.get_timestamp_hour(
                                    timestamp_ms=_from_timestamp
                                )
                                amount = db.get_filtered_amount_of_data(
                                    db_campaign=db_campaign,
                                    db_user=db_participant_user,
                                    from_timestamp=_from_timestamp,
                                    till_timestamp=_till_timestamp,
                                    db_data_source=db_data_source,
                                )
                                if hour in hourly_stats:
                                    hourly_stats[hour] += amount
                                else:
                                    hourly_stats[hour] = amount
                                _from_timestamp += window
                                _till_timestamp += window
                        # endregion

                        plot_data_source = EnhancedDataSource(
                            db_data_source=db_data_source
                        )
                        # region plot hourly stats
                        x = []
                        y = []
                        max_amount = 10
                        hours = list(hourly_stats.keys())
                        hours.sort()
                        for hour in hours:
                            amount = hourly_stats[hour]
                            if hour < 13:
                                hour = f'{hour} {"pm" if hour == 12 else "am"}'
                            else:
                                hour = f"{hour % 12} pm"
                            x += [hour]
                            y += [amount]
                            max_amount = max(max_amount, amount)
                        fig = go.Figure([go.Bar(x=x, y=y)])
                        fig.update_yaxes(range=[0, max_amount])
                        plot_str = plotly.offline.plot(
                            fig, auto_open=False, output_type="div"
                        )
                        plot_data_source.attach_plot(plot_str=plot_str)
                        # endregion

                        return render(
                            request=request,
                            template_name="easytrack_monitor.html",
                            context={
                                "title": "EasyTracker",
                                "campaign": db_campaign,
                                "plot_date": f"{from_datetime.year}-{from_datetime.month:02}-{from_datetime.day:02}",
                                "participants": db_campaign_participant_users,
                                "plot_participant": plot_participant,
                                "all_data_sources": db_campaign_data_sources,
                                "plot_data_source": plot_data_source,
                            },
                        )
                    else:
                        return redirect(to="campaigns-list")
                else:
                    return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_dataset_info(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and utils.is_numeric(
            request.GET["campaign_id"]
        ):
            db_campaign = db.get_campaign(campaign_id=int(request.GET["campaign_id"]))
            if db_campaign is not None:
                campaign_data_sources = list(json.loads(s=db_campaign.configJson))
                campaign_data_sources.sort(key=lambda x: x["name"])
                db_participants = list(
                    db.get_campaign_participants(db_campaign=db_campaign)
                )
                db_participants.sort(key=lambda db_participant: db_participant.id)
                return render(
                    request=request,
                    template_name="page_dataset_configs.html",
                    context={
                        "campaign": db_campaign,
                        "data_sources": campaign_data_sources,
                        "participants": db_participants,
                    },
                )
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_delete_campaign_api(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and utils.is_numeric(
            request.GET["campaign_id"]
        ):
            db_campaign = db.get_campaign(campaign_id=int(request.GET["campaign_id"]))
            if db_campaign is not None:
                db.delete_campaign(db_campaign=db_campaign)
                return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_download_data_api(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and utils.is_numeric(
            request.GET["campaign_id"]
        ):
            db_campaign = db.get_campaign(campaign_id=int(request.GET["campaign_id"]))
            if db_campaign is not None:
                if "participant_id" in request.GET and utils.is_numeric(
                    request.GET["participant_id"]
                ):
                    db_participant_user = db.get_user(
                        user_id=int(request.GET["participant_id"])
                    )
                    if db_participant_user is not None:
                        # dump db data
                        dump_file_path = db.dump_data(
                            db_campaign=db_campaign, db_user=db_participant_user
                        )
                        print(f"dump path : {dump_file_path}")
                        with open(dump_file_path, "rb") as r:
                            dump_content = bytes(r.read())
                        os.remove(dump_file_path)

                        # archive the dump content
                        now = datetime.datetime.now()
                        file_name = f"et data {db_participant_user.email} {now.month}-{now.day}-{now.year} {now.hour}-{now.minute}.zip"
                        file_path = utils.get_download_file_path(file_name=file_name)
                        fp = zipfile.ZipFile(file_path, "w", zipfile.ZIP_STORED)
                        with open(
                            os.path.join(
                                settings.STATIC_DIR, "restoring_cassandra_data.txt"
                            ),
                            "r",
                        ) as r:
                            fp.writestr("!README.txt", r.read())
                        fp.writestr(f"{db_participant_user.email}.csv", dump_content)
                        fp.close()
                        with open(file_path, "rb") as r:
                            content = r.read()
                        os.remove(file_path)

                        res = HttpResponse(
                            content=content, content_type="application/x-binary"
                        )
                        res["Content-Disposition"] = f"attachment; filename={file_name}"
                        return res
                    else:
                        return redirect(to="campaigns-list")
                else:
                    return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_download_csv_api(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and utils.is_numeric(
            request.GET["campaign_id"]
        ):
            db_campaign = db.get_campaign(campaign_id=int(request.GET["campaign_id"]))
            if db_campaign is not None:
                if "user_id" in request.GET and utils.is_numeric(
                    request.GET["user_id"]
                ):
                    db_participant_user = db.get_user(
                        user_id=int(request.GET["user_id"])
                    )
                    if db_participant_user is not None:
                        dump_file_path = db.dump_data(
                            db_campaign=db_campaign, db_user=db_participant_user
                        )
                    else:
                        return redirect(to="campaigns-list")
                elif "data_source_id" in request.GET and utils.is_numeric(
                    request.GET["data_source_id"]
                ):
                    db_data_source = db.get_data_source(
                        data_source_id=int(request.GET["data_source_id"])
                    )
                    if db_data_source is not None:
                        dump_file_path = db.dump_data(
                            db_campaign=db_campaign, db_data_source=db_data_source
                        )
                    else:
                        return redirect(to="campaigns-list")
                else:
                    dump_file_path = db.dump_data(db_campaign=db_campaign)

                filename = os.path.basename(dump_file_path)
                chunk_size = 8192
                res = StreamingHttpResponse(
                    streaming_content=FileWrapper(
                        open(dump_file_path, "rb"), chunk_size
                    ),
                    content_type=mimetypes.guess_type(dump_file_path)[0],
                )
                res["Content-Length"] = os.path.getsize(dump_file_path)
                res["Content-Disposition"] = f"attachment; filename={filename}"
                return res
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_download_dataset_api(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if "campaign_id" in request.GET and utils.is_numeric(
            request.GET["campaign_id"]
        ):
            db_campaign = db.get_campaign(campaign_id=int(request.GET["campaign_id"]))
            if db_campaign is not None:
                # archive
                now = datetime.datetime.now()
                file_name = f"et data campaign {db_campaign.id} {now.month}-{now.day}-{now.year} {now.hour}-{now.minute}.zip"
                file_path = utils.get_download_file_path(file_name=file_name)
                fp = zipfile.ZipFile(file_path, "w", zipfile.ZIP_STORED)
                with open(
                    os.path.join(settings.STATIC_DIR, "restoring_cassandra_data.txt"),
                    "r",
                ) as r:
                    fp.writestr("!README.txt", r.read())

                for db_participant_user in db.get_campaign_participants(
                    db_campaign=db_campaign
                ):
                    # dump db data
                    dump_file_path = db.dump_data(
                        db_campaign=db_campaign, db_user=db_participant_user
                    )
                    with open(dump_file_path, "rb") as r:
                        dump_content = bytes(r.read())
                    os.remove(dump_file_path)
                    # archive the dump content
                    fp.writestr(f"{db_participant_user.email}.csv", dump_content)
                fp.close()
                with open(file_path, "rb") as r:
                    content = r.read()
                os.remove(file_path)

                res = HttpResponse(content=content, content_type="application/x-binary")
                res["Content-Disposition"] = f"attachment; filename={file_name}"
                return res
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@csrf_exempt
@require_http_methods(["POST"])
def handle_db_mgmt_api(request):
    import psycopg2
    from psycopg2 import extras as psycopg2_extras

    conn = psycopg2.connect(
        host="127.0.0.1", database="easytrack_db", user="postgres", password="postgres"
    )
    cur = conn.cursor(cursor_factory=psycopg2_extras.DictCursor)
    session = db.get_cassandra_session()

    # 1. copy campaign
    cur.execute('select * from "et"."campaign" where "id"=4;')
    pg_campaign = cur.fetchone()
    # cs_creator_user = db.get_user(user_id=2)
    # cs_campaign = db.create_or_update_campaign(
    #     db_creator_user=cs_creator_user,
    #     name=pg_campaign['name'],
    #     notes=pg_campaign['notes'],
    #     configurations=pg_campaign['config_json'],
    #     start_timestamp=pg_campaign['start_timestamp'],
    #     end_timestamp=pg_campaign['end_timestamp']
    # )
    cs_campaign = db.get_campaign(campaign_id=1)
    print("1. campaign copied")

    # prepare data source map
    cur.execute('select * from "et"."data_source";')
    pg_data_source_ids = {}
    for pg_data_source in cur.fetchall():
        pg_data_source_ids[pg_data_source["name"]] = pg_data_source["id"]
    data_source_id_map = {}
    for cs_data_source in db.get_all_data_sources():
        data_source_id_map[pg_data_source_ids[cs_data_source.name]] = cs_data_source.id
    print("   data sources prepared")

    # 2. copy participants and data
    cur.execute(
        'select * from "stats"."campaign_participant_stats" where "campaign_id"=4;'
    )
    pg_stats = cur.fetchall()
    cs_data_sources = {}
    for pg_stat in pg_stats:
        # 2.1. copy participant
        cur.execute('select * from "et"."user" where "id"=%s;', (pg_stat["user_id"],))
        pg_participant = cur.fetchone()
        cs_participant = db.get_user(
            email=pg_participant["email"]
        )  # db.create_user(name=pg_participant['name'], email=pg_participant['email'], session_key=utils.md5(value=f'{pg_participant["email"]}{utils.now_us()}'))
        print(f"   participant copied (name = {cs_participant.name})")
        # 2.2. copy data
        # db.bind_participant_to_campaign(db_user=cs_participant, db_campaign=cs_campaign)
        cur.execute(
            f'select * from "data"."{pg_campaign["id"]}-{pg_participant["id"]}";'
        )
        for pg_value in cur.fetchall():
            cs_data_source_id = data_source_id_map[pg_value["data_source_id"]]
            if cs_data_source_id not in cs_data_sources:
                cs_data_sources[cs_data_source_id] = db.get_data_source(
                    data_source_id=cs_data_source_id
                )
            db.store_data_record(
                db_user=cs_participant,
                db_campaign=cs_campaign,
                db_data_source=cs_data_sources[cs_data_source_id],
                timestamp=pg_value["timestamp"],
                value=bytes(pg_value["value"]),
            )
        print(f"   user data copied (name = {cs_participant.name})")

    print("done")
    cur.close()
    conn.close()
    return JsonResponse(data={"result": "success"})


@login_required
@require_http_methods(["GET", "POST"])
def make_announcement(request):
    db_user = db.get_user(email=request.user.email)
    if db_user is not None:
        if request.method == "GET":
            if "campaign_id" in request.GET and utils.is_numeric(
                request.GET["campaign_id"]
            ):
                db_campaign = db.get_campaign(
                    campaign_id=int(request.GET["campaign_id"])
                )
                if db_campaign is not None:
                    return render(
                        request=request,
                        template_name="page_announcement_creator.html",
                        context={
                            "title": "Create announcement / notification",
                            "campaign": db_campaign,
                        },
                    )
                else:
                    return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        elif request.method == "POST":
            if "campaign_id" in request.POST and utils.is_numeric(
                request.POST["campaign_id"]
            ):
                db_campaign = db.get_campaign(
                    campaign_id=int(request.POST["campaign_id"])
                )
                if db_campaign is not None:
                    db.create_notification(
                        db_campaign=db_campaign,
                        timestamp=utils.get_timestamp_ms(),
                        subject=request.POST["subject"],
                        content=request.POST["content"],
                    )
                    return redirect(to="campaigns-list")
                else:
                    return redirect(to="campaigns-list")
            else:
                return redirect(to="campaigns-list")
        else:
            return redirect(to="campaigns-list")
    else:
        dj_logout(request=request)
        return redirect(to="login")


@login_required
@require_http_methods(["GET"])
def handle_notifications_list(request):
    return None


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_total_ema_score(request):
    if not utils.param_check(
        request.POST,
        ["campaign_id", "email", "data_source_id", "from_timestamp", "till_timestamp"],
    ):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source = db.get_data_source(
        data_source_id=int(request.POST["data_source_id"])
    )
    from_ts = int(request.POST["from_timestamp"])
    till_ts = int(request.POST["till_timestamp"])

    if None in [db_campaign, db_participant, db_data_source, from_ts, till_ts]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    res = {"success": True, "EMA": {}}
    for ema in db.get_filtered_data_records(
        db_campaign=db_campaign,
        from_timestamp=from_ts,
        till_timestamp=till_ts,
        db_user=db_participant,
        db_data_source=db_data_source,
    ):
        cells = str(bytes(ema.value), encoding="utf8").split(" ")
        res["EMA"][int(cells[0])] = {"sum": sum(int(x) for x in cells[2:-1])}

    return JsonResponse(data=res)


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_hr(request):
    if not utils.param_check(
        request.POST,
        ["campaign_id", "email", "data_source_id", "from_timestamp", "till_timestamp"],
    ):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source = db.get_data_source(
        data_source_id=int(request.POST["data_source_id"])
    )
    from_ts = int(request.POST["from_timestamp"])
    till_ts = int(request.POST["till_timestamp"])

    if None in [db_campaign, db_participant, db_data_source, from_ts, till_ts]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    hrs = []
    for hr in db.get_filtered_data_records(
        db_campaign=db_campaign,
        from_timestamp=from_ts,
        till_timestamp=till_ts,
        db_user=db_participant,
        db_data_source=db_data_source,
    ):
        cells = str(bytes(hr.value), encoding="utf8").split(" ")
        hrs += [int(cells[1])]
    res = {"success": True, "hr": "n/a" if len(hrs) == 0 else sum(hrs) / len(hrs)}

    return JsonResponse(data=res)


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_sleep(request):
    if not utils.param_check(
        request.POST,
        ["campaign_id", "email", "data_source_id", "from_timestamp", "till_timestamp"],
    ):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source = db.get_data_source(
        data_source_id=int(request.POST["data_source_id"])
    )
    from_ts = int(request.POST["from_timestamp"])
    till_ts = int(request.POST["till_timestamp"])

    if None in [db_campaign, db_participant, db_data_source, from_ts, till_ts]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    res = {"success": True, "sleep": {}}
    for sleep_record in db.get_filtered_data_records(
        db_campaign=db_campaign,
        from_timestamp=from_ts,
        till_timestamp=till_ts,
        db_user=db_participant,
        db_data_source=db_data_source,
    ):
        cells = str(bytes(sleep_record.value), encoding="utf8").split(" ")
        res["sleep"][int(cells[1])] = int(cells[2])

    return JsonResponse(data=res)


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_user_info(request):
    if not utils.param_check(request.POST, ["campaign_id", "email", "data_source_id"]):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source = db.get_data_source(
        data_source_id=int(request.POST["data_source_id"])
    )

    if None in [db_campaign, db_participant, db_data_source]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    res = {"success": True, "user_info": {}}
    for user_info_record in db.get_filtered_data_records(
        db_campaign=db_campaign, db_user=db_participant, db_data_source=db_data_source
    ):
        cells = str(bytes(user_info_record.value), encoding="utf8").split(" ")
        res["user_info"][cells[2]] = cells[1]

    return JsonResponse(data=res)


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_steps(request):
    if not utils.param_check(
        request.POST,
        ["campaign_id", "email", "data_source_id", "from_timestamp", "till_timestamp"],
    ):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source = db.get_data_source(
        data_source_id=int(request.POST["data_source_id"])
    )
    from_ts = int(request.POST["from_timestamp"])
    till_ts = int(request.POST["till_timestamp"])

    if None in [db_campaign, db_participant, db_data_source, from_ts, till_ts]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    res = {
        "success": True,
        "amount": db.get_filtered_amount_of_data(
            db_campaign=db_campaign,
            from_timestamp=from_ts,
            till_timestamp=till_ts,
            db_user=db_participant,
            db_data_source=db_data_source,
        ),
    }
    return JsonResponse(data=res)


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_total_reward(request):
    if not utils.param_check(request.POST, ["campaign_id", "email", "data_source_id"]):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source = db.get_data_source(
        data_source_id=int(request.POST["data_source_id"])
    )

    if None in [db_campaign, db_participant, db_data_source]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    reward_sum = 0
    for reward_record in db.get_filtered_data_records(
        db_campaign=db_campaign, db_user=db_participant, db_data_source=db_data_source
    ):
        cells = str(reward_record.value, encoding="utf8").split(" ")
        reward_sum += int(cells[1])

    res = {"success": True, "reward": reward_sum}
    return JsonResponse(data=res)


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_ema_resp_rate(request):
    if not utils.param_check(
        request.POST,
        [
            "campaign_id",
            "email",
            "data_source_id_1",
            "data_source_id_2",
            "from_timestamp",
            "till_timestamp",
        ],
    ):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source_1 = db.get_data_source(
        data_source_id=int(request.POST["data_source_id_1"])
    )
    db_data_source_2 = db.get_data_source(
        data_source_id=int(request.POST["data_source_id_2"])
    )
    from_ts = int(request.POST["from_timestamp"])
    till_ts = int(request.POST["till_timestamp"])

    if None in [
        db_campaign,
        db_participant,
        db_data_source_1,
        db_data_source_2,
        from_ts,
        till_ts,
    ]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    amount_1 = db.get_filtered_amount_of_data(
        db_campaign=db_campaign,
        from_timestamp=from_ts,
        till_timestamp=till_ts,
        db_user=db_participant,
        db_data_source=db_data_source_1,
    )
    amount_2 = 0
    data = db.get_filtered_data_records(
        db_user=db_participant,
        db_campaign=db_campaign,
        db_data_source=db_data_source_2,
        from_timestamp=from_ts,
        till_timestamp=till_ts,
    )
    if data and len(data) > 0:
        cells = str(data[0]["value"]).split(" ")
        if len(cells) == 2:
            amount_2 = cells[1]

    return JsonResponse(
        data={
            "success": True,
            "ema_rate": (
                "N/A (division by zero)"
                if amount_2 == 0
                else round((amount_1 / amount_2) * 100)
            ),
        }
    )


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_participant_stats(request):
    if not utils.param_check(request.POST, ["campaign_id", "email"]):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])

    if None in [db_campaign, db_participant]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    stats = db.get_participants_per_data_source_stats(
        db_user=db_participant, db_campaign=db_campaign
    )
    amount_of_samples = {}
    sync_timestamps = {}
    for _db_data_source, _amount_of_samples, _sync_timestamp in stats:
        amount_of_samples[_db_data_source["name"]] = _amount_of_samples
        sync_timestamps[_db_data_source["name"]] = _sync_timestamp

    return JsonResponse(
        data={
            "success": True,
            "participation_days": utils.calculate_day_number(
                join_timestamp=db.get_participant_join_timestamp(
                    db_user=db_participant, db_campaign=db_campaign
                )
            ),
            "total_amount": sum([amount_of_samples[x] for x in amount_of_samples]),
            "last_sync_ts": max([sync_timestamps[x] for x in sync_timestamps]),
            "per_data_source_amount": {
                x: amount_of_samples[x] for x in amount_of_samples
            },
            "per_data_source_last_sync_ts": {
                x: sync_timestamps[x] for x in sync_timestamps
            },
        }
    )


@csrf_exempt
@require_http_methods(["POST"])
def huno_json_lottery_winners(request):
    if not utils.param_check(
        request.POST,
        ["campaign_id", "email", "data_source_id", "from_timestamp", "till_timestamp"],
    ):
        return JsonResponse(
            data={"success": False, "err_msg": "huno, check your param types"}
        )

    db_campaign = db.get_campaign(campaign_id=int(request.POST["campaign_id"]))
    db_participant = db.get_user(email=request.POST["email"])
    db_data_source = db.get_data_source(
        data_source_id=int(request.POST["data_source_id"])
    )
    from_ts = int(request.POST["from_timestamp"])
    till_ts = int(request.POST["till_timestamp"])

    if None in [db_campaign, db_participant]:
        return JsonResponse(
            data={
                "success": False,
                "err_msg": "huno, values for some params are invalid, pls recheck",
            }
        )

    res = {"success": True, "winners": {}}
    for winner_records in db.get_filtered_data_records(
        db_user=db_participant,
        db_campaign=db_campaign,
        db_data_source=db_data_source,
        from_timestamp=from_ts,
        till_timestamp=till_ts,
    ):
        cells = str(winner_records.value, encoding="utf8").split(" ")
        res["winners"][int(cells[0])] = {
            "id": int(cells[1]),
            "name": cells[2],
            "phone_number": cells[3],
            "email": cells[4],
        }

    return JsonResponse(data=res)
