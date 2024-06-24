"""ET_Dashboard URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import include, path

from ET_Dashboard import views

urlpatterns = [
    # authentication
    path("login/", views.handle_login_api, name="login"),
    path("logout/", views.handle_logout_api, name="logout"),
    path("dev_login/", views.handle_development_login_api, name="dev-login"),
    # dashboard navigation
    path("", views.handle_campaigns_list, name="campaigns-list"),
    path("campaign/", views.handle_participants_list, name="participants-list"),
    path("participant/", views.handle_participants_data_list, name="participant"),
    path("data/", views.handle_raw_samples_list, name="view_data"),
    path("edit/", views.handle_campaign_editor, name="campaign-editor"),
    path("notifications/", views.handle_notifications_list, name="notifications"),
    path("researchers/", views.handle_researchers_list, name="manage-researchers"),
    # API (e.g., download file)
    path("dataset-info/", views.handle_dataset_info, name="dataset-info"),
    path(
        "download-dataset/", views.handle_download_dataset_api, name="download-dataset"
    ),
    path("announce/", views.make_announcement, name="announce"),
    path("delete/", views.handle_delete_campaign_api, name="delete-campaign"),
    path("download-data/", views.handle_download_data_api, name="download-data"),
    path("download-csv/", views.handle_download_csv_api, name="download-csv"),
    path("dbmgmt/", views.handle_db_mgmt_api, name="db-mgmt"),
    # visuals (e.g., DQ)
    path("et-monitor/", views.handle_easytrack_monitor, name="easytrack-monitor"),
    # others
    path("admin/", admin.site.urls),
    path("google43e44b3701ba10c8.html", views.handle_google_verification),
    path("google-auth/", include("social_django.urls", namespace="social")),
    # APIs for huno
    path("huno_json_total_ema_score/", views.huno_json_total_ema_score),
    path("huno_json_hr/", views.huno_json_hr),
    path("huno_json_sleep/", views.huno_json_sleep),
    path("huno_json_user_info/", views.huno_json_user_info),
    path("huno_json_steps/", views.huno_json_steps),
    path("huno_json_total_reward/", views.huno_json_total_reward),
    path("huno_json_ema_resp_rate/", views.huno_json_ema_resp_rate),
    path("huno_json_participant_stats/", views.huno_json_participant_stats),
    path("huno_json_lottery_winners/", views.huno_json_lottery_winners),
]
