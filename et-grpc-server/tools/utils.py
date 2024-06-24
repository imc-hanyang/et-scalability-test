import base64
import datetime
import hashlib
import os
import pickle
import time
from email.mime.text import MIMEText

from google.auth.transport import requests as oauth_requests
from google.auth.transport.requests import Request
from google.oauth2 import id_token as oauth_id_token
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import errors
from googleapiclient.discovery import build

from tools import db_mgr as db
from tools import settings


def get_credentials():
    credentials = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "static/google_api_credentials.json",
                ["https://www.googleapis.com/auth/gmail.send"],
            )
            credentials = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as token:
            pickle.dump(credentials, token)
    return credentials


def get_timestamp_ms():
    return int(round(time.time() * 1000))


def send_email(user_id, message):
    """Send an email message.

    Args:
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.

    Returns:
      Sent Message.
    """
    try:
        service = build(
            serviceName="gmail", version="v1", credentials=get_credentials()
        )
        message = (
            service.users().messages().send(userId=user_id, body=message).execute()
        )
        print("Message Id: %s" % message["id"])
        service.close()
    except errors.HttpError as error:
        print("An error occurred: %s" % error)


def create_message(sender, to, subject, message_text):
    """Create a message for an email.

    Args:
      sender: Email address of the sender.
      to: Email address of the receiver.
      subject: The subject of the email message.
      message_text: The text of the email message.

    Returns:
      An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text, "html")
    message["to"] = to
    message["from"] = sender
    message["subject"] = subject
    return {
        "raw": base64.urlsafe_b64encode(
            message.as_string().encode(encoding="utf8")
        ).decode(encoding="utf8")
    }


def get_problematic_users_email_message(
    destination_email_addresses, db_campaign, db_participants
):
    def warn_html_row(
        _id, email, name, day_no, amount_of_data, last_heartbeat_time, last_sync_time
    ):
        return f"""<tr>
                    <td class="id_column">{_id}</td>
                    <td class="name_column" title="{email}">{name}</td>
                    <td class="duration_column">{day_no} days</td>
                    <td class="amount_column">{amount_of_data} samples</td>
                    <td class="heartbeat_column">{last_heartbeat_time}</td>
                    <td class="sync_time_column">{last_sync_time}</td>
                </tr>"""

    def calculate_day_number(join_timestamp):
        then = datetime.datetime.fromtimestamp(float(join_timestamp) / 1000).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        then += datetime.timedelta(days=1)

        now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        now += datetime.timedelta(days=1)

        return (now - then).days

    def timestamp_to_readable_string(timestamp_ms):
        if timestamp_ms == 0:
            return "N/A"
        else:
            return datetime.datetime.fromtimestamp(float(timestamp_ms) / 1000).strftime(
                "%m/%d (%a), %I:%M %p"
            )

    rows = []
    for db_user in db_participants:
        rows += [
            warn_html_row(
                _id=db_user["id"],
                email=db_user["email"],
                name=db_user["name"],
                day_no=calculate_day_number(
                    join_timestamp=db.get_participant_join_timestamp(
                        db_user=db_user, db_campaign=db_campaign
                    )
                ),
                amount_of_data=f"{db.get_participants_amount_of_data(db_user=db_user, db_campaign=db_campaign):,} samples",
                last_heartbeat_time=timestamp_to_readable_string(
                    timestamp_ms=db.get_participant_heartbeat_timestamp(
                        db_user=db_user, db_campaign=db_campaign
                    )
                ),
                last_sync_time=timestamp_to_readable_string(
                    timestamp_ms=db.get_participant_last_sync_timestamp(
                        db_user=db_user, db_campaign=db_campaign
                    )
                ),
            )
        ]
    with open("static/problematic_users_email_template.html", "r") as r:
        a = r.read()
        message_text = a.replace(
            "campaign_html_element",
            f'<h3>Campaign : <a href="http://etdb.myvnc.com/campaign/?id={db_campaign["id"]}" style="text-decoration: none;">{db_campaign["name"]}</a></h3>',
        )
        message_text = message_text.replace("table_rows", ",".join(rows))

    return [
        create_message(
            sender="easytracknoreply@gmail.com",
            to=to,
            subject="[ET warning] problematic participants detected",
            message_text=message_text,
        )
        for to in destination_email_addresses
    ]


def get_missing_ema_email_messages(
    destination_email_addresses, db_campaign, warn_users
):
    def warn_html_row(
        _id, email, name, data_source_name, amount_of_emas, _last_submission
    ):
        return f"""<tr>
                    <td>{_id}</td>
                    <td title="{email}">{name}</td>
                    <td>{data_source_name}</td>
                    <td>{amount_of_emas}</td>
                    <td>≥{_last_submission}</td>
                </tr>"""

    rows = []
    for db_user, db_data_source, amount_of_data, last_submission in warn_users:
        rows += [
            warn_html_row(
                _id=db_user["id"],
                email=db_user["email"],
                name=db_user["name"],
                data_source_name=db_data_source["name"],
                amount_of_emas=amount_of_data,
                _last_submission=last_submission,
            )
        ]
    with open("static/missing_ema_email_template.html", "r") as r:
        a = r.read()
        message_text = a.replace(
            "campaign_html_element",
            f'<h3>Campaign : <a href="http://etdb.myvnc.com/campaign/?id={db_campaign["id"]}" style="text-decoration: none;">{db_campaign["name"]}</a></h3>',
        )
        message_text = message_text.replace("table_rows", "\n".join(rows))

    return [
        create_message(
            sender="easytracknoreply@gmail.com",
            to=to,
            subject="[ET warning] EMA submission issue(s) detected",
            message_text=message_text,
        )
        for to in destination_email_addresses
    ]


def validate(array):
    return None not in array


def load_google_profile(id_token):
    google_id_details = oauth_id_token.verify_oauth2_token(
        id_token=id_token, request=oauth_requests.Request()
    )
    if google_id_details["iss"] not in [
        "accounts.google.com",
        "https://accounts.google.com",
    ]:
        print("google auth failure, wrong issuer")
        return None
    return {
        "id_token": id_token,
        "name": google_id_details["name"],
        "email": google_id_details["email"],
    }


def campaign_has_started(db_campaign):
    return get_timestamp_ms() >= db_campaign.startTimestamp


def get_download_file_path(file_name):
    if not os.path.exists(settings.download_dir):
        os.mkdir(settings.download_dir)
        os.chmod(settings.download_dir, 0o777)

    file_path = os.path.join(settings.download_dir, file_name)
    fp = open(file_path, "w+")
    fp.close()
    os.chmod(file_path, 0o777)

    return file_path


def now_us():
    return int(time.time() * 1000 * 1000)


def md5(value):
    return hashlib.md5(value.encode()).hexdigest()
