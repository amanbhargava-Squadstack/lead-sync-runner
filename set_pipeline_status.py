#!/usr/bin/env python3
"""
Sets the pipeline pause/resume state, triggered by the 'Pause Sync' or
'Resume Sync' GitHub Actions workflow (currently manual/workflow_dispatch
only; wired to also accept repository_dispatch so Slack buttons can
trigger this later using the same bridge pattern as the cap increase,
once Slack Interactivity is set up).

Writes leads_archive/pipeline_status.json - checked by
swiggy_lead_sync.py BEFORE it touches SFTP or the SquadStack API, so a
pause takes effect immediately on the next scheduled run with zero
risk of a half-finished push.
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

ARCHIVE_DIR = Path("./leads_archive")
ARCHIVE_DIR.mkdir(exist_ok=True)
PIPELINE_STATUS_FILE = ARCHIVE_DIR / "pipeline_status.json"

IST = timezone(timedelta(hours=5, minutes=30))
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")


def send_slack_alert(message: str):
    if not SLACK_WEBHOOK_URL:
        print(f"SLACK_WEBHOOK_URL not set - skipping Slack alert: {message}")
        return
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
        if resp.status_code != 200:
            print(f"Slack alert failed -> {resp.status_code} | {resp.text[:300]}")
    except requests.RequestException as e:
        print(f"Slack alert request error: {e}")


def send_slack_message_with_control_buttons(text: str):
    """
    Same three-button pattern as swiggy_lead_sync.py's version -
    duplicated here (rather than imported) so this script stays
    lightweight and doesn't need SQUADSTACK_API_TOKEN set just to send
    a status confirmation.
    """
    if not SLACK_WEBHOOK_URL:
        print(f"SLACK_WEBHOOK_URL not set - skipping Slack message: {text}")
        return

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {
            "type": "actions",
            "block_id": "pipeline_control_actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "⏸ Pause Today"},
                    "value": "pause_sync_today",
                    "action_id": "pause_sync_today",
                    "confirm": {
                        "title": {"type": "plain_text", "text": "Pause for today?"},
                        "text": {"type": "mrkdwn", "text": "No leads pushed for the rest of today - resumes automatically tomorrow."},
                        "confirm": {"type": "plain_text", "text": "Yes, pause today"},
                        "deny": {"type": "plain_text", "text": "Cancel"},
                    },
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "🛑 Stop Sync"},
                    "style": "danger",
                    "value": "pause_sync_indefinite",
                    "action_id": "pause_sync_indefinite",
                    "confirm": {
                        "title": {"type": "plain_text", "text": "Stop the sync indefinitely?"},
                        "text": {"type": "mrkdwn", "text": "This stays off across days - no leads pushed until you explicitly click Start Sync."},
                        "confirm": {"type": "plain_text", "text": "Yes, stop it"},
                        "deny": {"type": "plain_text", "text": "Cancel"},
                    },
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "▶️ Start Sync"},
                    "style": "primary",
                    "value": "resume_sync",
                    "action_id": "resume_sync",
                },
            ],
        },
    ]
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=10)
        if resp.status_code != 200:
            print(f"Slack control-button message failed -> {resp.status_code} | {resp.text[:300]}")
    except requests.RequestException as e:
        print(f"Slack control-button message request error: {e}")


def main():
    action = os.environ.get("ACTION", "").strip().lower()
    reason = os.environ.get("REASON", "").strip()
    scope = os.environ.get("SCOPE", "indefinite").strip().lower()  # "today" or "indefinite"

    if action not in ("pause", "resume"):
        sys.exit(f"ACTION must be 'pause' or 'resume', got: {action!r}")

    now = datetime.now(IST)
    now_str = now.strftime("%Y-%m-%d %I:%M %p IST")
    today_str = now.strftime("%Y-%m-%d")
    paused = action == "pause"

    status = {
        "paused": paused,
        "reason": reason,
        "updated_at": now_str,
    }
    if paused and scope == "today":
        status["auto_pause_date"] = today_str  # self-expires at IST midnight

    PIPELINE_STATUS_FILE.write_text(json.dumps(status, indent=2))

    if paused:
        reason_part = f" Reason: {reason}" if reason else ""
        if scope == "today":
            print(f"Pipeline PAUSED (today only) at {now_str}.{reason_part}")
            send_slack_message_with_control_buttons(
                f":no_entry: *Swiggy lead sync PAUSED for today* at {now_str}.{reason_part}\n"
                f"No leads will be pushed for the rest of today - resumes automatically tomorrow, "
                f"or hit Start Sync below to resume sooner."
            )
        else:
            print(f"Pipeline PAUSED (indefinite) at {now_str}.{reason_part}")
            send_slack_message_with_control_buttons(
                f":no_entry: *Swiggy lead sync PAUSED* at {now_str}.{reason_part}\n"
                f"This stays off across days until you explicitly resume it - "
                f"hit Start Sync below whenever you're ready."
            )
    else:
        print(f"Pipeline RESUMED at {now_str}.")
        send_slack_message_with_control_buttons(
            f":large_green_circle: *Swiggy lead sync RESUMED* at {now_str}.\n"
            f"Leads will start pushing again from the next scheduled run."
        )


if __name__ == "__main__":
    main()
